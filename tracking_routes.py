"""Flask Blueprint for tracking-integrasjon: admin-OAuth + customer-tracking.

Vendor: ABAX (api.abax.cloud, identity.abax.cloud).

Registreres i app.py med:
    from tracking_routes import register_tracking
    register_tracking(app, manual_orders_ref=lambda: _manual_orders,
                      save_state=_save_sync_state, state_dir=STATE_DIR,
                      admin_check=_user_from_request)
"""
from __future__ import annotations

import os
import secrets
import time
from typing import Callable, Iterable
from flask import Blueprint, jsonify, request, redirect, render_template_string, abort

from abax import AbaxClient, AbaxError, AbaxNotConnected
from eta import compute_eta, geocode, order_destination

bp = Blueprint("tracking", __name__)

# ── Singletons satt av register_tracking ──────────────────────────────────
_state: dict = {
    "client": None,            # AbaxClient
    "orders_ref": None,        # Callable -> list[dict]
    "save_state": None,        # Callable -> None
    "admin_check": None,       # Callable returns (user|None, errResp|None)
    "active_vehicle_id": None, # str — satt av admin: hvilken bil leverer i dag?
    "depot": None,             # (lat, lon) — fallback hvis bilen er offline
}


def register_tracking(
    app,
    *,
    manual_orders_ref: Callable[[], list],
    save_state: Callable[[], None],
    state_dir: str,
    admin_check: Callable[[], tuple],
    depot_coords: tuple[float, float] | None = None,
) -> None:
    _state["client"] = AbaxClient(state_dir)
    _state["orders_ref"] = manual_orders_ref
    _state["save_state"] = save_state
    _state["admin_check"] = admin_check
    _state["active_vehicle_id"] = os.environ.get("ABAX_DEFAULT_VEHICLE_ID")
    _state["depot"] = depot_coords or _parse_depot()
    app.register_blueprint(bp)


def _parse_depot() -> tuple[float, float] | None:
    raw = os.environ.get("HAVOYET_DEPOT_COORDS")  # "60.39,5.32"
    if not raw or "," not in raw:
        return None
    try:
        lat, lon = raw.split(",", 1)
        return float(lat.strip()), float(lon.strip())
    except ValueError:
        return None


def _client() -> AbaxClient:
    c = _state["client"]
    if c is None:
        raise RuntimeError("Tracking-blueprint ikke registrert — kall register_tracking() i app.py")
    return c


def _admin_only():
    """Returns (user_or_None, error_response_or_None).

    admin_check er forventet å returnere enten en user-dict eller en (user, _)-tuple
    som matcher _user_from_request() i app.py.
    """
    check = _state["admin_check"]
    result = check()
    user = result[0] if isinstance(result, tuple) else result
    if not user:
        return None, (jsonify({"error": "unauthorized"}), 401)
    if user.get("role") != "admin":
        return None, (jsonify({"error": "admin_required"}), 403)
    return user, None


def _find_order(order_id: str) -> dict | None:
    orders = _state["orders_ref"]() or []
    for o in orders:
        if str(o.get("ordrenr") or o.get("id")) == str(order_id):
            return o
    return None


# ═══════════════════════════════════════════════════════════════════════════
# ADMIN-ENDEPUNKTER
# ═══════════════════════════════════════════════════════════════════════════

@bp.get("/api/admin/tracking/status")
def admin_tracking_status():
    _, err = _admin_only()
    if err:
        return err
    return jsonify({
        **_client().status(),
        "active_vehicle_id": _state["active_vehicle_id"],
        "depot": _state["depot"],
    })


@bp.post("/api/admin/tracking/connect")
def admin_tracking_connect():
    """Returnerer URL admin skal sendes til for OAuth-godkjenning."""
    _, err = _admin_only()
    if err:
        return err
    try:
        url, state = _client().build_authorize_url()
    except AbaxError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"authorize_url": url, "state": state})


@bp.get("/api/admin/tracking/callback")
def admin_tracking_callback():
    """OAuth-redirect kommer hit. Bytter code → tokens."""
    code = request.args.get("code")
    err_param = request.args.get("error")
    if err_param:
        return f"<h1>ABAX-tilkobling avbrutt</h1><p>{err_param}</p>", 400
    if not code:
        return "Mangler 'code'-parameter", 400
    try:
        _client().exchange_code(code)
    except AbaxError as e:
        return f"<h1>Token-utveksling feilet</h1><pre>{e}</pre>", 502
    return (
        "<h1>✅ ABAX tilkoblet</h1>"
        "<p>Du kan lukke dette vinduet og gå tilbake til admin-panelet.</p>"
    )


@bp.post("/api/admin/tracking/disconnect")
def admin_tracking_disconnect():
    _, err = _admin_only()
    if err:
        return err
    _client().disconnect()
    return jsonify({"ok": True})


@bp.get("/api/admin/tracking/vehicles")
def admin_tracking_vehicles():
    _, err = _admin_only()
    if err:
        return err
    try:
        vehicles = _client().list_vehicles()
    except AbaxNotConnected:
        return jsonify({"error": "not_connected"}), 409
    except AbaxError as e:
        return jsonify({"error": str(e)}), 502
    return jsonify({"vehicles": vehicles})


@bp.post("/api/admin/tracking/active-vehicle")
def admin_tracking_set_active():
    _, err = _admin_only()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    vehicle_id = (data.get("vehicle_id") or "").strip()
    _state["active_vehicle_id"] = vehicle_id or None
    return jsonify({"ok": True, "active_vehicle_id": _state["active_vehicle_id"]})


# ═══════════════════════════════════════════════════════════════════════════
# ORDRE: token-generering + ETA
# ═══════════════════════════════════════════════════════════════════════════

@bp.post("/api/admin/orders/<order_id>/track-token")
def admin_create_track_token(order_id: str):
    """Admin trykker 'Send tracking-link' → vi genererer token og lagrer på ordren."""
    _, err = _admin_only()
    if err:
        return err
    order = _find_order(order_id)
    if not order:
        return jsonify({"error": "Ordre ikke funnet"}), 404

    if not order.get("track_token"):
        order["track_token"] = secrets.token_urlsafe(20)
        order["track_token_created"] = int(time.time())
    if request.json and request.json.get("vehicle_id"):
        order["track_vehicle_id"] = request.json["vehicle_id"]
    elif _state["active_vehicle_id"]:
        order.setdefault("track_vehicle_id", _state["active_vehicle_id"])

    _state["save_state"]()

    base = (request.host_url or "").rstrip("/")
    return jsonify({
        "ok": True,
        "track_token": order["track_token"],
        "track_url": f"{base}/track/{order_id}?token={order['track_token']}",
        "vehicle_id": order.get("track_vehicle_id"),
    })


@bp.delete("/api/admin/orders/<order_id>/track-token")
def admin_revoke_track_token(order_id: str):
    _, err = _admin_only()
    if err:
        return err
    order = _find_order(order_id)
    if not order:
        return jsonify({"error": "Ordre ikke funnet"}), 404
    order.pop("track_token", None)
    order.pop("track_token_created", None)
    order.pop("track_vehicle_id", None)
    _state["save_state"]()
    return jsonify({"ok": True})


def _verify_track_access(order_id: str) -> dict:
    order = _find_order(order_id)
    if not order:
        abort(404)
    token = request.args.get("token", "")
    expected = order.get("track_token", "")
    if not expected or not secrets.compare_digest(str(token), str(expected)):
        abort(403)
    return order


@bp.get("/api/orders/<order_id>/eta")
def public_order_eta(order_id: str):
    """Public: kunden poller dette for å oppdatere live ETA. Token-beskyttet."""
    order = _verify_track_access(order_id)

    dest = order_destination(order)
    if not dest:
        return jsonify({
            "error": "Klarte ikke finne leveringsadressen",
            "status": order.get("status"),
        }), 422

    # Hent live-posisjon
    vehicle_id = order.get("track_vehicle_id") or _state["active_vehicle_id"]
    position = None
    source = None
    if vehicle_id:
        try:
            position = _client().get_position(vehicle_id)
            source = "abax"
        except AbaxNotConnected:
            position = None
            source = "not_connected"
        except AbaxError:
            position = None
            source = "abax_error"

    if position is None:
        # Fallback: bruk depotet hvis vi har det
        depot = _state["depot"]
        if depot:
            position = {"lat": depot[0], "lon": depot[1], "speed_kmh": 0,
                        "timestamp": None}
            source = source or "depot_fallback"
        else:
            return jsonify({
                "error": "Ingen posisjonsdata tilgjengelig",
                "source": source,
                "status": order.get("status"),
            }), 503

    eta = compute_eta(position["lat"], position["lon"], dest[0], dest[1])

    return jsonify({
        "order_id": order_id,
        "status": order.get("status"),
        "minutes": int(round(eta["duration_min"])),
        "duration_min": eta["duration_min"],
        "distance_km": eta["distance_km"],
        "eta_source": eta["source"],
        "vehicle": {
            "lat": position["lat"],
            "lon": position["lon"],
            "speed_kmh": position.get("speed_kmh"),
            "timestamp": position.get("timestamp"),
            "data_source": source,
        },
        "destination": {"lat": dest[0], "lon": dest[1]},
        "delivered": _is_delivered(order),
    })


def _is_delivered(order: dict) -> bool:
    s = str(order.get("status") or "").lower()
    return "lever" in s or s in ("done", "completed", "fullført")


# ═══════════════════════════════════════════════════════════════════════════
# Tracking-side
# ═══════════════════════════════════════════════════════════════════════════

_TRACK_HTML = """<!doctype html>
<html lang="no">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Følg leveringen din — Havøyet</title>
  <style>
    :root{--blue:#1A3A5C;--gold:#C8A45C;--bg:#F4F1EA;--ink:#1B1B1B;--soft:#666}
    *{box-sizing:border-box}
    body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
         background:var(--bg);color:var(--ink);min-height:100dvh}
    .wrap{max-width:520px;margin:0 auto;padding:24px 18px}
    header{text-align:center;margin-bottom:18px}
    .brand{display:inline-flex;align-items:center;gap:8px;color:var(--blue);
           font-weight:700;font-size:18px}
    .mark{width:32px;height:32px;border-radius:50%;background:var(--blue);
          color:var(--gold);display:inline-flex;align-items:center;justify-content:center;
          font-weight:700}
    .card{background:#fff;border-radius:14px;padding:24px;
          box-shadow:0 4px 14px rgba(0,0,0,.08);margin-bottom:14px}
    .eta-card{text-align:center;padding:32px 22px}
    .eta-label{color:var(--soft);font-size:13px;text-transform:uppercase;
               letter-spacing:.5px}
    .eta-num{font-size:72px;font-weight:700;color:var(--blue);line-height:1;
             margin:8px 0;font-variant-numeric:tabular-nums}
    .eta-unit{color:var(--soft);font-size:18px}
    .meta{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:18px}
    .meta-block{background:var(--bg);border-radius:10px;padding:12px;text-align:center}
    .meta-block span{display:block;font-size:11px;color:var(--soft);
                     text-transform:uppercase;letter-spacing:.5px}
    .meta-block strong{display:block;font-size:18px;color:var(--blue);margin-top:2px}
    .delivered{background:#E5F4EB;color:#1F6B43;padding:14px 18px;
               border-radius:10px;text-align:center;font-weight:600}
    .error{background:#FFE5E5;color:#A33;padding:14px;border-radius:10px;
           text-align:center;font-size:14px}
    .pulse{display:inline-block;width:8px;height:8px;border-radius:50%;
           background:#28A745;margin-right:6px;animation:p 1.6s ease-in-out infinite}
    @keyframes p{50%{opacity:.3}}
    footer{text-align:center;color:var(--soft);font-size:12px;margin-top:14px}
    a{color:var(--blue)}
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <div class="brand"><span class="mark">H</span><span>Havøyet</span></div>
    </header>

    <div class="card eta-card" id="eta-card">
      <div class="eta-label">Forventet ankomst</div>
      <div class="eta-num" id="num">…</div>
      <div class="eta-unit" id="unit">minutter</div>
      <div class="meta">
        <div class="meta-block">
          <span>Avstand</span>
          <strong id="dist">—</strong>
        </div>
        <div class="meta-block">
          <span>Status</span>
          <strong id="status">Henter…</strong>
        </div>
      </div>
    </div>

    <div class="card" id="info-card">
      <p style="margin:0;color:var(--soft);font-size:14px">
        <span class="pulse"></span> Oppdateres automatisk hvert 30. sekund.
        Sjåføren kan ikke se denne siden.
      </p>
    </div>

    <footer>
      Spørsmål? Kontakt oss på
      <a href="mailto:erik@havoyet.no">erik@havoyet.no</a>
    </footer>
  </div>

<script>
  const ORDER_ID = {{ order_id|tojson }};
  const TOKEN    = {{ token|tojson }};
  const $ = (id) => document.getElementById(id);

  async function refresh() {
    try {
      const r = await fetch(`/api/orders/${encodeURIComponent(ORDER_ID)}/eta?token=${encodeURIComponent(TOKEN)}`);
      const d = await r.json();
      if (!r.ok) {
        $("status").textContent = d.error || "Feil";
        $("num").textContent = "—";
        return;
      }
      if (d.delivered) {
        document.getElementById("eta-card").innerHTML =
          '<div class="delivered">✓ Leveringen er fullført. Takk for at du handlet hos Havøyet!</div>';
        return;
      }
      $("num").textContent = d.minutes;
      $("unit").textContent = d.minutes === 1 ? "minutt" : "minutter";
      $("dist").textContent = `${d.distance_km.toFixed(1)} km`;
      $("status").textContent = d.status || "Underveis";
    } catch (e) {
      $("status").textContent = "Mistet kontakt";
    }
  }

  refresh();
  setInterval(refresh, 30000);
</script>
</body>
</html>
"""


@bp.get("/track/<order_id>")
def public_track_page(order_id: str):
    _verify_track_access(order_id)
    token = request.args.get("token", "")
    return render_template_string(_TRACK_HTML, order_id=order_id, token=token)
