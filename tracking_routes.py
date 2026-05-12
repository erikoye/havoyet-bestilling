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
import threading
import time
from pathlib import Path
from typing import Callable, Optional
from flask import Blueprint, jsonify, request, redirect, render_template_string, abort

from abax import AbaxClient, AbaxError, AbaxNotConnected
from eta import (
    compute_eta,
    fallback_eta,
    geocode,
    google_directions_fixed_order,
    matrix_one_to_many,
    order_destination,
    trip_optimize,
)

bp = Blueprint("tracking", __name__)

# ── Singletons satt av register_tracking ──────────────────────────────────
_state: dict = {
    "client": None,            # AbaxClient
    "orders_ref": None,        # Callable -> list[dict]
    "save_state": None,        # Callable -> None
    "admin_check": None,       # Callable returns (user|None, errResp|None)
    "active_vehicle_id": None, # str — satt av admin: hvilken bil leverer i dag?
    "depot": None,             # (lat, lon) — fallback hvis bilen er offline
    "sms_sender": None,        # Callable(phone, body) -> (ok, detail)
    "route_eta_sender": None,  # Callable(order, eta_clock, tracking_url) -> (ok, detail)
    "tracking_base_url": "",   # "https://bestilling.havoyet.no" — for SMS-lenker
    "state_dir": None,         # Path — brukes til proximity-låser
    "notify_threshold_min": 5, # ETA-terskel for "snart fremme"-SMS
    "watcher_started": False,
}


def register_tracking(
    app,
    *,
    manual_orders_ref: Callable[[], list],
    save_state: Callable[[], None],
    state_dir: str,
    admin_check: Callable[[], tuple],
    depot_coords: tuple[float, float] | None = None,
    sms_sender: Optional[Callable[[str, str], tuple]] = None,
    route_eta_sender: Optional[Callable[[dict, str, str], tuple]] = None,
    tracking_base_url: str = "",
) -> None:
    _state["client"] = AbaxClient(state_dir)
    _state["orders_ref"] = manual_orders_ref
    _state["save_state"] = save_state
    _state["admin_check"] = admin_check
    _state["active_vehicle_id"] = os.environ.get("ABAX_DEFAULT_VEHICLE_ID")
    _state["depot"] = depot_coords or _parse_depot()
    _state["sms_sender"] = sms_sender
    _state["route_eta_sender"] = route_eta_sender
    _state["tracking_base_url"] = (
        tracking_base_url
        or os.environ.get("TRACKING_PUBLIC_URL")
        or "https://bestilling.havoyet.no"
    ).rstrip("/")
    _state["state_dir"] = Path(state_dir)
    try:
        _state["notify_threshold_min"] = int(os.environ.get("TRACKING_NOTIFY_MIN", "10"))
    except ValueError:
        _state["notify_threshold_min"] = 10
    app.register_blueprint(bp)
    _start_proximity_watcher()


# Havøyet AS, Nesttunvegen 96, 5221 Nesttun (geokodet via Nominatim).
# Brukes hvis HAVOYET_DEPOT_COORDS-env-var ikke er satt.
_DEFAULT_DEPOT: tuple[float, float] = (60.3184, 5.3528)


def _parse_depot() -> tuple[float, float] | None:
    raw = os.environ.get("HAVOYET_DEPOT_COORDS")  # "60.39,5.32"
    if raw and "," in raw:
        try:
            lat, lon = raw.split(",", 1)
            return float(lat.strip()), float(lon.strip())
        except ValueError:
            pass
    return _DEFAULT_DEPOT


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


def _drivers_file() -> Path:
    return (_state["state_dir"] or Path("/tmp")) / "drivers.json"


def _load_drivers() -> list[dict]:
    """Returnerer alle registrerte sjåfører. Tom liste hvis ingen er lagret."""
    path = _drivers_file()
    if not path.exists():
        return []
    try:
        import json
        data = json.loads(path.read_text())
        if isinstance(data, dict) and isinstance(data.get("drivers"), list):
            return data["drivers"]
        if isinstance(data, list):
            return data
    except (ValueError, OSError):
        pass
    return []


def _save_drivers(drivers: list[dict]) -> None:
    import json
    path = _drivers_file()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"drivers": drivers}, indent=2, ensure_ascii=False))
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
    except OSError as e:
        print(f"[DRIVERS] save feilet: {e}")


def _match_driver_by_pin(pin: str) -> dict | None:
    """Returnerer driver-dict hvis PINen matcher en lagret sjåfør, eller None."""
    if not pin:
        return None
    drivers = _load_drivers()
    for d in drivers:
        stored = str(d.get("pin") or "").strip()
        if stored and secrets.compare_digest(stored, pin):
            return d
    # Bakoverkompatibilitet: hvis ingen drivers.json finnes, prøv env-var DRIVER_PIN
    if not drivers:
        env_pin = os.environ.get("DRIVER_PIN", "").strip()
        if env_pin and secrets.compare_digest(env_pin, pin):
            return {"id": "env", "name": "Sjåfør", "pin": env_pin}
    return None


def _driver_only():
    """Sjåfør-auth: PIN via X-Driver-PIN-header eller ?pin=-query.

    PIN-en sjekkes mot drivers.json (multi-sjåfør). Faller tilbake til env-var
    DRIVER_PIN hvis ingen sjåfører er lagret enda (gradvis migrering).
    """
    provided = (request.headers.get("X-Driver-PIN") or request.args.get("pin") or "").strip()
    if not provided:
        return None, (jsonify({"error": "unauthorized"}), 401)
    driver = _match_driver_by_pin(provided)
    if not driver:
        return None, (jsonify({"error": "unauthorized"}), 401)
    return {"role": "driver", "name": driver.get("name"), "id": driver.get("id")}, None


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


@bp.get("/api/admin/tracking/diagnose")
def admin_tracking_diagnose():
    """Helt-systemets-helse-sjekk — hver komponent rapporterer ok/feil/mangler.

    Brukes av tracking-admin-siden til å vise en sjekkliste så admin ser
    nøyaktig hva som mangler før brikken kan spores.
    """
    _, err = _admin_only()
    if err:
        return err

    checks: list[dict] = []
    overall_ready = True

    def add(name: str, ok: bool, detail: str = "", action: str = "", critical: bool = True):
        nonlocal overall_ready
        if critical and not ok:
            overall_ready = False
        checks.append({"name": name, "ok": ok, "detail": detail, "action": action,
                       "critical": critical})

    # 1) ABAX env-vars
    client = _client()
    missing_env = []
    for key in ("ABAX_CLIENT_ID", "ABAX_CLIENT_SECRET", "ABAX_REDIRECT_URI"):
        if not os.environ.get(key):
            missing_env.append(key)
    add(
        "ABAX-kreds satt",
        not missing_env,
        detail="Alle env-vars OK" if not missing_env else f"Mangler: {', '.join(missing_env)}",
        action="Sett env-vars i Render-dashboard → Environment",
    )

    # 2) OAuth gjennomført?
    connected = client.is_connected()
    add(
        "OAuth-tilkoblet ABAX",
        connected,
        detail="Tokens lagret" if connected else "Ingen tokens — OAuth ikke gjennomført",
        action='Trykk "Koble til" på denne siden',
    )

    # 3) Vehicles tilgjengelige (kun hvis OAuth er gjort)
    vehicles_count = 0
    vehicles_ok = False
    vehicles_detail = "Hopper over — OAuth ikke gjort"
    if connected:
        try:
            vehicles = client.list_vehicles()
            vehicles_count = len(vehicles)
            vehicles_ok = vehicles_count > 0
            vehicles_detail = f"{vehicles_count} kjøretøy funnet på kontoen" if vehicles_ok else (
                "0 kjøretøy — brikken er sannsynligvis ikke tildelt enda i ABAX-portalen"
            )
        except (AbaxError, AbaxNotConnected) as e:
            vehicles_detail = f"API-feil: {e}"
    add(
        "Kjøretøy hos ABAX",
        vehicles_ok,
        detail=vehicles_detail,
        action='I ABAX-portalen: trykk "KOBLE TIL" på brikken for å tildele kjøretøy',
    )

    # 4) Aktiv bil valgt
    active = bool(_state["active_vehicle_id"])
    add(
        "Aktiv bil valgt",
        active,
        detail=f"Aktiv vehicleId: {_state['active_vehicle_id']}" if active else "Ingen bil valgt",
        action="Velg fra dropdown nedenfor",
    )

    # 5) Test live-posisjon (mest tellende — viser om brikken faktisk sender data)
    pos_ok = False
    pos_detail = "Hopper over — aktiv bil ikke valgt"
    if active and connected:
        try:
            pos = client.get_position(_state["active_vehicle_id"])
            if pos and pos.get("lat") is not None:
                pos_ok = True
                pos_detail = (f"Sist sett: lat={pos['lat']:.5f}, "
                              f"lon={pos['lon']:.5f}"
                              + (f" ({pos.get('timestamp', '?')})" if pos.get("timestamp") else ""))
            else:
                pos_detail = (
                    "ABAX svarer, men brikken har ikke rapportert posisjon ennå "
                    "(monter brikken i OBD-port + kjør 5-10 min)"
                )
        except (AbaxError, AbaxNotConnected) as e:
            pos_detail = f"API-feil: {e}"
    add(
        "Brikke rapporterer posisjon",
        pos_ok,
        detail=pos_detail,
        action="Plugg brikken i OBD-porten og kjør en kort tur",
    )

    # 6) Depot satt
    depot = _state["depot"]
    add(
        "Depot konfigurert",
        bool(depot),
        detail=f"lat={depot[0]:.4f}, lon={depot[1]:.4f}" if depot else "Mangler",
        action="Sett HAVOYET_DEPOT_COORDS env-var (eller bruk standard Nesttun)",
        critical=False,
    )

    # 7) SMS-sender koblet
    sms_ready = bool(_state.get("sms_sender"))
    add(
        "SMS-sender klar",
        sms_ready,
        detail="Kobling til app.py _send_admin_sms OK" if sms_ready else "Ikke koblet",
        action="Sjekk at register_tracking() får sms_sender=_send_admin_sms",
        critical=False,
    )

    # 8) Driver-PIN satt
    driver_pin = os.environ.get("DRIVER_PIN", "")
    pin_ok = bool(driver_pin and len(driver_pin) >= 4)
    add(
        "Sjåfør-PIN satt",
        pin_ok,
        detail="Satt (sjåfør-app fungerer)" if pin_ok else "Ikke satt — sjåfør-app vil avvise login",
        action="Sett DRIVER_PIN env-var på Render (4+ tegn)",
        critical=False,
    )

    return jsonify({
        "ready_for_tracking": overall_ready,
        "checks": checks,
        "next_action": _next_action(checks),
    })


def _next_action(checks: list[dict]) -> str:
    """Returnerer instruksjonen for første ufullførte kritiske sjekk."""
    for c in checks:
        if c.get("critical") and not c.get("ok"):
            return c.get("action") or c.get("name", "")
    return "Alt klart — du kan begynne å spore ordrer!"


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
    """Admin trykker 'Start levering' → vi genererer token, sender SMS til kunden,
    og setter watcheren til å varsle når bilen er nær fremme."""
    _, err = _admin_only()
    if err:
        return err
    order = _find_order(order_id)
    if not order:
        return jsonify({"error": "Ordre ikke funnet"}), 404

    is_new_token = not order.get("track_token")
    if is_new_token:
        order["track_token"] = secrets.token_urlsafe(20)
        order["track_token_created"] = int(time.time())
    payload = request.get_json(silent=True) or {}
    if payload.get("vehicle_id"):
        order["track_vehicle_id"] = payload["vehicle_id"]
    elif _state["active_vehicle_id"]:
        order.setdefault("track_vehicle_id", _state["active_vehicle_id"])
    # Nullstill proximity-varsel hvis vi starter en ny leveringsrunde
    if is_new_token:
        order.pop("proximity_notified_at", None)
        _clear_proximity_lock(order_id)

    _state["save_state"]()

    base = _state["tracking_base_url"] or (request.host_url or "").rstrip("/")
    track_url = f"{base}/track/{order_id}?token={order['track_token']}"

    sms_result = {"sent": False, "detail": "skipped"}
    if is_new_token or payload.get("force_sms"):
        ok, detail = _send_track_link_sms(order, track_url)
        sms_result = {"sent": ok, "detail": detail}

    return jsonify({
        "ok": True,
        "track_token": order["track_token"],
        "track_url": track_url,
        "vehicle_id": order.get("track_vehicle_id"),
        "sms": sms_result,
    })


# ═══════════════════════════════════════════════════════════════════════════
# Kunde-varsler: lenke-SMS + "snart fremme"-SMS
# ═══════════════════════════════════════════════════════════════════════════

def _get_customer_phone(order: dict) -> str:
    kunde = order.get("kunde") or {}
    raw = (
        kunde.get("tlf")
        or kunde.get("phone")
        or kunde.get("phoneNumber")
        or order.get("phone")
        or ""
    )
    return str(raw).strip()


def _sms_opt_in(order: dict) -> bool:
    pref = (order.get("kunde") or {}).get("notify") or order.get("notify") or {}
    return bool(pref.get("sms", True)) and not pref.get("opted_out", False)


def _send_track_link_sms(order: dict, track_url: str,
                          eta_clock: str | None = None) -> tuple[bool, str]:
    """Send track-lenken til kunden. Hvis eta_clock er gitt ("HH:MM") inkluderer
    vi anslått ankomst i meldingen."""
    sender = _state["sms_sender"]
    if not sender:
        return False, "no-sms-sender"
    phone = _get_customer_phone(order)
    if not phone:
        return False, "no-phone"
    if not _sms_opt_in(order):
        return False, "opted-out"
    nr = order.get("ordrenr") or order.get("id") or "?"
    if eta_clock:
        body = (
            f"Havøyet: Bestilling #{nr} kommer ca. kl {eta_clock}. "
            f"Følg leveringen live: {track_url}"
        )
    else:
        body = (
            f"Havøyet: Bestilling #{nr} er på vei. "
            f"Følg leveringen live: {track_url}"
        )
    try:
        ok, detail = sender(phone, body)
        return bool(ok), str(detail)
    except Exception as e:
        return False, f"sms-exception: {e}"


def _send_proximity_sms(order: dict, minutes: int) -> tuple[bool, str]:
    sender = _state["sms_sender"]
    if not sender:
        return False, "no-sms-sender"
    phone = _get_customer_phone(order)
    if not phone:
        return False, "no-phone"
    if not _sms_opt_in(order):
        return False, "opted-out"
    nr = order.get("ordrenr") or order.get("id") or "?"
    unit = "minutt" if minutes == 1 else "minutter"
    body = (
        f"Havøyet: Sjåføren er ca. {minutes} {unit} unna med bestilling #{nr}. "
        f"Vennligst gjør deg klar."
    )
    try:
        ok, detail = sender(phone, body)
        return bool(ok), str(detail)
    except Exception as e:
        return False, f"sms-exception: {e}"


def _proximity_lock_dir() -> Path:
    d = (_state["state_dir"] or Path("/tmp")) / "proximity_locks"
    try:
        d.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    return d


def _claim_proximity_lock(order_id: str) -> bool:
    """Atomisk fil-opprettelse: returnerer True kun for første worker som tar låsen."""
    flag = _proximity_lock_dir() / f"{order_id}.flag"
    try:
        fd = os.open(str(flag), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        return True
    except FileExistsError:
        return False
    except OSError:
        return False


def _clear_proximity_lock(order_id: str) -> None:
    flag = _proximity_lock_dir() / f"{order_id}.flag"
    try:
        flag.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        pass


# ═══════════════════════════════════════════════════════════════════════════
# Background-watcher
# ═══════════════════════════════════════════════════════════════════════════

def _start_proximity_watcher() -> None:
    if _state.get("watcher_started"):
        return
    _state["watcher_started"] = True
    t = threading.Thread(target=_proximity_watcher_loop, name="abax-proximity", daemon=True)
    t.start()


def _proximity_watcher_loop() -> None:
    """Sjekker periodisk alle aktive tracking-ordrer og sender 'snart fremme'-SMS.

    Trygt på tvers av Gunicorn-workers via _claim_proximity_lock() — kun første
    worker som klarer å lage flagg-filen sender SMS. Etter sending lagres
    proximity_notified_at på ordren slik at vi ikke fyrer på nytt etter cooldown.
    """
    base_interval = 90  # sek
    while True:
        try:
            _proximity_tick()
        except Exception as e:
            print(f"[PROXIMITY] tick feilet: {e}")
        time.sleep(base_interval)


def _proximity_tick() -> None:
    client = _state.get("client")
    orders = _state.get("orders_ref")
    if not client or not orders:
        return
    if not client.is_connected():
        return  # ingen vits — venter på OAuth
    threshold = int(_state.get("notify_threshold_min") or 5)

    # Cache vehicle-posisjoner per kall så vi ikke kaller ABAX 1x per ordre
    pos_cache: dict[str, Optional[dict]] = {}

    active = []
    for o in (orders() or []):
        if not isinstance(o, dict):
            continue
        if not o.get("track_token"):
            continue
        if _is_delivered(o):
            continue
        if o.get("proximity_notified_at"):
            continue  # allerede varslet for denne runden
        active.append(o)

    if not active:
        return

    for order in active:
        order_id = str(order.get("ordrenr") or order.get("id") or "")
        if not order_id:
            continue
        dest = order_destination(order)
        if not dest:
            continue
        vehicle_id = order.get("track_vehicle_id") or _state["active_vehicle_id"]
        if not vehicle_id:
            continue

        if vehicle_id not in pos_cache:
            try:
                pos_cache[vehicle_id] = client.get_position(vehicle_id)
            except (AbaxError, AbaxNotConnected):
                pos_cache[vehicle_id] = None
        position = pos_cache.get(vehicle_id)
        if not position:
            continue

        try:
            eta = compute_eta(position["lat"], position["lon"], dest[0], dest[1])
        except Exception as e:
            print(f"[PROXIMITY] ETA-feil for #{order_id}: {e}")
            continue
        minutes = int(round(eta.get("duration_min") or 999))
        if minutes > threshold:
            continue

        # Vi er innenfor terskelen — prøv å ta låsen
        if not _claim_proximity_lock(order_id):
            continue  # en annen worker tok den

        ok, detail = _send_proximity_sms(order, minutes)
        order["proximity_notified_at"] = int(time.time())
        order["proximity_notified_minutes"] = minutes
        order["proximity_notified_detail"] = detail
        try:
            _state["save_state"]()
        except Exception as e:
            print(f"[PROXIMITY] save_state feilet: {e}")
        print(f"[PROXIMITY] #{order_id}: ETA {minutes}min, SMS {'ok' if ok else 'feil'} ({detail})")


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
    order.pop("proximity_notified_at", None)
    order.pop("proximity_notified_minutes", None)
    order.pop("proximity_notified_detail", None)
    _clear_proximity_lock(order_id)
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
# RUTE-PLANLEGGING (admin-side, kart)
# ═══════════════════════════════════════════════════════════════════════════

def _delivery_date(order: dict) -> str:
    """Returnerer ISO-leveringsdato eller tom streng."""
    kunde = order.get("kunde") or {}
    raw = kunde.get("leveringsdag") or order.get("delivery") or ""
    return str(raw).strip()


def _today_iso() -> str:
    return time.strftime("%Y-%m-%d", time.localtime())


def _orders_for_date(date_iso: str) -> list[dict]:
    """Filtrer ut ordrer som skal leveres på gitt dato og ikke er ferdige."""
    out = []
    for o in (_state["orders_ref"]() or []):
        if not isinstance(o, dict):
            continue
        if _is_delivered(o):
            continue
        if _delivery_date(o) != date_iso:
            continue
        out.append(o)
    return out


def _stop_payload(order: dict) -> dict:
    kunde = order.get("kunde") or {}
    levering = order.get("levering") or {}
    return {
        "order_id": str(order.get("ordrenr") or order.get("id") or ""),
        "navn": kunde.get("navn") or kunde.get("name") or "",
        "tlf": _get_customer_phone(order),
        "adresse": (
            levering.get("adresse")
            or order.get("leveringsadresse")
            or kunde.get("adresse")
            or ""
        ),
        "postnr": (
            levering.get("postnr")
            or order.get("leveringspostnr")
            or kunde.get("postnr")
            or ""
        ),
        "poststed": (
            levering.get("poststed")
            or order.get("leveringspoststed")
            or kunde.get("poststed")
            or ""
        ),
        "leveringstid": kunde.get("leveringstid") or order.get("slot") or "",
        "status": order.get("status") or "",
        "track_token": bool(order.get("track_token")),
    }


def _route_state_file() -> Path:
    return (_state["state_dir"] or Path("/tmp")) / "route_state.json"


def _load_route_state() -> dict:
    """Returnerer alt admin-rediget rute-data (per dato)."""
    import json
    path = _route_state_file()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text()) or {}
    except (ValueError, OSError):
        return {}


def _save_route_state(state: dict) -> None:
    import json
    path = _route_state_file()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2, ensure_ascii=False))
    except OSError as e:
        print(f"[ROUTE-STATE] save feilet: {e}")


def _get_day_state(date_iso: str) -> dict:
    return (_load_route_state() or {}).get(date_iso, {}) or {}


def _set_day_state(date_iso: str, updates: dict) -> dict:
    state = _load_route_state()
    day = state.get(date_iso) or {}
    day.update(updates)
    state[date_iso] = day
    _save_route_state(state)
    return day


_DWELL_MIN = 10           # minutter brukt hos hver kunde (default)
_DEFAULT_START = (14, 0)  # default avreise fra depot hvis ingen tidspunkt gitt


def _parse_slot_start_min(slot_str: str) -> int:
    """Returnerer slot-start i minutter siden midnatt. 9999 hvis ikke gyldig."""
    import re
    if not slot_str:
        return 9999
    m = re.match(r"\s*(\d{1,2})", str(slot_str))
    if not m:
        return 9999
    h = int(m.group(1))
    if 0 <= h <= 23:
        return h * 60
    return 9999


def _parse_slot_end_min(slot_str: str) -> int:
    """Returnerer slot-slutt i minutter siden midnatt. 9999 hvis ikke gyldig.
    Støtter "13-15", "13–15", "13:00-15:00", "13.00-15.00"."""
    import re
    if not slot_str:
        return 9999
    parts = re.split(r"[-–—]", str(slot_str), maxsplit=1)
    if len(parts) < 2:
        return 9999
    m = re.match(r"\s*(\d{1,2})", parts[1])
    if not m:
        return 9999
    h = int(m.group(1))
    if 0 <= h <= 23:
        return h * 60
    return 9999


def _min_to_clock(total_min: int) -> str:
    """Returnerer 'HH:MM' fra minutter siden midnatt."""
    total = int(round(total_min)) % (24 * 60)
    return f"{total // 60:02d}:{total % 60:02d}"


def _group_stops_by_slot(stops: list[dict]) -> list[list[dict]]:
    """Grupperer stopp etter leveringstid-slot-start, sortert kronologisk."""
    by_slot: dict[int, list[dict]] = {}
    for s in stops:
        key = _parse_slot_start_min(s.get("leveringstid", ""))
        by_slot.setdefault(key, []).append(s)
    return [by_slot[k] for k in sorted(by_slot.keys())]


def _optimize_group_geographically(group: list[dict], start_point: tuple) -> list[dict]:
    """Optimaliserer rekkefølge innen ÉN slot-gruppe ved bruk av OSRM/Google trip
    fra start_point. Returnerer stopp i optimal rekkefølge."""
    if len(group) <= 1:
        return list(group)
    coords = [start_point] + [(s["lat"], s["lon"]) for s in group]
    trip = trip_optimize(coords, roundtrip=False)
    if trip and trip.get("order"):
        ordered = []
        # trip.order er liste av input-indekser; 0 = start_point, 1+ = group
        for idx in trip["order"][1:]:
            ordered.append(group[idx - 1])
        return ordered
    return list(group)  # behold opprinnelig rekkefølge ved feil


def _parse_start_clock_hint(value: str | None) -> tuple[int, int] | None:
    """Parser "HH:MM" til (h, m). Returnerer None ved feil eller tomt."""
    if not isinstance(value, str) or ":" not in value:
        return None
    try:
        h, m = value.split(":", 1)
        return (int(h), int(m))
    except ValueError:
        return None


def _compute_earliest_start(stops: list[dict]) -> tuple[int, int] | None:
    """Finner tidligst mulige avreise som lar alle stopp ankomme etter sin
    slot-start. Returnerer (h, m) eller None hvis ingen stopp har slot-info.

    For hver stopp i: required_start = slot_start - cum_drive - prev_dwells
    Tar maks slik at det "sneste" kravet styrer. Resultatet blir den tidligste
    avreisen som unngår FOR TIDLIG-stopp.
    """
    if not stops:
        return None
    max_required: int | None = None
    cum_drive = 0.0
    for s in stops:
        cum_drive += float(s.get("leg_min") or 0)
        slot_start = _parse_slot_start_min(s.get("leveringstid", ""))
        # Hopp over stopp uten slot-info (slot_start == 9999) eller 00:00-default
        if slot_start == 9999 or slot_start == 0:
            continue
        prev_dwells = (int(s.get("stop_index", 1)) - 1) * _DWELL_MIN
        required = int(round(slot_start - cum_drive - prev_dwells))
        if max_required is None or required > max_required:
            max_required = required
    if max_required is None:
        return None
    # Klemt til [0..24h-1min] og runder NED til hele minuttet for å lande
    # akkurat på slot-start (ikke ett minutt etter)
    minutes = max(0, min(24 * 60 - 1, max_required))
    return (minutes // 60, minutes % 60)


def _normalize_breaks(breaks_raw) -> list[dict]:
    """Validerer breaks-list. Format: [{after_order_id, minutes, label?}].
    after_order_id == 'depot' = pause før første stopp.
    """
    out: list[dict] = []
    if not isinstance(breaks_raw, list):
        return out
    for br in breaks_raw:
        if not isinstance(br, dict):
            continue
        anchor = str(br.get("after_order_id") or "").strip()
        if not anchor:
            continue
        try:
            mins = int(br.get("minutes") or 0)
        except (TypeError, ValueError):
            mins = 0
        if mins <= 0:
            continue
        item = {"after_order_id": anchor, "minutes": mins}
        label = str(br.get("label") or "").strip()
        if label:
            item["label"] = label[:40]
        out.append(item)
    return out


def _break_min_before_stop(stop_zero_idx: int, breaks_list: list[dict], stops: list[dict]) -> int:
    """Returnerer akkumulerte pause-minutter som skjer FØR stoppet på indeks i.
    `after_order_id == 'depot'` = før første stopp (legges til alle).
    `after_order_id == X` = etter stopp X's avgang, så påvirker alle senere.
    """
    if not breaks_list:
        return 0
    total = 0
    for br in breaks_list:
        anchor = str(br.get("after_order_id") or "")
        mins = int(br.get("minutes") or 0)
        if mins <= 0:
            continue
        if anchor == "depot":
            total += mins
            continue
        anchor_idx = next(
            (i for i, s in enumerate(stops) if str(s.get("order_id")) == anchor),
            None,
        )
        if anchor_idx is not None and anchor_idx < stop_zero_idx:
            total += mins
    return total


def _annotate_clock_times(stops: list[dict], start_clock: tuple[int, int] | None = None,
                          breaks: list[dict] | None = None) -> tuple[int, int]:
    """Setter `arrival_clock`, `departure_clock`, `late`-felter på hver stopp.

    arrival_clock = avreise + cum_min_from_depot (kjøretid) + dwell + pauser før stoppet
    departure_clock = arrival + _DWELL_MIN
    late = True hvis arrival er etter slot-slutt

    Hvis `start_clock` ikke er gitt, autoberegnes tidligst mulig avreise som
    unngår FOR TIDLIG-stopp. Returnerer (h, m) brukt for avreise.
    """
    if not stops:
        return start_clock or _DEFAULT_START
    if start_clock is None:
        start_clock = _compute_earliest_start(stops) or _DEFAULT_START
    start_h, start_m = start_clock
    start_total = start_h * 60 + start_m
    breaks_list = breaks or []
    cum_drive = 0.0
    for i, s in enumerate(stops):
        cum_drive += float(s.get("leg_min") or 0)
        arrival_min = start_total + cum_drive
        # Legg på dwell-tid for tidligere stopp (alle utenom dette)
        prev_dwells = (s.get("stop_index", 1) - 1) * _DWELL_MIN
        arrival_min += prev_dwells
        # Legg på pauser som ligger før dette stoppet
        arrival_min += _break_min_before_stop(i, breaks_list, stops)
        s["arrival_clock"] = _min_to_clock(arrival_min)
        s["departure_clock"] = _min_to_clock(arrival_min + _DWELL_MIN)
        slot_end = _parse_slot_end_min(s.get("leveringstid", ""))
        slot_start = _parse_slot_start_min(s.get("leveringstid", ""))
        s["late"] = (slot_end != 9999 and arrival_min > slot_end * 1)
        s["early"] = (slot_start != 9999 and slot_start != 0 and arrival_min + _DWELL_MIN < slot_start * 1)
    return start_clock


def _build_route(date_iso: str, *, optimize: bool = True) -> dict:
    """Bygger dagens rute. Felles for /today og /live.

    Returnerer:
      {
        date, depot, stops[], geometry, total_distance_km, total_duration_min,
        unresolved (ordrer uten geokod-treff)
      }
    """
    depot = _state.get("depot")
    if not depot:
        return {"error": "no_depot", "stops": [], "unresolved": []}

    raw_orders = _orders_for_date(date_iso)
    stops = []
    unresolved = []
    for o in raw_orders:
        dest = order_destination(o)
        item = _stop_payload(o)
        if not dest:
            unresolved.append(item)
            continue
        item["lat"] = dest[0]
        item["lon"] = dest[1]
        stops.append(item)

    if not stops:
        return {
            "date": date_iso,
            "depot": {"lat": depot[0], "lon": depot[1]},
            "stops": [],
            "geometry": None,
            "total_distance_km": 0,
            "total_duration_min": 0,
            "unresolved": unresolved,
        }

    # Admin kan ha lagret en custom rekkefølge ELLER godkjent OSRM-rekkefølgen.
    # Hvis vi har en lagret stop_order, bruker vi den og hopper over OSRM-trip.
    day_state = _get_day_state(date_iso)
    custom_order = day_state.get("stop_order") if isinstance(day_state.get("stop_order"), list) else None
    if custom_order:
        order_map = {s["order_id"]: s for s in stops}
        ordered_stops = []
        cum_min = 0.0
        prev = depot
        for sid in custom_order:
            s = order_map.get(str(sid))
            if not s:
                continue
            seg = compute_eta(prev[0], prev[1], s["lat"], s["lon"])
            cum_min += seg["duration_min"]
            ordered_stops.append({
                **s,
                "stop_index": len(ordered_stops) + 1,
                "leg_min": seg["duration_min"],
                "leg_km": seg["distance_km"],
                "cum_min_from_depot": round(cum_min, 1),
            })
            prev = (s["lat"], s["lon"])
        # Inkluder eventuelle nye stopp som ikke er i den lagrede rekkefølgen ennå
        included_ids = set(custom_order)
        for s in stops:
            if s["order_id"] in included_ids:
                continue
            seg = compute_eta(prev[0], prev[1], s["lat"], s["lon"])
            cum_min += seg["duration_min"]
            ordered_stops.append({
                **s,
                "stop_index": len(ordered_stops) + 1,
                "leg_min": seg["duration_min"],
                "leg_km": seg["distance_km"],
                "cum_min_from_depot": round(cum_min, 1),
            })
            prev = (s["lat"], s["lon"])
        # Auto-beregn tidligst mulig avreise + klokkeslett-annotering
        start_clock_hint = day_state.get("start_clock") if isinstance(day_state, dict) else None
        clock_override = _parse_start_clock_hint(start_clock_hint)
        breaks_list = _normalize_breaks(day_state.get("breaks"))
        used_clock = _annotate_clock_times(ordered_stops, clock_override, breaks_list)
        # Hent geometri for admin-rekkefølgen fra Google (samme call som slot-aware).
        # Hvis Google ikke er konfigurert, tegner frontend en rettlinje-fallback.
        geometry = None
        if ordered_stops:
            fixed_coords = [depot] + [(s["lat"], s["lon"]) for s in ordered_stops]
            fixed = google_directions_fixed_order(fixed_coords)
            if fixed and fixed.get("geometry"):
                geometry = fixed["geometry"]
        return {
            "date": date_iso,
            "depot": {"lat": depot[0], "lon": depot[1]},
            "stops": ordered_stops,
            "geometry": geometry,
            "total_distance_km": round(sum(s["leg_km"] for s in ordered_stops), 2),
            "total_duration_min": round(cum_min, 1),
            "optimizer": "custom",
            "approved": bool(day_state.get("approved")),
            "approved_at": day_state.get("approved_at"),
            "start_clock": f"{used_clock[0]:02d}:{used_clock[1]:02d}",
            "dwell_min": _DWELL_MIN,
            "breaks": breaks_list,
            "unresolved": unresolved,
        }

    # ─── Slot-bevisst rekkefølge: 13-15-slot leveres før 15-18-slot ─────
    # Innen samme slot optimeres geografisk via OSRM/Google trip.
    if optimize:
        groups = _group_stops_by_slot(stops)
        ordered_within_groups: list[dict] = []
        prev_point = depot
        for grp in groups:
            grp_ordered = _optimize_group_geographically(grp, prev_point)
            ordered_within_groups.extend(grp_ordered)
            if grp_ordered:
                last = grp_ordered[-1]
                prev_point = (last["lat"], last["lon"])
        # Bygg legs sekvensielt slik at cum-tider blir riktige
        slot_stops = []
        cum_min = 0.0
        prev = depot
        start_clock_hint = day_state.get("start_clock") if isinstance(day_state, dict) else None
        for i, s in enumerate(ordered_within_groups, start=1):
            seg = compute_eta(prev[0], prev[1], s["lat"], s["lon"])
            cum_min += seg["duration_min"]
            slot_stops.append({
                **s,
                "stop_index": i,
                "leg_min": seg["duration_min"],
                "leg_km": seg["distance_km"],
                "cum_min_from_depot": round(cum_min, 1),
            })
            prev = (s["lat"], s["lon"])

        # Klokkeslett-annotering: bruk admins eksplisitt satte start_clock
        # hvis tilgjengelig, ellers auto-beregn tidligst mulig avreise som
        # unngår FOR TIDLIG-stopp. Dette gjør at endringer på leveringstid i
        # en bestilling automatisk gir ny avreisetid neste gang ruten hentes.
        clock = _parse_start_clock_hint(start_clock_hint)
        breaks_list = _normalize_breaks(day_state.get("breaks"))
        clock = _annotate_clock_times(slot_stops, clock, breaks_list)

        # Hent geometri i fast rekkefølge fra Google (én ekstra call, billig)
        geometry = None
        if slot_stops:
            fixed_coords = [depot] + [(s["lat"], s["lon"]) for s in slot_stops]
            fixed = google_directions_fixed_order(fixed_coords)
            if fixed and fixed.get("geometry"):
                geometry = fixed["geometry"]

        return {
            "date": date_iso,
            "depot": {"lat": depot[0], "lon": depot[1]},
            "stops": slot_stops,
            "geometry": geometry,
            "total_distance_km": round(sum(s["leg_km"] for s in slot_stops), 2),
            "total_duration_min": round(cum_min, 1),
            "optimizer": "slot-aware",
            "approved": bool(day_state.get("approved")),
            "approved_at": day_state.get("approved_at"),
            "start_clock": f"{clock[0]:02d}:{clock[1]:02d}",
            "dwell_min": _DWELL_MIN,
            "breaks": breaks_list,
            "unresolved": unresolved,
        }

    coords = [depot] + [(s["lat"], s["lon"]) for s in stops]
    trip = trip_optimize(coords, roundtrip=False) if optimize else None

    if trip and trip.get("order"):
        order = trip["order"]
        # order[0] == 0 (depot). Resten peker inn i coords-listen (1..N → stop-index 0..N-1).
        ordered_stops = []
        cum_min = 0.0
        legs = trip.get("legs") or []
        for leg_idx, coord_idx in enumerate(order[1:], start=1):
            stop = stops[coord_idx - 1]
            leg = legs[leg_idx - 1] if leg_idx - 1 < len(legs) else {}
            cum_min += leg.get("duration_min", 0)
            stop = {
                **stop,
                "stop_index": leg_idx,
                "leg_min": leg.get("duration_min"),
                "leg_km": leg.get("distance_km"),
                "cum_min_from_depot": round(cum_min, 1),
            }
            ordered_stops.append(stop)
        clock_override = _parse_start_clock_hint(day_state.get("start_clock") if isinstance(day_state, dict) else None)
        breaks_list = _normalize_breaks(day_state.get("breaks"))
        used_clock = _annotate_clock_times(ordered_stops, clock_override, breaks_list)
        return {
            "date": date_iso,
            "depot": {"lat": depot[0], "lon": depot[1]},
            "stops": ordered_stops,
            "geometry": trip.get("geometry"),
            "total_distance_km": trip.get("total_distance_km"),
            "total_duration_min": trip.get("total_duration_min"),
            "optimizer": trip.get("source") or "osrm",
            "approved": bool(day_state.get("approved")),
            "approved_at": day_state.get("approved_at"),
            "start_clock": f"{used_clock[0]:02d}:{used_clock[1]:02d}",
            "dwell_min": _DWELL_MIN,
            "breaks": breaks_list,
            "unresolved": unresolved,
        }

    # Fallback: bevar opprinnelig rekkefølge, ikke optimalisert
    fallback_stops = []
    cum_min = 0.0
    prev = depot
    for i, s in enumerate(stops, start=1):
        seg = compute_eta(prev[0], prev[1], s["lat"], s["lon"])
        cum_min += seg["duration_min"]
        fallback_stops.append({
            **s,
            "stop_index": i,
            "leg_min": seg["duration_min"],
            "leg_km": seg["distance_km"],
            "cum_min_from_depot": round(cum_min, 1),
        })
        prev = (s["lat"], s["lon"])
    clock_override = _parse_start_clock_hint(day_state.get("start_clock") if isinstance(day_state, dict) else None)
    breaks_list = _normalize_breaks(day_state.get("breaks"))
    used_clock = _annotate_clock_times(fallback_stops, clock_override, breaks_list)
    return {
        "date": date_iso,
        "depot": {"lat": depot[0], "lon": depot[1]},
        "stops": fallback_stops,
        "geometry": None,
        "total_distance_km": round(sum(s["leg_km"] for s in fallback_stops), 2),
        "total_duration_min": round(cum_min, 1),
        "optimizer": "fallback",
        "approved": bool(day_state.get("approved")),
        "approved_at": day_state.get("approved_at"),
        "start_clock": f"{used_clock[0]:02d}:{used_clock[1]:02d}",
        "dwell_min": _DWELL_MIN,
        "breaks": breaks_list,
        "unresolved": unresolved,
    }


def _current_vehicle_position() -> Optional[dict]:
    vehicle_id = _state.get("active_vehicle_id")
    if not vehicle_id:
        return None
    client = _state.get("client")
    if not client or not client.is_connected():
        return None
    try:
        return client.get_position(vehicle_id)
    except (AbaxError, AbaxNotConnected):
        return None


@bp.post("/api/admin/tracking/route/reorder")
def admin_route_reorder():
    """Admin lagrer en custom stopp-rekkefølge for gitt dato.

    Body: {date: "YYYY-MM-DD", stop_order: ["123", "456", ...]}
    Lagring nullstiller "approved"-flagget — må godkjennes på nytt.
    """
    _, err = _admin_only()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    date_iso = (data.get("date") or _today_iso()).strip()
    stop_order = data.get("stop_order")
    if not isinstance(stop_order, list):
        return jsonify({"error": "stop_order must be a list of order_ids"}), 400
    _set_day_state(date_iso, {
        "stop_order": [str(s) for s in stop_order],
        "approved": False,
        "approved_at": None,
        "reordered_at": int(time.time()),
    })
    return jsonify({"ok": True, "date": date_iso})


@bp.post("/api/admin/tracking/route/schedule")
def admin_route_schedule():
    """Setter start-tid (første ankomst) og pauser mellom stopp for dato.

    Body (alt valgfritt — det som ikke sendes lar nåværende verdi stå):
      - date: "YYYY-MM-DD" (default: i dag)
      - start_clock: "HH:MM" — avreise fra depot
      - breaks: [{after_order_id, minutes, label?}, ...]
        after_order_id == 'depot' = pause før første stopp
      - clear_start: bool — nullstill manuell start_clock (auto-beregn igjen)

    Endringer nullstiller godkjenningen.
    """
    _, err = _admin_only()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    date_iso = (data.get("date") or _today_iso()).strip()
    updates: dict = {"approved": False, "approved_at": None}

    if data.get("clear_start"):
        updates["start_clock"] = None
    elif "start_clock" in data:
        sc = (data.get("start_clock") or "").strip()
        if sc:
            parsed = _parse_start_clock_hint(sc)
            if not parsed:
                return jsonify({"error": "start_clock must be HH:MM"}), 400
            updates["start_clock"] = f"{parsed[0]:02d}:{parsed[1]:02d}"
        else:
            updates["start_clock"] = None

    if "breaks" in data:
        updates["breaks"] = _normalize_breaks(data.get("breaks"))

    _set_day_state(date_iso, updates)
    return jsonify({"ok": True, "date": date_iso})


def _parse_start_time(value: str | None, fallback_stops: list[dict]) -> tuple[int, int]:
    """Returnerer (hour, minute) for ruteens start.

    1) Bruker eksplisitt "HH:MM" hvis gitt.
    2) Ellers prøver første stopps leveringstid (f.eks. "13:00-15:00" → 13:00).
    3) Faller tilbake til 14:00.
    """
    if value:
        import re
        m = re.match(r"^\s*(\d{1,2})[:.](\d{2})\s*$", str(value))
        if m:
            h, mn = int(m.group(1)), int(m.group(2))
            if 0 <= h <= 23 and 0 <= mn <= 59:
                return h, mn
    for s in fallback_stops or []:
        slot = (s.get("leveringstid") or "").strip()
        if slot:
            import re
            m = re.match(r"^\s*(\d{1,2})[:.](\d{2})", slot)
            if m:
                h, mn = int(m.group(1)), int(m.group(2))
                if 0 <= h <= 23 and 0 <= mn <= 59:
                    return h, mn
    return 14, 0


def _eta_clock_for_stop(start_hh: int, start_mm: int, cum_min: float) -> str:
    """Returnerer "HH:MM" for stoppet basert på start-tid + kumulativ kjøretid.

    Runder til nærmeste 5-min for å virke menneskelig ("ca kl 14:35", ikke "14:33").
    """
    total = start_hh * 60 + start_mm + int(round(cum_min or 0))
    # Rund til nærmeste 5 minutter
    total = int(round(total / 5)) * 5
    h = (total // 60) % 24
    m = total % 60
    return f"{h:02d}:{m:02d}"


@bp.post("/api/admin/tracking/route/approve")
def admin_route_approve():
    """Markerer dagens rute som godkjent (admin OK). Sender IKKE varsler —
    bruk /api/admin/tracking/route/notify for å sende e-post/SMS til kunder.

    Body (alt valgfritt):
      - date: "YYYY-MM-DD" (default: i dag)
      - start_time: "HH:MM" (lagres for senere notify-kall)
    """
    _, err = _admin_only()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    date_iso = (data.get("date") or _today_iso()).strip()
    start_time_arg = (data.get("start_time") or "").strip()

    now = int(time.time())
    updates: dict = {"approved": True, "approved_at": now}
    if start_time_arg:
        parsed = _parse_start_clock_hint(start_time_arg)
        if parsed:
            updates["start_clock"] = f"{parsed[0]:02d}:{parsed[1]:02d}"
    _set_day_state(date_iso, updates)
    return jsonify({"ok": True, "date": date_iso, "approved_at": now})


@bp.post("/api/admin/tracking/route/notify")
def admin_route_notify():
    """Sender e-post / SMS til alle kundene på dagens rute med deres estimerte
    leveringstid. Bruker `route_eta`-malen fra customer-notify-config så admin
    kan styre tekst på Varsel-siden.

    Body:
      - date: "YYYY-MM-DD" (default: i dag)
      - start_time: "HH:MM" (valgfritt — overskriver evt. lagret start_clock)
    """
    _, err = _admin_only()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    date_iso = (data.get("date") or _today_iso()).strip()
    start_time_arg = (data.get("start_time") or "").strip()

    notify_summary = {"sent": 0, "skipped": 0, "failed": 0, "details": []}
    route = _build_route(date_iso, optimize=True)
    stops = route.get("stops") or []
    if not stops:
        return jsonify({"ok": True, "date": date_iso, "notify": notify_summary,
                        "info": "ingen stopp denne datoen"})

    start_hh, start_mm = _parse_start_time(start_time_arg, stops)
    base = (_state["tracking_base_url"] or "").rstrip("/")
    breaks_list = _normalize_breaks(_get_day_state(date_iso).get("breaks"))
    _set_day_state(date_iso, {"start_clock": f"{start_hh:02d}:{start_mm:02d}"})

    eta_sender = _state.get("route_eta_sender")
    now = int(time.time())

    for i, stop in enumerate(stops):
        order_id = str(stop.get("order_id") or "")
        order = _find_order(order_id)
        if not order:
            notify_summary["skipped"] += 1
            notify_summary["details"].append({"order_id": order_id, "result": "order_not_found"})
            continue
        if not order.get("track_token"):
            order["track_token"] = secrets.token_urlsafe(20)
            order["track_token_created"] = now
        order.pop("proximity_notified_at", None)
        _clear_proximity_lock(order_id)

        cum = float(stop.get("cum_min_from_depot") or 0)
        cum += i * _DWELL_MIN
        cum += _break_min_before_stop(i, breaks_list, stops)
        eta_clock = _eta_clock_for_stop(start_hh, start_mm, cum)
        order["estimated_eta_clock"] = eta_clock

        track_url = f"{base}/track/{order_id}?token={order['track_token']}"

        # Bruk route_eta-mal hvis registrert. Fallback til gammel SMS-funksjon.
        if eta_sender:
            ok, detail = eta_sender(order, eta_clock, track_url)
        else:
            ok, detail = _send_track_link_sms(order, track_url, eta_clock=eta_clock)
        if ok:
            notify_summary["sent"] += 1
        elif detail in ("no-phone", "opted-out", "no-contact-info", "disabled-by-config"):
            notify_summary["skipped"] += 1
        else:
            notify_summary["failed"] += 1
        notify_summary["details"].append({
            "order_id": order_id, "eta_clock": eta_clock,
            "result": "ok" if ok else detail,
        })

    try:
        _state["save_state"]()
    except Exception as e:
        print(f"[NOTIFY] save_state feilet: {e}")

    return jsonify({"ok": True, "date": date_iso,
                    "start_time": f"{start_hh:02d}:{start_mm:02d}",
                    "notify": notify_summary})


@bp.post("/api/admin/tracking/route/reset")
def admin_route_reset():
    """Sletter custom-rekkefølge og godkjenning for dato → OSRM-optimaliseringen tar over igjen."""
    _, err = _admin_only()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    date_iso = (data.get("date") or _today_iso()).strip()
    state = _load_route_state()
    if date_iso in state:
        del state[date_iso]
        _save_route_state(state)
    return jsonify({"ok": True, "date": date_iso})


@bp.get("/api/admin/tracking/route/today")
def admin_route_today():
    """Bygger dagens optimaliserte leveringsrute fra depot."""
    _, err = _admin_only()
    if err:
        return err
    date_iso = (request.args.get("date") or _today_iso()).strip()
    route = _build_route(date_iso, optimize=True)
    route["vehicle"] = _current_vehicle_position()
    return jsonify(route)


@bp.get("/api/driver/route/today")
def driver_route_today():
    """Sjåfør-versjon av /api/admin/tracking/route/today. PIN-beskyttet.

    Returnerer den planlagte ruten uansett godkjenningsstatus, så sjåføren
    kan forhåndsvise dagens leveringer før admin godkjenner. Når ruten
    ikke er godkjent settes 'awaiting_approval' = True så app-en kan vise
    et tydelig "venter på godkjenning"-banner.
    """
    _, err = _driver_only()
    if err:
        return err
    date_iso = (request.args.get("date") or _today_iso()).strip()
    route = _build_route(date_iso, optimize=True)
    route["vehicle"] = _current_vehicle_position()
    route["awaiting_approval"] = not bool(route.get("approved"))
    return jsonify(route)


@bp.get("/api/driver/route/live")
def driver_route_live():
    """Sjåfør-versjon av live ETA-poll. PIN-beskyttet."""
    _, err = _driver_only()
    if err:
        return err
    return _route_live_payload(request.args.get("date") or _today_iso())


@bp.post("/api/driver/auth")
def driver_auth_check():
    """Sjåfør sender PIN — vi bekrefter, og returnerer sjåførens navn så appen
    kan vise 'Hei, <navn>'. Selve PINen må sendes på alle påfølgende requests."""
    data = request.get_json(silent=True) or {}
    provided = str(data.get("pin") or "").strip()
    driver = _match_driver_by_pin(provided)
    if not driver:
        return jsonify({"ok": False, "error": "wrong_pin"}), 401
    return jsonify({"ok": True, "name": driver.get("name"), "id": driver.get("id")})


# ── Sjåfør-CRUD (admin) ──────────────────────────────────────────────────

@bp.get("/api/admin/drivers")
def admin_list_drivers():
    _, err = _admin_only()
    if err:
        return err
    return jsonify({"drivers": _load_drivers()})


_PIN_LEN = 4


def _is_valid_pin(pin: str) -> bool:
    return bool(pin) and len(pin) == _PIN_LEN and pin.isdigit()


@bp.post("/api/admin/drivers")
def admin_create_driver():
    _, err = _admin_only()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    name = str(data.get("name") or "").strip()
    pin = str(data.get("pin") or "").strip()
    if not name:
        return jsonify({"error": "Navn er påkrevd"}), 400
    if not _is_valid_pin(pin):
        return jsonify({"error": f"PIN må være nøyaktig {_PIN_LEN} siffer"}), 400
    drivers = _load_drivers()
    if any(str(d.get("pin")) == pin for d in drivers):
        return jsonify({"error": "PIN allerede i bruk av en annen sjåfør"}), 409
    driver = {
        "id": secrets.token_urlsafe(8),
        "name": name,
        "pin": pin,
        "created": int(time.time()),
    }
    drivers.append(driver)
    _save_drivers(drivers)
    return jsonify({"ok": True, "driver": driver})


@bp.patch("/api/admin/drivers/<driver_id>")
def admin_update_driver(driver_id: str):
    _, err = _admin_only()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    drivers = _load_drivers()
    for d in drivers:
        if str(d.get("id")) == str(driver_id):
            if "name" in data:
                d["name"] = str(data["name"]).strip() or d.get("name", "")
            if "pin" in data:
                new_pin = str(data["pin"]).strip()
                if not _is_valid_pin(new_pin):
                    return jsonify({"error": f"PIN må være nøyaktig {_PIN_LEN} siffer"}), 400
                if any(str(o.get("pin")) == new_pin and str(o.get("id")) != driver_id
                       for o in drivers):
                    return jsonify({"error": "PIN allerede i bruk"}), 409
                d["pin"] = new_pin
            _save_drivers(drivers)
            return jsonify({"ok": True, "driver": d})
    return jsonify({"error": "ikke funnet"}), 404


@bp.delete("/api/admin/drivers/<driver_id>")
def admin_delete_driver(driver_id: str):
    _, err = _admin_only()
    if err:
        return err
    drivers = _load_drivers()
    filtered = [d for d in drivers if str(d.get("id")) != str(driver_id)]
    if len(filtered) == len(drivers):
        return jsonify({"error": "ikke funnet"}), 404
    _save_drivers(filtered)
    return jsonify({"ok": True})


# ── Live-poll cache: hindrer at hver poll trigger nye Google-API-kall ───
# Vehicle-position oppdateres ~30s av ABAX, så det er ingen mening i å kalle
# Distance Matrix oftere enn det. Vi cachar matrix-resultatet per (vehicle_lat,
# vehicle_lon-runded, date_iso) i 25 sekunder.
_LIVE_CACHE: dict = {}
_LIVE_CACHE_TTL = 25  # sek


def _route_live_payload(date_iso: str):
    """Felles live-ETA-payload: brukes både av admin- og sjåfør-endepunktet.

    Vi re-optimaliserer ikke her; vi gir bare oppdaterte ETAs fra bilens
    NÅ-posisjon til hver stopp. Frontend holder rekkefølgen mellom polls.
    """
    date_iso = (date_iso or _today_iso()).strip()
    raw_orders = _orders_for_date(date_iso)
    stops = []
    for o in raw_orders:
        dest = order_destination(o)
        if not dest:
            continue
        item = _stop_payload(o)
        item["lat"] = dest[0]
        item["lon"] = dest[1]
        stops.append(item)

    vehicle = _current_vehicle_position()
    etas_to_stops: list[dict] = []
    if vehicle and stops:
        # Cache-key: vehicle-posisjon rundet til ~50m (3 desimaler ≈ 100m) +
        # liste av order-ids. Hvis bilen ikke har flyttet seg mye og samme
        # ordresett, returner cached resultat (sparer Google-kall).
        cache_key = (
            round(vehicle["lat"], 3),
            round(vehicle["lon"], 3),
            date_iso,
            tuple(s["order_id"] for s in stops),
        )
        cached = _LIVE_CACHE.get(cache_key)
        now = time.time()
        if cached and (now - cached[0]) < _LIVE_CACHE_TTL:
            etas_to_stops = cached[1]
        else:
            dests = [(s["lat"], s["lon"]) for s in stops]
            table = matrix_one_to_many((vehicle["lat"], vehicle["lon"]), dests)
            if table:
                etas_to_stops = table
            else:
                etas_to_stops = [
                    fallback_eta(vehicle["lat"], vehicle["lon"], s["lat"], s["lon"])
                    for s in stops
                ]
            _LIVE_CACHE[cache_key] = (now, etas_to_stops)
            # Rydd opp gamle cache-entries så dict ikke vokser ubegrenset
            for k in list(_LIVE_CACHE.keys()):
                if (now - _LIVE_CACHE[k][0]) > _LIVE_CACHE_TTL * 4:
                    del _LIVE_CACHE[k]

    eta_by_order: dict[str, dict] = {}
    for s, eta in zip(stops, etas_to_stops):
        eta_by_order[s["order_id"]] = {
            "duration_min": eta.get("duration_min"),
            "distance_km": eta.get("distance_km"),
            "minutes": int(round(eta.get("duration_min") or 0)),
        }

    return jsonify({
        "date": date_iso,
        "vehicle": vehicle,
        "eta_by_order": eta_by_order,
    })


@bp.get("/api/admin/tracking/route/live")
def admin_route_live():
    """Live ETA-poll for admin."""
    _, err = _admin_only()
    if err:
        return err
    return _route_live_payload(request.args.get("date") or _today_iso())


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
