"""
Havøyet AS — Flask backend
Kjøres på Raspberry Pi 5. Henter ordre fra Shopify Admin API og
eksponerer dem for frontend via /api/orders.

Start: python3 app.py
Krav:  pip install flask flask-cors requests
"""

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import requests
import threading
import time
import json
import os
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)  # Tillat kall fra HTML-filer åpnet lokalt

# ── KONFIG ────────────────────────────────────────────────────────────────────
# Les fra .env-fil om den finnes
_env_file = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(_env_file):
    with open(_env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

SHOPIFY_SHOP    = os.environ.get("SHOPIFY_SHOP",  "havoyet.myshopify.com")
SHOPIFY_TOKEN   = os.environ.get("SHOPIFY_TOKEN", "")
SHOPIFY_VERSION = os.environ.get("SHOPIFY_API_VERSION", "2024-01")
POLL_INTERVAL   = int(os.environ.get("POLL_INTERVAL", "300"))  # sekunder (5 min)

PORT       = int(os.environ.get("PORT", 5001))
CACHE_FILE = os.path.join("/tmp", "havoyet_orders_cache.json")

# In-memory cache
_cache = {
    "orders": [],
    "last_sync": None,
    "error": None,
}

# ── SHOPIFY-HENTING ────────────────────────────────────────────────────────────
def shopify_get(endpoint, params=None):
    url = f"https://{SHOPIFY_SHOP}/admin/api/{SHOPIFY_VERSION}/{endpoint}"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json",
    }
    r = requests.get(url, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def extract_delivery_info(order):
    """
    Henter leveringsdato og tidsluke fra order.note_attributes.
    Bird Pickup Delivery-appen bruker følgende nøkler:
      - "Delivery Date"            → f.eks. "Apr 13, 2026"
      - "Translated Delivery Time" → f.eks. "13:00 - 15:00" eller "15:00 - 18:00"
    """
    delivery_date = None
    delivery_slot = None

    attrs = {a.get("name", ""): a.get("value", "").strip()
             for a in order.get("note_attributes", [])}

    # 1. Leveringsdato: "Delivery Date" → "Apr 13, 2026" → "2026-04-13"
    raw_date = attrs.get("Delivery Date", "")
    if raw_date:
        try:
            parsed = datetime.strptime(raw_date, "%b %d, %Y")
            delivery_date = (parsed - timedelta(days=1)).strftime("%Y-%m-%d")
        except ValueError:
            pass

    # 2. Fallback: prøv norske nøkler (DD.MM.YYYY)
    if not delivery_date:
        for key in ("Leveringsdato", "leveringsdato", "delivery_date"):
            raw = attrs.get(key, "")
            if raw:
                if "." in raw:
                    parts = raw.split(".")
                    if len(parts) == 3:
                        try:
                            delivery_date = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
                        except Exception:
                            pass
                else:
                    delivery_date = raw
                break

    # 3. Siste fallback: dagens dato
    if not delivery_date:
        delivery_date = datetime.now().strftime("%Y-%m-%d")

    # 4. Tidsluke: "Translated Delivery Time" → "15:00 - 18:00"
    raw_time = attrs.get("Translated Delivery Time", "") or attrs.get("Delivery Time", "")
    if raw_time:
        # Normaliser til 24-timers format hvis nødvendig (f.eks. "3:00 PM")
        if "PM" in raw_time or "AM" in raw_time:
            pass  # bruk start-timetallet etter konvertering nedenfor
        start = raw_time.split("-")[0].strip().replace("PM", "").replace("AM", "").strip()
        try:
            hour = int(start.split(":")[0])
            if "PM" in raw_time and hour != 12:
                hour += 12
            delivery_slot = "a" if hour < 15 else "b"
        except (ValueError, IndexError):
            delivery_slot = "a"
    else:
        delivery_slot = "a"

    return delivery_date, delivery_slot


def map_order(order):
    """Mapper Shopify-ordre til appens interne datamodell."""
    customer = order.get("customer") or {}
    shipping = order.get("shipping_address") or {}

    first = customer.get("first_name") or shipping.get("first_name") or ""
    last  = customer.get("last_name")  or shipping.get("last_name")  or ""
    name  = f"{first} {last}".strip() or customer.get("email", "Ukjent")

    delivery_date, delivery_slot = extract_delivery_info(order)

    items = []
    for li in order.get("line_items", []):
        items.append({
            "id":       li.get("id"),
            "name":     li.get("title", ""),
            "quantity": li.get("quantity", 1),
            "weight":   None,   # Fylles inn i Pakke & Merk
            "expiry":   None,   # Fylles inn i Pakke & Merk
            "variant":  li.get("variant_title"),
            "sku":      li.get("sku"),
            "grams":    li.get("grams", 0),
        })

    # Status-mapping
    fs = order.get("fulfillment_status") or ""
    fin = order.get("financial_status") or ""
    if fs == "fulfilled":
        status = "DONE"
    elif fs in ("partial", "in_progress"):
        status = "IN_PROGRESS"
    else:
        status = "NEW"

    return {
        "id":           order.get("name", str(order.get("id"))),
        "shopify_id":   order.get("id"),
        "customer":     name,
        "email":        customer.get("email", ""),
        "phone":        customer.get("phone") or shipping.get("phone") or "",
        "delivery":     delivery_date,
        "slot":         delivery_slot,
        "status":       status,
        "items":        items,
        "note":         order.get("note") or "",
        "financial":    fin,
        "created_at":   order.get("created_at", ""),
    }


def fetch_orders():
    """Henter alle åpne ordre fra Shopify og oppdaterer cache."""
    global _cache
    try:
        data = shopify_get("orders.json", params={
            "status": "open",
            "limit":  250,
            "fields": "id,name,customer,line_items,note_attributes,note,"
                      "financial_status,fulfillment_status,created_at,"
                      "shipping_address",
        })
        orders = [map_order(o) for o in data.get("orders", [])]

        # Sorter: nærmeste leveringsdato øverst
        orders.sort(key=lambda o: o.get("delivery") or "9999-99-99")

        _cache["orders"]    = orders
        _cache["last_sync"] = datetime.now().isoformat()
        _cache["error"]     = None

        # Lagre til disk slik at frontend kan laste ved oppstart
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(_cache, f, ensure_ascii=False, indent=2)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Hentet {len(orders)} ordre fra Shopify")
        return orders

    except requests.exceptions.HTTPError as e:
        msg = f"Shopify HTTP-feil: {e.response.status_code} — {e.response.text[:200]}"
        _cache["error"] = msg
        print(f"[FEIL] {msg}")
    except Exception as e:
        _cache["error"] = str(e)
        print(f"[FEIL] {e}")

    return _cache.get("orders", [])


def poll_loop():
    """Bakgrunns-tråd som poller Shopify hvert POLL_INTERVAL sekund."""
    while True:
        fetch_orders()
        time.sleep(POLL_INTERVAL)


# ── API-ENDEPUNKTER ────────────────────────────────────────────────────────────

# ── SERVE HTML-FILER (iPad / andre enheter på samme WiFi) ─────────────────────
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

@app.route("/")
def serve_index():
    return send_from_directory(_BASE_DIR, "index.html")

@app.route("/<path:filename>")
def serve_static(filename):
    return send_from_directory(_BASE_DIR, filename)

# ── API ────────────────────────────────────────────────────────────────────────
@app.route("/api/orders")
def api_orders():
    return jsonify({
        "orders":    _cache["orders"],
        "last_sync": _cache["last_sync"],
        "error":     _cache["error"],
        "count":     len(_cache["orders"]),
    })


@app.route("/api/orders/<order_id>")
def api_order(order_id):
    order = next((o for o in _cache["orders"]
                  if o["id"] == order_id or str(o.get("shopify_id")) == order_id), None)
    if not order:
        return jsonify({"error": "Ikke funnet"}), 404
    return jsonify(order)


@app.route("/api/sync", methods=["POST"])
def api_sync():
    """Manuell synk-trigger fra frontend."""
    orders = fetch_orders()
    return jsonify({
        "ok":        True,
        "count":     len(orders),
        "last_sync": _cache["last_sync"],
        "error":     _cache["error"],
    })


@app.route("/api/debug/order/<shopify_id>")
def api_debug_order(shopify_id):
    """Returnerer rå note_attributes og shipping_lines for én ordre."""
    try:
        data = shopify_get(f"orders/{shopify_id}.json", params={
            "fields": "id,name,note,note_attributes,shipping_lines,delivery_instructions"
        })
        o = data.get("order", {})
        return jsonify({
            "id":              o.get("id"),
            "name":            o.get("name"),
            "note":            o.get("note"),
            "note_attributes": o.get("note_attributes", []),
            "shipping_lines":  o.get("shipping_lines", []),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/status")
def api_status():
    return jsonify({
        "shop":      SHOPIFY_SHOP,
        "last_sync": _cache["last_sync"],
        "count":     len(_cache["orders"]),
        "error":     _cache["error"],
        "poll_interval_sec": POLL_INTERVAL,
    })


_admin_state = {}

@app.route("/api/admin/state", methods=["GET", "POST"])
def api_admin_state():
    global _admin_state
    if request.method == "POST":
        _admin_state = request.get_json(force=True)
        return jsonify({"ok": True})
    return jsonify(_admin_state)


# ── CROSS-DEVICE SYNC ──────────────────────────────────────────────────────────
_manual_orders = []
_hidden_orders = []
_overrides = {}
_packing_state = {}
_order_notes = {}


@app.route("/api/manual-orders", methods=["GET", "POST"])
def api_manual_orders():
    global _manual_orders
    if request.method == "POST":
        _manual_orders = request.get_json(force=True) or []
        return jsonify({"ok": True, "count": len(_manual_orders)})
    return jsonify(_manual_orders)


@app.route("/api/manual-orders/<order_id>", methods=["DELETE"])
def api_delete_manual_order(order_id):
    global _manual_orders
    before = len(_manual_orders)
    _manual_orders = [o for o in _manual_orders if str(o.get("id")) != str(order_id)]
    return jsonify({"ok": True, "removed": before - len(_manual_orders)})


@app.route("/api/hidden-orders", methods=["GET", "POST"])
def api_hidden_orders():
    global _hidden_orders
    if request.method == "POST":
        _hidden_orders = request.get_json(force=True) or []
        return jsonify({"ok": True, "count": len(_hidden_orders)})
    return jsonify(_hidden_orders)


@app.route("/api/overrides", methods=["GET", "POST"])
def api_overrides():
    global _overrides
    if request.method == "POST":
        _overrides = request.get_json(force=True) or {}
        return jsonify({"ok": True})
    return jsonify(_overrides)


@app.route("/api/packing-state", methods=["GET", "POST"])
def api_packing_state():
    global _packing_state
    if request.method == "POST":
        _packing_state = request.get_json(force=True) or {}
        return jsonify({"ok": True})
    return jsonify(_packing_state)


@app.route("/api/notes", methods=["GET", "POST"])
def api_notes():
    global _order_notes
    if request.method == "POST":
        _order_notes = request.get_json(force=True) or {}
        return jsonify({"ok": True})
    return jsonify(_order_notes)


# ── OPPSTART ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Last cache fra disk ved oppstart (om den finnes)
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                _cache = json.load(f)
            print(f"Lastet {len(_cache.get('orders', []))} ordre fra disk-cache")
        except Exception:
            pass

    # Start poller i bakgrunnen
    t = threading.Thread(target=poll_loop, daemon=True)
    t.start()
    print(f"Havøyet backend starter — http://0.0.0.0:5000")
    print(f"Shopify: {SHOPIFY_SHOP} | Poll: hvert {POLL_INTERVAL}s")

    is_cloud = os.environ.get("RENDER") or os.environ.get("RAILWAY_ENVIRONMENT")
    app.run(host="0.0.0.0", port=PORT, debug=not is_cloud, use_reloader=not is_cloud)
