"""
Havøyet AS — Flask backend
Kjøres på Raspberry Pi 5. Henter ordre fra Shopify Admin API og
eksponerer dem for frontend via /api/orders.

Start: python3 app.py
Krav:  pip install flask flask-cors requests
"""

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
import requests
import threading
import time
import json
import os
import secrets
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

PORT            = int(os.environ.get("PORT", 5001))
CACHE_FILE      = os.path.join("/tmp", "havoyet_orders_cache.json")
SYNC_STATE_FILE = os.path.join("/tmp", "havoyet_sync_state.json")

# PowerOffice GO
POWEROFFICE_CLIENT_ID     = os.environ.get("POWEROFFICE_CLIENT_ID", "")
POWEROFFICE_CLIENT_SECRET = os.environ.get("POWEROFFICE_CLIENT_SECRET", "")
POWEROFFICE_API           = "https://api.poweroffice.net"
POWEROFFICE_SUPPLIER      = os.environ.get("POWEROFFICE_SUPPLIER_FILTER", "domstein")
PRISLISTE_FILE            = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prisliste_domstein.json")

# Vipps ePayment API — alle hemmeligheter MÅ komme fra .env / Render env vars.
# Sett VIPPS_TEST_MODE=1 for å bruke Vipps sandkasse (apitest.vipps.no).
VIPPS_CLIENT_ID         = os.environ.get("VIPPS_CLIENT_ID", "")
VIPPS_CLIENT_SECRET     = os.environ.get("VIPPS_CLIENT_SECRET", "")
VIPPS_SUBSCRIPTION_KEY  = os.environ.get("VIPPS_SUBSCRIPTION_KEY", "")
VIPPS_MSN               = os.environ.get("VIPPS_MSN", "")             # Merchant Serial Number
VIPPS_TEST_MODE         = os.environ.get("VIPPS_TEST_MODE", "0") == "1"
VIPPS_API_BASE          = "https://apitest.vipps.no" if VIPPS_TEST_MODE else "https://api.vipps.no"
VIPPS_PAYMENTS_FILE     = os.path.join("/tmp", "havoyet_vipps_payments.json")
_vipps_token_cache      = {"access_token": None, "expires_at": 0.0}

# In-memory cache
_cache = {
    "orders": [],
    "last_sync": None,
    "error": None,
}

# PowerOffice token-cache
_po_token = {"access_token": None, "expires_at": 0.0}

# Prisliste-cache
_prisliste = {"items": [], "last_sync": None, "error": None, "faktura": None}

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
    global _cache, _shopify_seen_ids
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

        # Detekter nye Shopify-ordre. Første gang vi henter ordre i denne
        # prosessen seedes settet uten å varsle (unngår å spamme alle åpne).
        current_ids = {str(o.get("id")) for o in orders if o.get("id")}
        if _shopify_seen_ids:
            new_ids = current_ids - _shopify_seen_ids
            for o in orders:
                if str(o.get("id")) in new_ids:
                    nr = o.get("name") or o.get("id") or "?"
                    _notify_admins(
                        "new_order",
                        f"[Havøyet] Ny bestilling {nr} (Shopify)",
                        "Ny ordre kom inn fra Shopify.\n"
                        + "=" * 54 + "\n\n"
                        + _format_order_lines(o),
                    )
        _shopify_seen_ids = current_ids

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


# ── POWEROFFICE INTEGRASJON ────────────────────────────────────────────────────

def poweroffice_token():
    """Henter OAuth2-token fra PowerOffice GO med client credentials."""
    import time as _time
    if _po_token["access_token"] and _time.time() < _po_token["expires_at"] - 30:
        return _po_token["access_token"]

    if not POWEROFFICE_CLIENT_ID or not POWEROFFICE_CLIENT_SECRET:
        raise ValueError("POWEROFFICE_CLIENT_ID / POWEROFFICE_CLIENT_SECRET mangler i .env")

    r = requests.post(
        f"{POWEROFFICE_API}/OAuth/Token",
        data={
            "grant_type":    "client_credentials",
            "client_id":     POWEROFFICE_CLIENT_ID,
            "client_secret": POWEROFFICE_CLIENT_SECRET,
        },
        timeout=15,
    )
    r.raise_for_status()
    d = r.json()
    _po_token["access_token"] = d["access_token"]
    _po_token["expires_at"]   = _time.time() + int(d.get("expires_in", 3600))
    return _po_token["access_token"]


def fetch_domstein_prisliste():
    """Henter siste faktura fra Domstein i PowerOffice GO og bygger prisliste."""
    global _prisliste
    try:
        token   = poweroffice_token()
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        # Hent leverandørfakturaer (siste 90 dager)
        since = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00")
        r = requests.get(
            f"{POWEROFFICE_API}/SupplierInvoice",
            headers=headers,
            params={"createdFromDate": since, "pageSize": 100},
            timeout=20,
        )
        r.raise_for_status()
        raw = r.json()
        invoices = raw.get("data", raw) if isinstance(raw, dict) else raw

        # Filtrer på leverandørnavn som inneholder søkeordet
        keyword = POWEROFFICE_SUPPLIER.lower()
        domstein_invs = [
            inv for inv in invoices
            if keyword in (inv.get("supplierName") or "").lower()
            or keyword in (inv.get("contactName")  or "").lower()
        ]

        if not domstein_invs:
            _prisliste["error"] = f"Ingen fakturaer fra '{POWEROFFICE_SUPPLIER}' funnet (siste 90 dager)"
            return

        # Bruk siste faktura
        domstein_invs.sort(key=lambda x: x.get("invoiceDate") or "", reverse=True)
        siste = domstein_invs[0]

        # Hent fakturalinjene
        inv_id = siste.get("id")
        r2 = requests.get(
            f"{POWEROFFICE_API}/SupplierInvoice/{inv_id}",
            headers=headers,
            timeout=15,
        )
        r2.raise_for_status()
        detalj = r2.json()
        if isinstance(detalj, dict) and "data" in detalj:
            detalj = detalj["data"]

        lines = (detalj.get("lines")
                 or detalj.get("vouchers")
                 or detalj.get("lineItems")
                 or [])

        items = []
        for line in lines:
            desc   = (line.get("description") or line.get("productName") or "").strip()
            pris   = float(line.get("unitPrice") or line.get("unitCost") or 0)
            antall = line.get("quantity") or 1
            enhet  = (line.get("unit") or line.get("unitOfMeasure") or "stk").strip()
            varenr = (line.get("productCode") or line.get("itemCode") or "").strip()
            if desc:
                items.append({
                    "varenr":      varenr,
                    "beskrivelse": desc,
                    "pris":        round(pris, 2),
                    "antall":      antall,
                    "enhet":       enhet,
                })

        _prisliste["items"]    = items
        _prisliste["last_sync"] = datetime.now().isoformat()
        _prisliste["error"]    = None
        _prisliste["faktura"]  = {
            "id":     siste.get("id"),
            "dato":   siste.get("invoiceDate"),
            "nummer": siste.get("invoiceNumber") or siste.get("supplierInvoiceNumber"),
            "belop":  siste.get("grossAmount") or siste.get("totalAmount"),
            "lev":    siste.get("supplierName") or siste.get("contactName"),
        }

        with open(PRISLISTE_FILE, "w", encoding="utf-8") as f:
            json.dump(_prisliste, f, ensure_ascii=False, indent=2)

        print(f"[PowerOffice] Hentet {len(items)} varelinjer fra {_prisliste['faktura']['lev']}"
              f" faktura {_prisliste['faktura']['nummer']}")

    except Exception as e:
        _prisliste["error"] = str(e)
        print(f"[PowerOffice FEIL] {e}")


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
_product_overrides = {}
_reviews = []  # [{id, slug, name, rating, text, date}]
_customer_favorites = {}  # email → [slug, slug, ...]
_admin_notifiers = []  # [{id, name, email, events:[...], created_at}]
_customers = []  # [{id, navn, tlf, epost, adresse, kommentar, created_at}]
_auth_users = []   # [{email, role, password_hash, must_set_password, created_at}]
_auth_sessions = {}  # token → {email, role, created_at} — i minnet kun
_shopify_seen_ids = set()  # spor sett Shopify-ordre for å oppdage nye i poll-loop

def _save_sync_state():
    """Persist cross-device sync state to disk."""
    try:
        with open(SYNC_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "manual_orders":     _manual_orders,
                "hidden_orders":     _hidden_orders,
                "overrides":         _overrides,
                "packing_state":     _packing_state,
                "order_notes":       _order_notes,
                "product_overrides":   _product_overrides,
                "reviews":             _reviews,
                "customer_favorites":  _customer_favorites,
                "admin_notifiers":     _admin_notifiers,
                "customers":           _customers,
                "auth_users":          _auth_users,
            }, f, ensure_ascii=False)
    except Exception:
        pass

def _load_sync_state():
    """Load cross-device sync state from disk on startup."""
    global _manual_orders, _hidden_orders, _overrides, _packing_state, _order_notes, _product_overrides, _reviews, _customer_favorites, _admin_notifiers, _customers, _auth_users
    if not os.path.exists(SYNC_STATE_FILE):
        _seed_auth_users()
        return
    try:
        with open(SYNC_STATE_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        _manual_orders     = d.get("manual_orders", [])
        _hidden_orders     = d.get("hidden_orders", [])
        _overrides         = d.get("overrides", {})
        _packing_state     = d.get("packing_state", {})
        _order_notes       = d.get("order_notes", {})
        _product_overrides = d.get("product_overrides", {})
        _reviews            = d.get("reviews", [])
        _customer_favorites = d.get("customer_favorites", {})
        _admin_notifiers    = d.get("admin_notifiers", [])
        _customers          = d.get("customers", [])
        _auth_users         = d.get("auth_users", [])
        _seed_auth_users()
        print(f"Lastet sync-state fra disk: {len(_packing_state)} pakket, {len(_manual_orders)} manuelle ordre, {len(_product_overrides)} produkt-overrides, {len(_reviews)} anmeldelser, {len(_admin_notifiers)} admin-mottakere, {len(_customers)} kunder, {len(_auth_users)} auth-brukere")
    except Exception as e:
        print(f"[ADVARSEL] Kunne ikke laste sync-state: {e}")
        _seed_auth_users()


@app.route("/api/manual-orders", methods=["GET", "POST"])
def api_manual_orders():
    global _manual_orders
    if request.method == "POST":
        old_ids = {str(o.get("ordrenr") or o.get("id")) for o in _manual_orders}
        new_list = request.get_json(force=True) or []
        # Finn ordre som er nye i innkommende liste
        added = [o for o in new_list
                 if str(o.get("ordrenr") or o.get("id")) not in old_ids]
        _manual_orders = new_list
        _save_sync_state()
        for o in added:
            nr = o.get("ordrenr") or o.get("id") or "?"
            _notify_admins(
                "new_order",
                f"[Havøyet] Ny bestilling #{nr}",
                "Det er kommet inn en ny bestilling.\n"
                + "=" * 54 + "\n\n"
                + _format_order_lines(o),
            )
        return jsonify({"ok": True, "count": len(_manual_orders)})
    return jsonify(_manual_orders)


@app.route("/api/manual-orders/<order_id>", methods=["DELETE"])
def api_delete_manual_order(order_id):
    global _manual_orders
    before = len(_manual_orders)
    _manual_orders = [o for o in _manual_orders if str(o.get("id")) != str(order_id)]
    _save_sync_state()
    return jsonify({"ok": True, "removed": before - len(_manual_orders)})


@app.route("/api/hidden-orders", methods=["GET", "POST"])
def api_hidden_orders():
    global _hidden_orders
    if request.method == "POST":
        _hidden_orders = request.get_json(force=True) or []
        _save_sync_state()
        return jsonify({"ok": True, "count": len(_hidden_orders)})
    return jsonify(_hidden_orders)


@app.route("/api/overrides", methods=["GET", "POST"])
def api_overrides():
    global _overrides
    if request.method == "POST":
        _overrides = request.get_json(force=True) or {}
        _save_sync_state()
        return jsonify({"ok": True})
    return jsonify(_overrides)


@app.route("/api/packing-state", methods=["GET", "POST"])
def api_packing_state():
    global _packing_state
    if request.method == "POST":
        _packing_state = request.get_json(force=True) or {}
        _save_sync_state()
        return jsonify({"ok": True})
    return jsonify(_packing_state)


@app.route("/api/prisliste")
def api_prisliste():
    return jsonify(_prisliste)


@app.route("/api/prisliste/sync", methods=["POST"])
def api_prisliste_sync():
    fetch_domstein_prisliste()
    return jsonify(_prisliste)


@app.route("/api/notes", methods=["GET", "POST"])
def api_notes():
    global _order_notes
    if request.method == "POST":
        _order_notes = request.get_json(force=True) or {}
        _save_sync_state()
        return jsonify({"ok": True})
    return jsonify(_order_notes)


# ── PRODUKT-OVERRIDES (nettside ↔ admin på tvers av enheter) ───────────────────
@app.route("/api/products/overrides", methods=["GET", "POST"])
def api_product_overrides():
    """GET returnerer hele overrides-dict {slug: patch}. POST erstatter alt."""
    global _product_overrides
    if request.method == "POST":
        data = request.get_json(force=True)
        if not isinstance(data, dict):
            return jsonify({"error": "Forventer JSON-objekt"}), 400
        _product_overrides = data
        _save_sync_state()
        return jsonify({"ok": True, "count": len(_product_overrides)})
    return jsonify(_product_overrides)


@app.route("/api/products/overrides/<slug>", methods=["PATCH", "DELETE"])
def api_product_override(slug):
    """PATCH merger patch inn i overrides[slug]. DELETE fjerner slug."""
    global _product_overrides
    if request.method == "DELETE":
        removed = _product_overrides.pop(slug, None)
        _save_sync_state()
        return jsonify({"ok": True, "removed": removed is not None})
    patch = request.get_json(force=True)
    if not isinstance(patch, dict):
        return jsonify({"error": "Forventer JSON-objekt"}), 400
    existing = _product_overrides.get(slug, {})
    existing.update(patch)
    _product_overrides[slug] = existing
    _save_sync_state()
    return jsonify({"ok": True, "slug": slug, "override": existing})


# ── KUNDEANMELDELSER ────────────────────────────────────────────────────────
import uuid as _uuid

@app.route("/api/reviews", methods=["GET", "POST"])
def api_reviews():
    """GET returnerer alle anmeldelser (valgfritt filter ?slug=...).
    POST legger til ny anmeldelse {slug, name, rating, text}."""
    global _reviews
    if request.method == "POST":
        data = request.get_json(force=True) or {}
        text = (data.get("text") or "").strip()
        if not text:
            return jsonify({"error": "Tekst er påkrevet"}), 400
        try:
            rating = int(data.get("rating", 5))
        except (TypeError, ValueError):
            rating = 5
        rating = max(1, min(5, rating))
        review = {
            "id":     _uuid.uuid4().hex[:10],
            "slug":   (data.get("slug") or "").strip(),
            "name":   (data.get("name") or "Anonym").strip()[:80] or "Anonym",
            "rating": rating,
            "text":   text[:2000],
            "date":   datetime.now().strftime("%Y-%m-%d"),
        }
        _reviews.append(review)
        _save_sync_state()
        return jsonify({"ok": True, "review": review})
    # GET
    slug = request.args.get("slug")
    items = [r for r in _reviews if not slug or r.get("slug") == slug]
    avg = (sum(r["rating"] for r in items) / len(items)) if items else 0
    return jsonify({"reviews": list(reversed(items)), "count": len(items), "avg": round(avg, 2)})


@app.route("/api/reviews/<review_id>", methods=["DELETE"])
def api_review_delete(review_id):
    """Admin-sletting av upassende anmeldelser."""
    global _reviews
    before = len(_reviews)
    _reviews = [r for r in _reviews if r.get("id") != review_id]
    _save_sync_state()
    return jsonify({"ok": True, "removed": before - len(_reviews)})


# ── KONTAKT-MAIL (sendes til erik@havoyet.no) ───────────────────────────────
import smtplib as _smtplib
from email.mime.text import MIMEText as _MIMEText
from email.mime.multipart import MIMEMultipart as _MIMEMultipart
from email.utils import formataddr as _formataddr

CONTACT_TO       = os.environ.get("CONTACT_TO", "erik@havoyet.no")
# Resend (anbefalt — https://resend.com) — sett RESEND_API_KEY i .env
RESEND_API_KEY   = os.environ.get("RESEND_API_KEY", "")
RESEND_FROM      = os.environ.get("RESEND_FROM", "onboarding@resend.dev")  # default test-adresse
# SMTP (alternativ — Gmail o.l.)
SMTP_HOST        = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT        = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER        = os.environ.get("SMTP_USER", "")
SMTP_PASS        = os.environ.get("SMTP_PASS", "")
CONTACT_LOG_FILE = os.path.join(os.path.dirname(_BASE_DIR), "contact_messages.jsonl")


def _send_via_resend(from_email, from_name, subject, body, to_email=None, reply_to=None):
    """Send via Resend API (enklest — bare API-nøkkel trengs)."""
    try:
        payload = {
            "from": f"Havøyet nettside <{RESEND_FROM}>",
            "to": [to_email or CONTACT_TO],
            "subject": subject,
            "text": body,
        }
        if reply_to or from_email:
            payload["reply_to"] = reply_to or from_email
        r = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=15,
        )
        if r.status_code in (200, 201):
            return True, "sent-via-resend"
        return False, f"resend-{r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, f"resend-exception: {e}"


def _send_via_smtp(from_email, from_name, subject, body, to_email=None, reply_to=None):
    """Send via SMTP (Gmail / annen SMTP-server)."""
    recipient = to_email or CONTACT_TO
    msg = _MIMEMultipart()
    msg["From"]     = _formataddr((f"Havøyet – {from_name}", SMTP_USER))
    if reply_to or from_email:
        msg["Reply-To"] = reply_to or from_email
    msg["To"]       = recipient
    msg["Subject"]  = subject
    msg.attach(_MIMEText(body, "plain", "utf-8"))
    try:
        with _smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_USER, [recipient], msg.as_string())
        return True, "sent-via-smtp"
    except Exception as e:
        return False, f"smtp-exception: {e}"


def _send_contact_mail(from_email, from_name, subject, body):
    """Prøv Resend først, så SMTP, og logg alltid til disk som backup."""
    # Alltid logg til disk (backup)
    try:
        with open(CONTACT_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "at": datetime.now().isoformat(),
                "from": from_email, "name": from_name,
                "subject": subject, "body": body, "to": CONTACT_TO,
            }, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[CONTACT] Kunne ikke logge: {e}")

    # Prøv Resend først (enklere)
    if RESEND_API_KEY:
        ok, detail = _send_via_resend(from_email, from_name, subject, body)
        print(f"[CONTACT] Resend: {detail}")
        if ok:
            return True, detail

    # Fallback: SMTP
    if SMTP_USER and SMTP_PASS:
        ok, detail = _send_via_smtp(from_email, from_name, subject, body)
        print(f"[CONTACT] SMTP: {detail}")
        if ok:
            return True, detail

    # Ingen sending konfigurert
    print(f"[CONTACT] Ingen mail-tjeneste konfigurert — meldingen ble logget til {CONTACT_LOG_FILE}")
    print(f"[CONTACT] Sett RESEND_API_KEY (anbefalt) eller SMTP_USER/SMTP_PASS i .env")
    return True, "logged-only"


# ── ADMIN-VARSLER ──────────────────────────────────────────────────────────────
# Send e-post + SMS til registrerte admin-mottakere ved nye/oppdaterte/leverte
# ordre og innkommende kontaktmeldinger.
ADMIN_EVENTS = ("new_order", "order_updated", "order_delivered", "new_message")
ADMIN_NOTIFY_LOG = os.path.join(os.path.dirname(_BASE_DIR), "admin_notifications.jsonl")

# Twilio (valgfritt — sett env-vars for å aktivere SMS)
TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN  = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM        = os.environ.get("TWILIO_FROM", "")  # f.eks. "+4790000000"

# ntfy.sh — gratis push-varsel til mobil. Mottaker installerer ntfy-appen og
# abonnerer på sin egen hemmelige topic. Default-server er ntfy.sh; kan
# overstyres via NTFY_SERVER for selv-hostet versjon.
NTFY_SERVER = os.environ.get("NTFY_SERVER", "https://ntfy.sh").rstrip("/")


def _normalize_phone(raw):
    """Normaliser norsk telefonnr til E.164-format (+47XXXXXXXX).

    Aksepterer: '+4790000000', '4790000000', '90000000', '900 00 000',
    '+47 900 00 000'. Returnerer None hvis ugyldig.
    """
    if not raw:
        return None
    digits = "".join(c for c in str(raw) if c.isdigit() or c == "+")
    if digits.startswith("+"):
        if len(digits) >= 9:
            return digits
        return None
    if digits.startswith("00"):
        digits = "+" + digits[2:]
        return digits if len(digits) >= 9 else None
    if len(digits) == 8:
        return "+47" + digits
    if len(digits) == 10 and digits.startswith("47"):
        return "+" + digits
    return None


def _send_admin_mail(to_email, subject, body):
    """Send én e-post til en admin-mottaker. Bruker Resend → SMTP → log."""
    if RESEND_API_KEY:
        ok, detail = _send_via_resend("", "Admin-varsel", subject, body, to_email=to_email)
        if ok:
            return True, detail
    if SMTP_USER and SMTP_PASS:
        ok, detail = _send_via_smtp("", "Admin-varsel", subject, body, to_email=to_email)
        if ok:
            return True, detail
    return False, "no-mail-service"


def _normalize_ntfy_topic(raw):
    """Trekk ut topic fra rå input. Aksepterer 'topic', 'ntfy.sh/topic',
    'https://ntfy.sh/topic'. Returnerer None hvis ugyldig."""
    if not raw:
        return None
    t = str(raw).strip()
    # Strip skjema og verts-prefiks
    for prefix in ("https://", "http://", "ntfy://"):
        if t.startswith(prefix):
            t = t[len(prefix):]
    if "/" in t:
        t = t.split("/", 1)[1]
    # ntfy-topics: bokstaver, tall, _, -. 1–64 tegn.
    import re as _re
    if not _re.fullmatch(r"[A-Za-z0-9_-]{1,64}", t):
        return None
    return t


def _send_admin_push(topic, subject, body):
    """Send push-varsel via ntfy.sh. Trenger ingen konto eller credentials."""
    norm = _normalize_ntfy_topic(topic)
    if not norm:
        return False, "ntfy-invalid-topic"
    try:
        # ntfy støtter både rå body med Title-header og JSON. Bruker headers
        # for enkelhet. Klipper body til 4 KB for å holde nyttelasten lav.
        payload = (body or "").encode("utf-8")[:4000]
        r = requests.post(
            f"{NTFY_SERVER}/{norm}",
            data=payload,
            headers={
                "Title":    subject.encode("utf-8"),
                "Priority": "default",
                "Tags":     "bell",
            },
            timeout=10,
        )
        if 200 <= r.status_code < 300:
            return True, "sent-via-ntfy"
        return False, f"ntfy-{r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, f"ntfy-exception: {e}"


def _send_admin_sms(to_phone, body):
    """Send SMS via Twilio. Trimmer til 1 SMS-segment (160 tegn) for å holde
    kostnaden lav. Returnerer (ok, detail)."""
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_FROM):
        return False, "twilio-not-configured"
    msg = body if len(body) <= 160 else body[:157] + "…"
    try:
        r = requests.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json",
            data={"From": TWILIO_FROM, "To": to_phone, "Body": msg},
            auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
            timeout=15,
        )
        if r.status_code in (200, 201):
            return True, "sent-via-twilio"
        return False, f"twilio-{r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, f"twilio-exception: {e}"


def _short_sms_for(event, subject, body):
    """Bygg en kort SMS-tekst (≤160 tegn) basert på subject."""
    # Subject er allerede formatert som "[Havøyet] ...". Bruk første linje av body.
    first_line = ""
    for line in (body or "").splitlines():
        s = line.strip()
        if s and not s.startswith("=") and not s.startswith("-"):
            first_line = s
            break
    text = subject.replace("[Havøyet] ", "Havøyet: ")
    if first_line and first_line.lower() not in text.lower():
        text = f"{text} — {first_line}"
    return text


def _notify_admins(event, subject, body):
    """Send varsel til alle admin-mottakere som har valgt `event`. Sender e-post
    hvis mottakeren har e-post, og SMS hvis mottakeren har telefon (og Twilio
    er konfigurert). Begge kanaler brukes hvis begge feltene er fylt ut."""
    if event not in ADMIN_EVENTS:
        return

    matching = [n for n in _admin_notifiers if event in (n.get("events") or [])]

    # Logg alltid til disk (backup + audit)
    try:
        with open(ADMIN_NOTIFY_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "at": datetime.now().isoformat(),
                "event": event, "subject": subject, "body": body,
                "recipients": [{"email": n.get("email"), "phone": n.get("phone"),
                                "ntfy": n.get("ntfy_topic")} for n in matching],
            }, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[ADMIN-NOTIFY] Logg-feil: {e}")

    sms_text = _short_sms_for(event, subject, body)
    mail_sent = sms_sent = push_sent = 0
    mail_failed = sms_failed = push_failed = 0
    for n in matching:
        email = (n.get("email") or "").strip()
        phone = (n.get("phone") or "").strip()
        ntfy  = (n.get("ntfy_topic") or "").strip()
        if email:
            ok, detail = _send_admin_mail(email, subject, body)
            if ok: mail_sent += 1
            else:
                mail_failed += 1
                print(f"[ADMIN-NOTIFY] mail {email}: {detail}")
        if phone:
            normalized = _normalize_phone(phone)
            if not normalized:
                sms_failed += 1
                print(f"[ADMIN-NOTIFY] sms: ugyldig telefonnummer '{phone}'")
            else:
                ok, detail = _send_admin_sms(normalized, sms_text)
                if ok: sms_sent += 1
                else:
                    sms_failed += 1
                    print(f"[ADMIN-NOTIFY] sms {normalized}: {detail}")
        if ntfy:
            ok, detail = _send_admin_push(ntfy, subject, body)
            if ok: push_sent += 1
            else:
                push_failed += 1
                print(f"[ADMIN-NOTIFY] push {ntfy}: {detail}")
    print(f"[ADMIN-NOTIFY] {event}: "
          f"mail={mail_sent}/{mail_sent+mail_failed}, "
          f"sms={sms_sent}/{sms_sent+sms_failed}, "
          f"push={push_sent}/{push_sent+push_failed}")


def _format_order_lines(order):
    """Tekstoppsummering av en ordre — håndterer både manuelle og Shopify-ordre."""
    nr = order.get("ordrenr") or order.get("name") or order.get("id") or "?"
    # Kunde kan være dict (manuelle) eller streng (Shopify-mappede)
    raw_kunde = order.get("kunde")
    if isinstance(raw_kunde, dict):
        navn = raw_kunde.get("navn") or raw_kunde.get("name") or "Ukjent"
        tlf  = raw_kunde.get("tlf") or raw_kunde.get("phone") or ""
        adr  = raw_kunde.get("adresse") or raw_kunde.get("address") or ""
        dag  = raw_kunde.get("leveringsdag") or order.get("delivery") or ""
        tid  = raw_kunde.get("leveringstid") or order.get("slot") or ""
        merk = raw_kunde.get("kommentar") or order.get("note") or ""
    else:
        navn = order.get("customer") or raw_kunde or "Ukjent"
        tlf  = order.get("phone") or ""
        adr  = ""  # Shopify shipping_address blir ikke med via map_order
        dag  = order.get("delivery") or ""
        tid  = order.get("slot") or ""
        merk = order.get("note") or ""
    total  = order.get("sum") if order.get("sum") is not None else order.get("total", "")
    status = order.get("status", "")
    varer  = order.get("varer") or order.get("items") or []
    lines = []
    for v in varer:
        name = v.get("name") or "?"
        qty  = v.get("qty") or v.get("quantity") or 1
        price = v.get("price")
        if price is not None:
            lines.append(f"  · {name} ×{qty}  ({price} kr)")
        else:
            lines.append(f"  · {name} ×{qty}")
    varer_tekst = "\n".join(lines) or "  (ingen varer)"
    sum_tekst = f"{total} kr" if total != "" else "—"
    return (
        f"Ordrenr.: #{nr}\n"
        f"Kunde:    {navn}\n"
        f"Telefon:  {tlf}\n"
        f"Adresse:  {adr}\n"
        f"Levering: {dag} {tid}\n"
        f"Status:   {status}\n"
        f"Sum:      {sum_tekst}\n"
        f"\nVarer:\n{varer_tekst}\n"
        + (f"\nMerknad:\n{merk}\n" if merk else "")
    )


def _find_order(order_id):
    """Finn en manuell ordre på id eller ordrenr."""
    for o in _manual_orders:
        if str(o.get("ordrenr") or o.get("id")) == str(order_id):
            return o
    return None


# ── KUNDER (manuelt registrerte) ───────────────────────────────────────────────
@app.route("/api/customers", methods=["GET", "POST"])
def api_customers():
    """GET: liste manuelt registrerte kunder. POST: opprett ny."""
    global _customers
    if request.method == "POST":
        data = request.get_json(force=True) or {}
        navn = (data.get("navn") or "").strip()
        if not navn:
            return jsonify({"error": "Navn er påkrevet"}), 400
        tlf  = (data.get("tlf") or "").strip()
        ep   = (data.get("epost") or "").strip().lower()
        adr  = (data.get("adresse") or "").strip()
        komm = (data.get("kommentar") or "").strip()
        # Unngå duplikat på navn+telefon
        for c in _customers:
            if c.get("navn", "").lower() == navn.lower() and c.get("tlf", "") == tlf:
                return jsonify({"error": "Kunde med samme navn og telefon finnes allerede"}), 409
        def _to_num(v, default=0):
            try: return float(v)
            except (TypeError, ValueError): return default
        new = {
            "id": str(_uuid.uuid4()),
            "navn": navn,
            "tlf": tlf,
            "epost": ep,
            "adresse": adr,
            "kommentar": komm,
            "total_spent":  _to_num(data.get("total_spent")),
            "total_orders": int(_to_num(data.get("total_orders"))),
            "shopify_id":   (data.get("shopify_id") or "").strip(),
            "created_at": datetime.now().isoformat(),
        }
        _customers.append(new)
        _save_sync_state()
        return jsonify({"ok": True, "customer": new})
    return jsonify(_customers)


@app.route("/api/customers/<customer_id>", methods=["PATCH", "DELETE"])
def api_customer_one(customer_id):
    """PATCH: oppdater kunde. DELETE: fjern."""
    global _customers
    if request.method == "DELETE":
        before = len(_customers)
        _customers = [c for c in _customers if c.get("id") != customer_id]
        _save_sync_state()
        return jsonify({"ok": True, "removed": before - len(_customers)})
    data = request.get_json(force=True) or {}
    def _to_num(v, default=0):
        try: return float(v)
        except (TypeError, ValueError): return default
    for c in _customers:
        if c.get("id") == customer_id:
            for key in ("navn", "tlf", "epost", "adresse", "kommentar", "shopify_id"):
                if key in data:
                    c[key] = (data.get(key) or "").strip()
            if "total_spent" in data:
                c["total_spent"] = _to_num(data.get("total_spent"))
            if "total_orders" in data:
                c["total_orders"] = int(_to_num(data.get("total_orders")))
            if not c.get("navn"):
                return jsonify({"error": "Navn kan ikke være tomt"}), 400
            _save_sync_state()
            return jsonify({"ok": True, "customer": c})
    return jsonify({"error": "Ikke funnet"}), 404


@app.route("/api/admin/notifiers", methods=["GET", "POST"])
def api_admin_notifiers():
    """GET: liste alle admin-mottakere. POST: opprett ny."""
    global _admin_notifiers
    if request.method == "POST":
        data = request.get_json(force=True) or {}
        email = (data.get("email") or "").strip().lower()
        phone_raw = (data.get("phone") or "").strip()
        ntfy_raw  = (data.get("ntfy_topic") or "").strip()
        name  = (data.get("name") or "").strip()
        events = data.get("events") or list(ADMIN_EVENTS)
        # Minst én kanal må være fylt ut
        if not email and not phone_raw and not ntfy_raw:
            return jsonify({"error": "Du må fylle inn e-post, telefon eller ntfy-topic"}), 400
        if email and "@" not in email:
            return jsonify({"error": "Ugyldig e-postadresse"}), 400
        phone = ""
        if phone_raw:
            normalized = _normalize_phone(phone_raw)
            if not normalized:
                return jsonify({"error": "Ugyldig telefonnummer (bruk +47XXXXXXXX eller 8 sifre)"}), 400
            phone = normalized
        ntfy = ""
        if ntfy_raw:
            n_norm = _normalize_ntfy_topic(ntfy_raw)
            if not n_norm:
                return jsonify({"error": "Ugyldig ntfy-topic (bruk bokstaver, tall, _ og -)"}), 400
            ntfy = n_norm
        # Filtrer bare gyldige events
        events = [e for e in events if e in ADMIN_EVENTS]
        if not events:
            events = list(ADMIN_EVENTS)
        # Unngå duplikat
        for n in _admin_notifiers:
            if email and n.get("email", "").lower() == email:
                return jsonify({"error": "E-postadressen er allerede registrert"}), 409
            if phone and n.get("phone", "") == phone:
                return jsonify({"error": "Telefonnummeret er allerede registrert"}), 409
            if ntfy and n.get("ntfy_topic", "") == ntfy:
                return jsonify({"error": "Ntfy-topicen er allerede registrert"}), 409
        new = {
            "id": str(_uuid.uuid4()),
            "name": name,
            "email": email,
            "phone": phone,
            "ntfy_topic": ntfy,
            "events": events,
            "created_at": datetime.now().isoformat(),
        }
        _admin_notifiers.append(new)
        _save_sync_state()
        return jsonify({"ok": True, "notifier": new})
    return jsonify(_admin_notifiers)


@app.route("/api/admin/notifiers/<notifier_id>", methods=["PATCH", "DELETE"])
def api_admin_notifier_one(notifier_id):
    """PATCH: oppdater navn/events. DELETE: fjern."""
    global _admin_notifiers
    if request.method == "DELETE":
        before = len(_admin_notifiers)
        _admin_notifiers = [n for n in _admin_notifiers if n.get("id") != notifier_id]
        _save_sync_state()
        return jsonify({"ok": True, "removed": before - len(_admin_notifiers)})
    data = request.get_json(force=True) or {}
    for n in _admin_notifiers:
        if n.get("id") == notifier_id:
            if "name" in data:
                n["name"] = (data.get("name") or "").strip()
            if "events" in data:
                ev = [e for e in (data.get("events") or []) if e in ADMIN_EVENTS]
                n["events"] = ev
            if "email" in data:
                em = (data.get("email") or "").strip().lower()
                if em == "":
                    n["email"] = ""
                elif "@" in em:
                    n["email"] = em
                else:
                    return jsonify({"error": "Ugyldig e-post"}), 400
            if "phone" in data:
                ph_raw = (data.get("phone") or "").strip()
                if ph_raw == "":
                    n["phone"] = ""
                else:
                    norm = _normalize_phone(ph_raw)
                    if not norm:
                        return jsonify({"error": "Ugyldig telefonnummer"}), 400
                    n["phone"] = norm
            if "ntfy_topic" in data:
                nt_raw = (data.get("ntfy_topic") or "").strip()
                if nt_raw == "":
                    n["ntfy_topic"] = ""
                else:
                    nt_norm = _normalize_ntfy_topic(nt_raw)
                    if not nt_norm:
                        return jsonify({"error": "Ugyldig ntfy-topic"}), 400
                    n["ntfy_topic"] = nt_norm
            # Sikkerhetssjekk: minst én kanal må gjenstå
            if not (n.get("email") or n.get("phone") or n.get("ntfy_topic")):
                return jsonify({"error": "Mottakeren må ha minst én kanal (e-post, telefon eller ntfy)"}), 400
            _save_sync_state()
            return jsonify({"ok": True, "notifier": n})
    return jsonify({"error": "Ikke funnet"}), 404


@app.route("/api/admin/notifiers/test", methods=["POST"])
def api_admin_notifier_test():
    """Send testvarsel (e-post + SMS) til alle (eller én spesifikk) mottaker."""
    data = request.get_json(force=True) or {}
    target_id = data.get("id")
    targets = _admin_notifiers
    if target_id:
        targets = [n for n in _admin_notifiers if n.get("id") == target_id]
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    mail_sent = mail_failed = sms_sent = sms_failed = push_sent = push_failed = 0
    for n in targets:
        email = (n.get("email") or "").strip()
        phone = (n.get("phone") or "").strip()
        ntfy  = (n.get("ntfy_topic") or "").strip()
        if email:
            ok, _ = _send_admin_mail(
                email,
                "[Havøyet] Testvarsel fra admin",
                f"Dette er en test sendt {ts}.\n\n"
                f"Hvis du mottok denne e-posten er admin-varsler korrekt satt opp for {email}.",
            )
            if ok: mail_sent += 1
            else:  mail_failed += 1
        if phone:
            norm = _normalize_phone(phone)
            if not norm:
                sms_failed += 1
            else:
                ok, _ = _send_admin_sms(norm, f"Havøyet: testvarsel {ts}")
                if ok: sms_sent += 1
                else:  sms_failed += 1
        if ntfy:
            ok, _ = _send_admin_push(
                ntfy,
                "[Havøyet] Testvarsel",
                f"Push-varsel sendt {ts}.\n\nNår du ser dette på telefonen, fungerer admin-varsler.",
            )
            if ok: push_sent += 1
            else:  push_failed += 1
    return jsonify({
        "ok": True,
        "mail_sent": mail_sent, "mail_failed": mail_failed,
        "sms_sent":  sms_sent,  "sms_failed":  sms_failed,
        "push_sent": push_sent, "push_failed": push_failed,
        # Bakoverkompatibilitet
        "sent": mail_sent + sms_sent + push_sent,
        "failed": mail_failed + sms_failed + push_failed,
    })


@app.route("/api/admin/notifiers/status", methods=["GET"])
def api_admin_notifier_status():
    """Hva er konfigurert? Brukes av admin-UI til å vise hvilke kanaler som
    faktisk vil sende noe akkurat nå."""
    return jsonify({
        "email": {
            "resend":    bool(RESEND_API_KEY),
            "smtp":      bool(SMTP_USER and SMTP_PASS),
            "available": bool(RESEND_API_KEY or (SMTP_USER and SMTP_PASS)),
        },
        "sms": {
            "twilio":    bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_FROM),
            "available": bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_FROM),
        },
        "push": {
            "ntfy":      True,  # Krever ingen credentials
            "server":    NTFY_SERVER,
            "available": True,
        },
    })


@app.route("/api/contact", methods=["POST"])
def api_contact():
    """Kontakt-skjema → e-post til erik@havoyet.no."""
    data = request.get_json(force=True) or {}
    navn    = (data.get("navn") or "").strip()
    epost   = (data.get("epost") or "").strip()
    melding = (data.get("melding") or "").strip()
    emne    = (data.get("emne") or f"[Kontakt] Ny henvendelse fra {navn or 'Havøyet-nettside'}").strip()

    if not navn or not epost or not melding:
        return jsonify({"ok": False, "error": "Navn, e-post og melding er påkrevet"}), 400

    body = (
        f"Ny melding fra Havøyet-nettsiden\n"
        f"{'='*54}\n\n"
        f"Navn:     {navn}\n"
        f"E-post:   {epost}\n"
        f"Mottatt:  {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        f"Melding:\n"
        f"{'-'*54}\n"
        f"{melding}\n"
        f"{'-'*54}\n\n"
        f"Svar på denne e-posten for å svare {navn} direkte —\n"
        f"Reply-To peker til {epost}.\n"
    )
    ok, detail = _send_contact_mail(epost, navn, emne, body)
    # Send også varsel til registrerte admin-mottakere
    _notify_admins(
        "new_message",
        f"[Havøyet] Ny melding fra {navn}",
        body,
    )
    return jsonify({"ok": ok, "detail": detail})


@app.route("/api/orders/new", methods=["POST"])
def api_orders_new():
    """Ny kundebestilling fra checkout → lagres + e-post til Erik."""
    global _manual_orders
    data = request.get_json(force=True) or {}
    kunde = data.get("kunde") or {}
    varer = data.get("varer") or []
    navn  = (kunde.get("navn") or "").strip()
    epost = (kunde.get("epost") or "").strip()

    if not navn or not epost or not varer:
        return jsonify({"ok": False, "error": "Mangler kundenavn, e-post eller varer"}), 400

    # Sørg for at ordrenummer finnes
    if not data.get("ordrenr"):
        data["ordrenr"] = "H" + _uuid.uuid4().hex[:6].upper()
    if not data.get("dato"):
        data["dato"] = datetime.now().strftime("%Y-%m-%d")
    if not data.get("status"):
        data["status"] = "NEW"

    # Legg til i state
    _manual_orders.append(data)
    _save_sync_state()

    # Bygg ordreoppsummering
    lines = []
    lines.append(f"Ny bestilling via havoyet-nettsiden")
    lines.append("=" * 54)
    lines.append("")
    lines.append(f"Ordrenummer:   {data['ordrenr']}")
    lines.append(f"Dato:          {data['dato']}")
    lines.append(f"Kunde:         {navn}")
    lines.append(f"E-post:        {epost}")
    if kunde.get("tlf"):         lines.append(f"Telefon:       {kunde['tlf']}")
    lines.append("")
    lines.append("Leveringsadresse:")
    if kunde.get("adresse"):     lines.append(f"  {kunde['adresse']}")
    if kunde.get("postnr") or kunde.get("sted"):
        lines.append(f"  {kunde.get('postnr','')} {kunde.get('sted','')}".strip())
    lines.append("")
    lines.append(f"Leveringsdag:  {kunde.get('leveringsdag','')} kl. {kunde.get('leveringstid','')}")
    lines.append(f"Betaling:      {kunde.get('betaling','')}")
    if kunde.get("kommentar"):
        lines.append("")
        lines.append("Kommentar fra kunden:")
        lines.append(f"  {kunde['kommentar']}")
    lines.append("")
    lines.append("-" * 54)
    lines.append("VARER")
    lines.append("-" * 54)
    for v in varer:
        navn_v = v.get("name") or v.get("navn") or "?"
        qty    = v.get("qty", 1)
        pris   = v.get("price", 0)
        lines.append(f"  {qty} × {navn_v:<32} {qty * pris:>6} kr")
        if v.get("boxSelection"):
            for s in v["boxSelection"]:
                lines.append(f"      + {s.get('navn','')}")
    lines.append("-" * 54)
    lines.append(f"{'Subtotal':<44} {data.get('total', 0):>6} kr")
    lines.append(f"{'Levering':<44} {data.get('fee', 0):>6} kr")
    lines.append(f"{'TOTAL':<44} {data.get('sum', 0):>6} kr")
    lines.append("=" * 54)
    lines.append("")
    lines.append(f"Svar på denne e-posten for å svare {navn} direkte.")
    lines.append("Ordren er også synlig i admin-panelet.")

    emne = f"[Bestilling {data['ordrenr']}] {navn} – {data.get('sum', 0)} kr"
    ok, detail = _send_contact_mail(epost, navn, emne, "\n".join(lines))
    return jsonify({"ok": True, "mail": detail, "ordrenr": data["ordrenr"], "order": data})


# ── KUNDE-KONTO: ordrehistorikk + favoritter (identifiseres via e-post) ──────
def _orders_for_email(email):
    """Samler alle ordre som matcher en e-postadresse."""
    email = (email or "").strip().lower()
    if not email:
        return []
    orders = []
    # Manuelle ordre lagret via checkout-skjema
    for o in _manual_orders:
        kunde_epost = ((o.get("kunde") or {}).get("epost") or "").lower()
        if kunde_epost == email:
            orders.append(o)
    # Shopify-ordre (fra _cache["orders"])
    for o in _cache.get("orders", []):
        if (o.get("email") or "").lower() == email:
            orders.append(o)
    # Sorter nyeste først
    def _key(o):
        return o.get("dato") or o.get("created_at") or ""
    orders.sort(key=_key, reverse=True)
    return orders


@app.route("/api/customer/account")
def api_customer_account():
    """?email=... → returnerer ordrehistorikk + favoritter for kunden."""
    email = (request.args.get("email") or "").strip().lower()
    if not email:
        return jsonify({"error": "E-post mangler"}), 400
    return jsonify({
        "email": email,
        "orders": _orders_for_email(email),
        "favorites": _customer_favorites.get(email, []),
    })


@app.route("/api/customer/favorites", methods=["POST"])
def api_customer_favorites():
    """POST {email, slug, action: toggle|add|remove} → oppdater favoritter."""
    global _customer_favorites
    data = request.get_json(force=True) or {}
    email  = (data.get("email") or "").strip().lower()
    slug   = (data.get("slug") or "").strip()
    action = (data.get("action") or "toggle").strip()
    if not email or not slug:
        return jsonify({"error": "E-post og slug er påkrevet"}), 400
    current = set(_customer_favorites.get(email, []))
    if action == "add":
        current.add(slug)
    elif action == "remove":
        current.discard(slug)
    else:  # toggle
        if slug in current: current.discard(slug)
        else: current.add(slug)
    _customer_favorites[email] = sorted(current)
    _save_sync_state()
    return jsonify({"ok": True, "favorites": _customer_favorites[email]})


@app.route("/api/manual-orders/<order_id>/status", methods=["POST"])
def api_order_update_status(order_id):
    """Admin oppdaterer status på en manuell ordre → synker til kundesiden."""
    global _manual_orders
    data = request.get_json(force=True) or {}
    new_status = (data.get("status") or "").strip()
    if not new_status:
        return jsonify({"error": "Status mangler"}), 400
    for o in _manual_orders:
        if str(o.get("ordrenr") or o.get("id")) == str(order_id):
            old_status = o.get("status", "")
            o["status"] = new_status
            _save_sync_state()
            nr = o.get("ordrenr") or o.get("id") or "?"
            if str(new_status).upper() in ("DONE", "LEVERT") or "lever" in str(new_status).lower():
                _notify_admins(
                    "order_delivered",
                    f"[Havøyet] Bestilling #{nr} er levert",
                    f"Status endret fra '{old_status}' til '{new_status}'.\n"
                    + "=" * 54 + "\n\n"
                    + _format_order_lines(o),
                )
            elif old_status != new_status:
                _notify_admins(
                    "order_updated",
                    f"[Havøyet] Bestilling #{nr} oppdatert",
                    f"Status endret fra '{old_status}' til '{new_status}'.\n"
                    + "=" * 54 + "\n\n"
                    + _format_order_lines(o),
                )
            return jsonify({"ok": True, "order": o})
    return jsonify({"error": "Ikke funnet"}), 404


@app.route("/api/manual-orders/<order_id>", methods=["PATCH"])
def api_order_patch(order_id):
    """Admin redigerer en manuell ordre i sin helhet (kunde, varer, levering,
    status, betaling, totalsummer). Kundesiden (/api/customer/account) plukker
    opp endringene automatisk siden den leser fra samme _manual_orders."""
    global _manual_orders
    data = request.get_json(force=True) or {}
    if not isinstance(data, dict):
        return jsonify({"error": "Forventer JSON-objekt"}), 400
    for o in _manual_orders:
        if str(o.get("ordrenr") or o.get("id")) == str(order_id):
            old_status = o.get("status", "")
            # Dyp merge for kunde-objektet, full erstatning for varer-listen
            if "kunde" in data:
                if isinstance(data["kunde"], dict):
                    o.setdefault("kunde", {})
                    o["kunde"].update(data["kunde"])
                else:
                    return jsonify({"error": "kunde må være objekt"}), 400
            if "varer" in data:
                if not isinstance(data["varer"], list):
                    return jsonify({"error": "varer må være liste"}), 400
                o["varer"] = data["varer"]
            # Flate felter (status, dato, total, sum, fee, betaling osv.)
            for k, v in data.items():
                if k in ("kunde", "varer"):
                    continue
                o[k] = v
            _save_sync_state()
            nr = o.get("ordrenr") or o.get("id") or "?"
            new_status = o.get("status", "")
            status_changed = old_status != new_status
            became_delivered = (
                status_changed and (
                    str(new_status).upper() in ("DONE", "LEVERT")
                    or "lever" in str(new_status).lower()
                )
            )
            if became_delivered:
                _notify_admins(
                    "order_delivered",
                    f"[Havøyet] Bestilling #{nr} er levert",
                    f"Status endret fra '{old_status}' til '{new_status}'.\n"
                    + "=" * 54 + "\n\n"
                    + _format_order_lines(o),
                )
            else:
                # Alle andre redigeringer regnes som "oppdatert"
                _notify_admins(
                    "order_updated",
                    f"[Havøyet] Bestilling #{nr} oppdatert",
                    (f"Status endret fra '{old_status}' til '{new_status}'.\n"
                     if status_changed else "Ordre ble redigert i admin.\n")
                    + "=" * 54 + "\n\n"
                    + _format_order_lines(o),
                )
            return jsonify({"ok": True, "order": o})
    return jsonify({"error": "Ikke funnet"}), 404


# ── OPPSTART ───────────────────────────────────────────────────────────────────
# ─── Vipps ePayment integrasjon ─────────────────────────────────────
def _vipps_configured():
    return bool(VIPPS_CLIENT_ID and VIPPS_CLIENT_SECRET and VIPPS_SUBSCRIPTION_KEY and VIPPS_MSN)

def _vipps_load_payments():
    if os.path.exists(VIPPS_PAYMENTS_FILE):
        try:
            with open(VIPPS_PAYMENTS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _vipps_save_payments(data):
    try:
        with open(VIPPS_PAYMENTS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"vipps: kunne ikke lagre betalinger: {e}")

def _vipps_token():
    """Hent (og cache) access token. Vipps-tokens varer ~1 time, vi bytter etter 50 min."""
    now = time.time()
    if _vipps_token_cache["access_token"] and _vipps_token_cache["expires_at"] > now + 60:
        return _vipps_token_cache["access_token"]
    url = f"{VIPPS_API_BASE}/accesstoken/get"
    headers = {
        "client_id": VIPPS_CLIENT_ID,
        "client_secret": VIPPS_CLIENT_SECRET,
        "Ocp-Apim-Subscription-Key": VIPPS_SUBSCRIPTION_KEY,
        "Merchant-Serial-Number": VIPPS_MSN,
    }
    r = requests.post(url, headers=headers, timeout=10)
    r.raise_for_status()
    body = r.json()
    token = body.get("access_token")
    expires = int(body.get("expires_in", 3000))
    _vipps_token_cache["access_token"] = token
    _vipps_token_cache["expires_at"]   = now + expires
    return token

def _vipps_headers(idempotency_key=None):
    h = {
        "Authorization": f"Bearer {_vipps_token()}",
        "Ocp-Apim-Subscription-Key": VIPPS_SUBSCRIPTION_KEY,
        "Merchant-Serial-Number": VIPPS_MSN,
        "Vipps-System-Name": "havoyet-flask",
        "Vipps-System-Version": "1.0.0",
        "Content-Type": "application/json",
    }
    if idempotency_key:
        h["Idempotency-Key"] = idempotency_key
    return h

@app.route("/api/vipps/init", methods=["POST"])
def api_vipps_init():
    """Oppretter en Vipps-betaling og returnerer redirect-URL."""
    if not _vipps_configured():
        return jsonify({"error": "Vipps er ikke konfigurert på serveren"}), 503
    data = request.get_json(silent=True) or {}
    ordrenr = data.get("ordrenr") or ("H" + str(int(time.time()*1000))[-8:])
    amount  = int(data.get("amount", 0))            # i ØRE (1 kr = 100 øre)
    if amount <= 0:
        return jsonify({"error": "Ugyldig beløp"}), 400
    return_url = data.get("returnUrl") or f"{request.host_url.rstrip('/')}/kasse?vipps={ordrenr}"
    phone     = (data.get("phoneNumber") or "").replace(" ", "").lstrip("+")
    # Vipps krever 47XXXXXXXX (landkode + 8 sifre)
    if phone and not phone.startswith("47") and len(phone) == 8:
        phone = "47" + phone
    reference = f"havoyet-{ordrenr}-{int(time.time())}"

    payload = {
        "amount": {"currency": "NOK", "value": amount},
        "paymentMethod": {"type": "WALLET"},
        "reference": reference,
        "returnUrl": return_url,
        "userFlow": "WEB_REDIRECT",
        "paymentDescription": f"Havøyet ordre {ordrenr}",
    }
    if phone and len(phone) == 10:
        payload["customer"] = {"phoneNumber": phone}

    url = f"{VIPPS_API_BASE}/epayment/v1/payments"
    try:
        r = requests.post(url, headers=_vipps_headers(idempotency_key=reference),
                          json=payload, timeout=15)
        body = r.json() if r.content else {}
    except Exception as e:
        return jsonify({"error": f"Kunne ikke kontakte Vipps: {e}"}), 502
    if r.status_code >= 400:
        return jsonify({"error": "Vipps avviste betalingen", "details": body}), r.status_code

    # Lagre referansen lokalt for status-oppslag
    payments = _vipps_load_payments()
    payments[reference] = {
        "ordrenr": ordrenr, "amount": amount, "state": "CREATED",
        "created_at": time.time(),
    }
    _vipps_save_payments(payments)

    return jsonify({
        "reference": reference,
        "redirectUrl": body.get("redirectUrl"),
        "ordrenr": ordrenr,
    })

@app.route("/api/vipps/status/<reference>")
def api_vipps_status(reference):
    """Hent status for en Vipps-betaling fra Vipps API."""
    if not _vipps_configured():
        return jsonify({"error": "Vipps er ikke konfigurert"}), 503
    url = f"{VIPPS_API_BASE}/epayment/v1/payments/{reference}"
    try:
        r = requests.get(url, headers=_vipps_headers(), timeout=10)
        body = r.json() if r.content else {}
    except Exception as e:
        return jsonify({"error": f"Kunne ikke kontakte Vipps: {e}"}), 502
    if r.status_code >= 400:
        return jsonify({"error": "Vipps-feil", "details": body}), r.status_code

    state = body.get("state", "UNKNOWN")
    payments = _vipps_load_payments()
    if reference in payments:
        payments[reference]["state"] = state
        payments[reference]["last_check"] = time.time()
        _vipps_save_payments(payments)
    return jsonify({"reference": reference, "state": state, "vipps": body})

@app.route("/api/vipps/callback", methods=["POST"])
def api_vipps_callback():
    """Webhook fra Vipps når betalingsstatus endrer seg.
    Vipps sender hele betalingsobjektet som JSON. Vi lagrer status og bekrefter mottatt."""
    body = request.get_json(silent=True) or {}
    reference = body.get("reference") or (body.get("data") or {}).get("reference")
    state     = body.get("name") or body.get("state") or "UNKNOWN"
    if reference:
        payments = _vipps_load_payments()
        if reference not in payments:
            payments[reference] = {"created_at": time.time()}
        payments[reference]["state"] = state
        payments[reference]["callback_at"] = time.time()
        payments[reference]["last_callback"] = body
        _vipps_save_payments(payments)
        print(f"vipps callback: {reference} → {state}")
    return jsonify({"ok": True})

# ─── Vipps Checkout (Vipps + Kort på samme side) ──────────────────────
def _force_https(url: str) -> str:
    """Vipps Checkout krever HTTPS. Bytter http:// til https:// hvis aktuelt."""
    if url and url.startswith("http://") and "localhost" not in url and "127.0.0.1" not in url:
        return "https://" + url[len("http://"):]
    return url

@app.route("/api/checkout/init", methods=["POST"])
def api_checkout_init():
    """Oppretter en Vipps Checkout-sesjon hvor kunden kan velge Vipps eller kort."""
    if not _vipps_configured():
        return jsonify({"error": "Vipps Checkout er ikke konfigurert"}), 503
    data = request.get_json(silent=True) or {}
    ordrenr   = data.get("ordrenr") or ("H" + str(int(time.time()*1000))[-8:])
    amount    = int(data.get("amount", 0))      # i øre
    if amount <= 0:
        return jsonify({"error": "Ugyldig beløp"}), 400
    methods   = data.get("methods") or ["WALLET", "CARD"]
    return_url = _force_https(data.get("returnUrl") or f"{request.host_url.rstrip('/')}/kasse?ordre={ordrenr}")
    callback_url = _force_https(f"{request.host_url.rstrip('/')}/api/vipps/callback")
    # Lokal HTTP-test: Vipps Checkout krever HTTPS for både callback og return.
    # Returner tydelig feilmelding så frontend kan falle tilbake til ePayment.
    if return_url.startswith("http://") or callback_url.startswith("http://"):
        return jsonify({
            "error": "Vipps Checkout krever HTTPS — kun Vipps ePayment fungerer på localhost. Deploy til Render for kort-betaling.",
            "code": "HTTPS_REQUIRED",
        }), 400
    customer  = data.get("customer") or {}
    reference = f"havoyet-{ordrenr}-{int(time.time())}"

    payload = {
        "merchantInfo": {
            "callbackUrl":   callback_url,
            "returnUrl":     return_url,
            "callbackAuthorizationToken": "havoyet-callback-token",
        },
        "transaction": {
            "amount": {"currency": "NOK", "value": amount},
            "reference": reference,
            "paymentDescription": f"Havoyet ordre {ordrenr}",
        },
        "configuration": {
            "userFlow": "WEB_REDIRECT",
            "elements": "Full",
            "showOrderSummary": True,
            "acceptedPaymentMethods": [{"type": m} for m in methods],
        },
    }
    if customer.get("email") or customer.get("phoneNumber"):
        payload["prefillCustomer"] = {
            **({"email": customer["email"]} if customer.get("email") else {}),
            **({"phoneNumber": customer["phoneNumber"]} if customer.get("phoneNumber") else {}),
            **({"firstName": customer["firstName"]} if customer.get("firstName") else {}),
            **({"lastName": customer["lastName"]} if customer.get("lastName") else {}),
        }

    url = f"{VIPPS_API_BASE}/checkout/v3/session"
    headers = {
        "client_id": VIPPS_CLIENT_ID,
        "client_secret": VIPPS_CLIENT_SECRET,
        "Ocp-Apim-Subscription-Key": VIPPS_SUBSCRIPTION_KEY,
        "Merchant-Serial-Number": VIPPS_MSN,
        "Vipps-System-Name": "havoyet-flask",
        "Vipps-System-Version": "1.0.0",
        "Vipps-System-Plugin-Name": "havoyet-checkout",
        "Vipps-System-Plugin-Version": "1.0.0",
        "Content-Type": "application/json",
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=15)
        body = r.json() if r.content else {}
    except Exception as e:
        return jsonify({"error": f"Kunne ikke kontakte Vipps Checkout: {e}"}), 502
    if r.status_code >= 400:
        return jsonify({"error": "Vipps avviste sesjonen", "details": body}), r.status_code

    payments = _vipps_load_payments()
    payments[reference] = {
        "ordrenr": ordrenr, "amount": amount, "state": "CREATED",
        "kind": "checkout", "created_at": time.time(),
        "polling_token": body.get("token"),
    }
    _vipps_save_payments(payments)

    return jsonify({
        "reference": reference,
        "checkoutFrontendUrl": body.get("checkoutFrontendUrl"),
        "pollingUrl": body.get("pollingUrl"),
        "ordrenr": ordrenr,
    })

@app.route("/api/checkout/status/<reference>")
def api_checkout_status(reference):
    """Polling for Vipps Checkout-sesjon."""
    if not _vipps_configured():
        return jsonify({"error": "Vipps Checkout er ikke konfigurert"}), 503
    url = f"{VIPPS_API_BASE}/checkout/v3/session/{reference}"
    headers = {
        "client_id": VIPPS_CLIENT_ID,
        "client_secret": VIPPS_CLIENT_SECRET,
        "Ocp-Apim-Subscription-Key": VIPPS_SUBSCRIPTION_KEY,
        "Merchant-Serial-Number": VIPPS_MSN,
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        body = r.json() if r.content else {}
    except Exception as e:
        return jsonify({"error": f"Kunne ikke kontakte Vipps: {e}"}), 502
    if r.status_code >= 400:
        return jsonify({"error": "Vipps-feil", "details": body}), r.status_code
    state = body.get("sessionState") or body.get("state") or "UNKNOWN"
    payments = _vipps_load_payments()
    if reference in payments:
        payments[reference]["state"] = state
        payments[reference]["last_check"] = time.time()
        _vipps_save_payments(payments)
    return jsonify({"reference": reference, "state": state, "vipps": body})

# ── AUTH (admin-brukere + sesjoner) ───────────────────────────────────────────
# Brukere lagres på disk via _save_sync_state. Sesjoner er kun i minnet — overlever
# ikke restart, men det er greit (klienten ber bare om login på nytt).

_AUTH_SEED = [
    {"email": "erik@havoyet.no",  "role": "admin"},
    {"email": "stian@havoyet.no", "role": "user"},
]

def _seed_auth_users():
    """Sett opp standard-brukere første gang serveren starter, eller legg til
    seed-brukere som mangler i en eksisterende database."""
    global _auth_users
    existing = {u.get("email", "").lower() for u in _auth_users}
    for s in _AUTH_SEED:
        if s["email"].lower() not in existing:
            _auth_users.append({
                "email": s["email"],
                "role": s["role"],
                "password_hash": None,
                "must_set_password": True,
                "created_at": int(time.time()),
            })

def _find_user(email):
    if not email:
        return None
    em = email.strip().lower()
    return next((u for u in _auth_users if u.get("email", "").lower() == em), None)

def _user_from_request():
    """Returnerer (user_dict, token) hvis det er en gyldig sesjon, ellers (None, None)."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None, None
    token = auth[7:].strip()
    sess = _auth_sessions.get(token)
    if not sess:
        return None, None
    user = _find_user(sess.get("email"))
    if not user:
        _auth_sessions.pop(token, None)
        return None, None
    return user, token

def _public_user(u):
    return {
        "email": u.get("email"),
        "role": u.get("role"),
        "mustSetPassword": bool(u.get("must_set_password")),
        "hasPassword": bool(u.get("password_hash")),
        "createdAt": u.get("created_at"),
    }

@app.route("/api/auth/login", methods=["POST"])
def api_auth_login():
    data = request.get_json(force=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    user = _find_user(email)
    if not user:
        return jsonify({"ok": False, "error": "Ugyldig e-post eller passord"}), 401
    if user.get("must_set_password") or not user.get("password_hash"):
        return jsonify({
            "ok": False,
            "mustSetPassword": True,
            "email": user.get("email"),
            "message": "Førstegangs-pålogging — du må sette et passord først.",
        })
    if not check_password_hash(user.get("password_hash") or "", password):
        return jsonify({"ok": False, "error": "Ugyldig e-post eller passord"}), 401
    token = secrets.token_urlsafe(32)
    _auth_sessions[token] = {
        "email": user["email"],
        "role": user["role"],
        "created_at": int(time.time()),
    }
    return jsonify({"ok": True, "token": token, "user": _public_user(user)})

@app.route("/api/auth/set-password", methods=["POST"])
def api_auth_set_password():
    data = request.get_json(force=True) or {}
    email = (data.get("email") or "").strip().lower()
    new_password = data.get("newPassword") or ""
    if len(new_password) < 8:
        return jsonify({"ok": False, "error": "Passordet må være minst 8 tegn"}), 400
    user = _find_user(email)
    if not user:
        return jsonify({"ok": False, "error": "Bruker finnes ikke"}), 404
    if not user.get("must_set_password"):
        return jsonify({"ok": False, "error": "Førstegangs-passord er allerede satt. Bruk «Endre passord» fra Min bruker."}), 400
    user["password_hash"] = generate_password_hash(new_password)
    user["must_set_password"] = False
    _save_sync_state()
    token = secrets.token_urlsafe(32)
    _auth_sessions[token] = {
        "email": user["email"],
        "role": user["role"],
        "created_at": int(time.time()),
    }
    return jsonify({"ok": True, "token": token, "user": _public_user(user)})

@app.route("/api/auth/me", methods=["GET"])
def api_auth_me():
    user, _ = _user_from_request()
    if not user:
        return jsonify({"ok": False}), 401
    return jsonify({"ok": True, "user": _public_user(user)})

@app.route("/api/auth/logout", methods=["POST"])
def api_auth_logout():
    _, token = _user_from_request()
    if token:
        _auth_sessions.pop(token, None)
    return jsonify({"ok": True})

@app.route("/api/auth/me/password", methods=["POST"])
def api_auth_me_password():
    user, _ = _user_from_request()
    if not user:
        return jsonify({"ok": False, "error": "Ikke innlogget"}), 401
    data = request.get_json(force=True) or {}
    current = data.get("currentPassword") or ""
    new_pwd = data.get("newPassword") or ""
    if len(new_pwd) < 8:
        return jsonify({"ok": False, "error": "Nytt passord må være minst 8 tegn"}), 400
    if not check_password_hash(user.get("password_hash") or "", current):
        return jsonify({"ok": False, "error": "Feil nåværende passord"}), 401
    user["password_hash"] = generate_password_hash(new_pwd)
    user["must_set_password"] = False
    _save_sync_state()
    return jsonify({"ok": True})

@app.route("/api/auth/users", methods=["GET", "POST"])
def api_auth_users():
    actor, _ = _user_from_request()
    if not actor:
        return jsonify({"ok": False, "error": "Ikke innlogget"}), 401
    if actor.get("role") != "admin":
        return jsonify({"ok": False, "error": "Bare admin kan administrere brukere"}), 403
    if request.method == "GET":
        return jsonify({"ok": True, "users": [_public_user(u) for u in _auth_users]})
    data = request.get_json(force=True) or {}
    email = (data.get("email") or "").strip().lower()
    role = data.get("role", "user")
    if role not in ("admin", "user"):
        return jsonify({"ok": False, "error": "Ugyldig rolle"}), 400
    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "Ugyldig e-post"}), 400
    if _find_user(email):
        return jsonify({"ok": False, "error": "E-posten er allerede registrert"}), 409
    new_user = {
        "email": email,
        "role": role,
        "password_hash": None,
        "must_set_password": True,
        "created_at": int(time.time()),
    }
    _auth_users.append(new_user)
    _save_sync_state()
    return jsonify({"ok": True, "user": _public_user(new_user)})

@app.route("/api/auth/users/<email>", methods=["DELETE", "PATCH"])
def api_auth_user_one(email):
    actor, _ = _user_from_request()
    if not actor:
        return jsonify({"ok": False, "error": "Ikke innlogget"}), 401
    if actor.get("role") != "admin":
        return jsonify({"ok": False, "error": "Bare admin kan administrere brukere"}), 403
    target_email = (email or "").strip().lower()
    target = _find_user(target_email)
    if not target:
        return jsonify({"ok": False, "error": "Bruker finnes ikke"}), 404
    if request.method == "DELETE":
        if target["email"].lower() == actor["email"].lower():
            return jsonify({"ok": False, "error": "Du kan ikke slette deg selv"}), 400
        global _auth_users
        _auth_users = [u for u in _auth_users if u.get("email", "").lower() != target_email]
        for tok in list(_auth_sessions.keys()):
            if _auth_sessions[tok].get("email", "").lower() == target_email:
                _auth_sessions.pop(tok, None)
        _save_sync_state()
        return jsonify({"ok": True})
    data = request.get_json(force=True) or {}
    if "role" in data:
        if data["role"] not in ("admin", "user"):
            return jsonify({"ok": False, "error": "Ugyldig rolle"}), 400
        if target["email"].lower() == actor["email"].lower() and data["role"] != "admin":
            return jsonify({"ok": False, "error": "Du kan ikke fjerne din egen admin-rolle"}), 400
        target["role"] = data["role"]
    if data.get("resetPassword"):
        target["password_hash"] = None
        target["must_set_password"] = True
        for tok in list(_auth_sessions.keys()):
            if _auth_sessions[tok].get("email", "").lower() == target["email"].lower():
                _auth_sessions.pop(tok, None)
    _save_sync_state()
    return jsonify({"ok": True, "user": _public_user(target)})


if __name__ == "__main__":
    # Last cache fra disk ved oppstart (om den finnes)
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                _cache = json.load(f)
            print(f"Lastet {len(_cache.get('orders', []))} ordre fra disk-cache")
        except Exception:
            pass

    # Last sync-state (pakkingstilstand, manuelle ordre, etc.)
    _load_sync_state()

    # Last prisliste fra disk
    if os.path.exists(PRISLISTE_FILE):
        try:
            with open(PRISLISTE_FILE, "r", encoding="utf-8") as f:
                _prisliste.update(json.load(f))
            print(f"Lastet prisliste: {len(_prisliste.get('items', []))} varelinjer fra disk")
        except Exception:
            pass

    # Start poller i bakgrunnen
    t = threading.Thread(target=poll_loop, daemon=True)
    t.start()
    print(f"Havøyet backend starter — http://0.0.0.0:5000")
    print(f"Shopify: {SHOPIFY_SHOP} | Poll: hvert {POLL_INTERVAL}s")

    is_cloud = os.environ.get("RENDER") or os.environ.get("RAILWAY_ENVIRONMENT")
    app.run(host="0.0.0.0", port=PORT, debug=not is_cloud, use_reloader=not is_cloud)
