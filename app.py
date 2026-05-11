"""
Havøyet AS — Flask backend
Eksponerer ordre fra ny.havoyet.no/kasse via /api/orders.
Kunde-checkout poster til /api/orders/new → lagres i _manual_orders.

Start: python3 app.py
Krav:  pip install flask flask-cors requests
"""

from flask import Flask, jsonify, request, send_from_directory, Response
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
import requests
import threading
import time
import json
import os
import secrets
from datetime import datetime, timedelta, date, timezone


def _now_iso_utc():
    """ISO-timestamp med UTC-tidssone (Z-suffix). Sendes til klienter slik at
    JS' new Date() konverterer riktig til lokal tid uavhengig av server-tidssone."""
    return datetime.now(timezone.utc).isoformat()

app = Flask(__name__)
CORS(app)  # Tillat kall fra HTML-filer åpnet lokalt
app.config["MAX_CONTENT_LENGTH"] = 6 * 1024 * 1024  # 6 MB body — rom for inline e-post-bilder (opptil ~5 MB)

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

PORT            = int(os.environ.get("PORT", 5001))
# STATE_DIR: hvor vedvarende data (brukere, sesjoner, manuelle ordre, osv.) lagres.
# På Render må dette peke til en mountet persistent disk (f.eks. /var/data),
# ellers blir /tmp blanket ved hver container-restart.
#
# Smart auto-detect: hvis /var/data eksisterer og er skrivbar, foretrekk den
# automatisk — slik at om brukeren legger til en Render-disk på /var/data,
# fungerer alt umiddelbart uten å måtte sette STATE_DIR-env-var.
def _detect_state_dir():
    explicit = os.environ.get("STATE_DIR")
    if explicit:
        return explicit
    # Foretrukne persistent-disk-paths (i prioritet)
    for candidate in ("/var/data", "/data", "/persistent-data"):
        if os.path.isdir(candidate):
            try:
                test_file = os.path.join(candidate, ".havoyet_write_test")
                with open(test_file, "w") as _f:
                    _f.write("ok")
                os.remove(test_file)
                print(f"[STATE] Auto-detected persistent disk at {candidate}")
                return candidate
            except Exception:
                pass
    print("[STATE] ⚠ Bruker /tmp — data går tapt ved container-restart!")
    print("[STATE]    Legg til persistent disk på Render og mount til /var/data for å fikse.")
    return "/tmp"

STATE_DIR       = _detect_state_dir()
try:
    os.makedirs(STATE_DIR, exist_ok=True)
except Exception:
    pass
SYNC_STATE_FILE = os.path.join(STATE_DIR, "havoyet_sync_state.json")

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

# ── STRIPE (kort-betaling, separat fra Vipps) ────────────────────────────────
STRIPE_SECRET_KEY      = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET  = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
# Publishable key er trygt å eksponere til frontend. Render-env-varen kan ha
# blandet casing; aksepter flere varianter.
STRIPE_PUBLISHABLE_KEY = (os.environ.get("STRIPE_PUBLISHABLE_KEY")
                         or os.environ.get("STRIPE_Publishable_KEY")
                         or os.environ.get("STRIPE_PUBLIC_KEY")
                         or "")
STRIPE_PAYMENTS_FILE   = os.path.join("/tmp", "havoyet_stripe_payments.json")
try:
    import stripe as _stripe
    if STRIPE_SECRET_KEY:
        _stripe.api_key = STRIPE_SECRET_KEY
except Exception:
    _stripe = None
_vipps_token_cache      = {"access_token": None, "expires_at": 0.0}

# PowerOffice token-cache
_po_token = {"access_token": None, "expires_at": 0.0}

# Prisliste-cache
_prisliste = {"items": [], "last_sync": None, "error": None, "faktura": None}

# ── ORDRE-NORMALISERING ───────────────────────────────────────────────────────
def _normalize_manual_order(o):
    """Konverter ny.havoyet.no/kasse-ordre (fra _manual_orders) til iPad-shapen
    som index.html / pakke.html forventer (id, customer, delivery, items, ...).

    VIKTIG: vi normaliserer BÅDE manuelle admin-ordre (lagret med felt som
    name/qty/unit/price) OG kunde-checkout-ordre (med slug/selectedOpts/
    variantLabel/cost), slik at pakke.html ser samme shape uansett kilde.
    """
    kunde = o.get("kunde") or {}
    varer = o.get("varer") or o.get("items") or []
    items = []
    for v in varer:
        # Bygg en konsistent `variant`-streng. Foretrekk eksplisitt felt,
        # ellers fall tilbake til kundeside-konvensjonen (variantLabel/
        # selectedOpts). For manuelle ordre der bare `unit` er satt brukes
        # ikke variant — pakke.html håndterer enhet separat.
        variant_str = v.get("variant") or v.get("variantStr") or v.get("variantLabel") or ""
        if not variant_str:
            sel = v.get("selectedOpts")
            if isinstance(sel, dict) and sel:
                # Stilen "ca. 250 g · Uten bein · Med skinn" — matcher kunde-checkout
                variant_str = " · ".join(str(x) for x in sel.values() if x)
        items.append({
            "id":           v.get("id"),
            "slug":         v.get("slug") or "",
            "name":         v.get("name") or v.get("navn") or v.get("title", ""),
            "productName":  v.get("productName") or v.get("name") or v.get("navn") or "",
            "quantity":     v.get("qty") or v.get("quantity", 1),
            "qty":          v.get("qty") or v.get("quantity", 1),
            "weight":       v.get("weight"),
            "expiry":       v.get("expiry"),
            "variant":      variant_str,
            "variantStr":   variant_str,
            "variantLabel": v.get("variantLabel") or "",
            "selectedOpts": v.get("selectedOpts") or None,
            "unit":         v.get("unit") or "",
            "price":        v.get("price"),
            "pris":         v.get("pris"),
            "cost":         v.get("cost"),
            "lineCost":     v.get("lineCost"),
            "sku":          v.get("sku"),
            "grams":        v.get("grams", 0),
            "kind":         v.get("kind") or "",
            "tilbehorValgt": v.get("tilbehorValgt") or v.get("tilbehor_valgt") or [],
            "boxSelection":  v.get("boxSelection") or [],
        })
    return {
        "id":         o.get("ordrenr") or o.get("id"),
        "shopify_id": None,
        "customer":   kunde.get("navn") or kunde.get("name") or o.get("customer") or "Ukjent",
        "email":      kunde.get("epost") or kunde.get("email") or o.get("email") or "",
        "phone":      kunde.get("tlf") or kunde.get("phone") or o.get("phone") or "",
        "delivery":   kunde.get("leveringsdag") or o.get("delivery") or "",
        "slot":       kunde.get("leveringstid") or o.get("slot") or "",
        # True når ordren mangler en konkret leveringsdato — bestillingssiden
        # gruperer slike ordre i en egen "Trenger leveringsdato"-seksjon istedenfor
        # å skjule dem helt (typisk havoyet.no-kasse-flow der kunden ikke valgte slot).
        "needs_delivery_date": not (kunde.get("leveringsdag") or o.get("delivery")),
        "status":     o.get("status") or "NEW",
        "items":      items,
        "note":       kunde.get("kommentar") or o.get("note") or "",
        "financial":  o.get("financial") or "",
        "created_at": o.get("dato") or o.get("created_at") or "",
        # Felter som lar iPad/admin filtrere på opprinnelse og butikk.
        # `source` utledes fra (i prioritet): eksplisitt felt → kilde → manual-flagg.
        # Historiske Shopify-imports har "kilde": "shopify-import" → normaliseres til "shopify".
        "store":      o.get("store") or "",
        "source":     (
            o.get("source")
            or ("shopify" if "shopify" in (o.get("kilde") or "").lower() else None)
            or ("import"  if "import"  in (o.get("kilde") or "").lower() else None)
            or ("admin"   if o.get("manual") else "")
        ),
        "manual":     bool(o.get("manual")),
        # Betalings-info forplantes så iPad-filteret kan speile admin sin
        # "skjul ferdige+betalte"-regel uten å gjette på status alene.
        # Default = "paid" for legacy/Stripe/Shopify (de var betalt før de
        # ble lagret). Eksplisitt "unpaid"/"pending" på admin-fakturaer bevares.
        "paymentStatus": (o.get("paymentStatus") or "paid"),
        "paymentMethod": o.get("paymentMethod") or "",
        # Behold de opprinnelige feltene også, så ny.havoyet.no-spesifikke ting
        # (boxSelection, fee, sum osv.) er fortsatt tilgjengelig for iPad-en.
        "_raw":       o,
    }


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
        _prisliste["last_sync"] = _now_iso_utc()
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

# Pretty URLs uten .html — Cloudflare foran bestilling.havoyet.no gjør 308 fra
# `*.html` til extension-less. Vi eksponerer derfor de samme HTML-filene under
# extension-less ruter så de tre subsidene fungerer både direkte (Render) og
# via CDN-redirect.
_PRETTY_PAGES = {
    "kalender":         "kalender.html",
    "pakke":            "pakke.html",
    "lager":            "lager.html",
    "admin":            "admin.html",
    "betalinger":       "betalinger.html",
    "butikk":           "butikk.html",
    "etikett":          "etikett.html",
    "ptouch":           "ptouch.html",
    "nesttun-admin":    "nesttun-admin.html",
    "tracking-admin":   "tracking-admin.html",
}

@app.route("/<page>")
def serve_pretty(page):
    fname = _PRETTY_PAGES.get(page)
    if fname and os.path.exists(os.path.join(_BASE_DIR, fname)):
        return send_from_directory(_BASE_DIR, fname)
    # Fall through til catch-all (returnerer 404 hvis fil ikke finnes)
    return serve_static(page)

@app.route("/<path:filename>")
def serve_static(filename):
    return send_from_directory(_BASE_DIR, filename)

# ── API ────────────────────────────────────────────────────────────────────────
_VIPPS_PAID_STATES   = {"AUTHORIZED", "CAPTURED", "CHARGED", "COMPLETED", "RESERVED"}
_STRIPE_PAID_STATES  = {"PAID"}

def _build_product_cost_map():
    """Bygg {navn_lc: {price, cost, ratio}} fra baseline + overrides.
    Brukes til kost-beregning i økonomi-stats. ratio = cost/price så vi kan
    beregne linje-kost proporsjonalt fra linje-pris (uavhengig av enhet)."""
    try:
        products = _get_products_baseline()
    except Exception:
        products = []
    overrides = _product_overrides or {}
    out = {}
    seen_slugs = set()
    for p in products:
        slug = p.get("slug")
        if slug:
            seen_slugs.add(slug)
        ov = overrides.get(slug, {})
        merged = {**p, **(ov if isinstance(ov, dict) else {})}
        name = (merged.get("name") or "").strip().lower()
        try:
            price = float(merged.get("price") or 0)
            cost  = float(merged.get("cost") or 0)
        except (TypeError, ValueError):
            continue
        if name and price > 0 and cost > 0:
            out[name] = {"price": price, "cost": cost, "ratio": cost / price}
    # Overrides-only-produkter (auto-opprettet via Excel-import)
    for slug, ov in overrides.items():
        if slug in seen_slugs or not isinstance(ov, dict):
            continue
        name = (ov.get("name") or "").strip().lower()
        if not name:
            continue
        try:
            price = float(ov.get("price") or 0)
            cost  = float(ov.get("cost") or 0)
        except (TypeError, ValueError):
            continue
        if price > 0 and cost > 0 and name not in out:
            out[name] = {"price": price, "cost": cost, "ratio": cost / price}
    return out


def _order_cost_kr(order, cost_map):
    """Beregn innkjøpskostnad for én ordre. Prioritetsrekkefølge per linje:

    1) `item.lineCost` — eksakt linje-kost lagret ved ordretidspunkt (best,
       overlever produkt-rename og prisendringer i etterkant).
    2) `item.cost` × qty — snapshot av enhets-kost lagret ved opprettelse,
       men kun hvis ingen lineCost finnes.
    3) Proporsjonal fallback (line.pris × cost_map[name].ratio) for gamle
       ordrer fra før vi begynte å snapshote kost på linja."""
    if not isinstance(order, dict):
        return 0.0
    items = order.get("varer") or order.get("prods") or order.get("items") or []
    if not isinstance(items, list):
        return 0.0
    total = 0.0
    for it in items:
        if not isinstance(it, dict):
            continue
        # 1) Lagret linje-kost — eksakt, beste kilde
        try:
            stored_line = it.get("lineCost")
            if stored_line is not None:
                v = float(stored_line)
                if v > 0:
                    total += v
                    continue
        except (TypeError, ValueError):
            pass
        # 2) Enhets-kost × qty (når lineCost mangler men cost er snapshotet)
        try:
            unit_cost = it.get("cost")
            if unit_cost is not None:
                uc = float(unit_cost)
                if uc > 0:
                    qty = float(it.get("qty") or it.get("quantity") or 1)
                    total += uc * qty
                    continue
        except (TypeError, ValueError):
            pass
        # 3) Proporsjonal fallback (gamle ordrer uten cost-snapshot)
        name = (it.get("name") or it.get("navn") or it.get("title") or "").strip().lower()
        info = cost_map.get(name)
        if not info:
            continue
        line_price = it.get("pris")
        if line_price is None:
            try:
                qty = float(it.get("qty") or it.get("quantity") or 1)
                unit_price = float(it.get("price") or 0)
                line_price = qty * unit_price
            except (TypeError, ValueError):
                line_price = 0
        try:
            line_price = float(line_price)
        except (TypeError, ValueError):
            line_price = 0.0
        if line_price > 0:
            total += line_price * info["ratio"]
    return total


def _settled_ordrenrs():
    """Ordre-numre som er "oppgjort" — paid ELLER free. Brukes for kost-
    beregning (gratis-ordre koster oss like mye uansett om vi fakturerer)."""
    out = set(_paid_ordrenrs())
    try:
        for o in (_manual_orders or []):
            if not isinstance(o, dict):
                continue
            ps = (o.get("paymentStatus") or "").lower()
            if ps == "free":
                ordrenr = str(o.get("ordrenr") or o.get("id") or "").strip()
                if ordrenr:
                    out.add(ordrenr)
    except Exception:
        pass
    return out


def _paid_ordrenrs():
    """Returnerer settet av ordrenumre som har bekreftet betaling.
    Tre kilder:
      1. Stripe (Stripe paid_states i payments-cache)
      2. Vipps (Vipps paid_states)
      3. Manuelt markert som 'paid' i admin-drawer (paymentStatus='paid')
         eller via Shopify orders_export-import (samme felt) — disse må også
         med i økonomi-statistikken så manuelt registrerte betalinger
         oppdateres umiddelbart i Omsetning."""
    paid = set()
    try:
        for rec in _stripe_load_payments().values():
            if rec.get("state") in _STRIPE_PAID_STATES and rec.get("ordrenr"):
                paid.add(str(rec["ordrenr"]))
    except Exception:
        pass
    try:
        for rec in _vipps_load_payments().values():
            if rec.get("state") in _VIPPS_PAID_STATES and rec.get("ordrenr"):
                paid.add(str(rec["ordrenr"]))
    except Exception:
        pass
    try:
        for o in (_manual_orders or []):
            if not isinstance(o, dict):
                continue
            try:
                ps = o.get("paymentStatus") or ""
                if str(ps).lower() == "paid":
                    ordrenr = str(o.get("ordrenr") or o.get("id") or "").strip()
                    if ordrenr:
                        paid.add(ordrenr)
            except Exception:
                continue
    except Exception:
        pass
    return paid


def _all_orders_normalized(only_paid=True):
    """Bygger den normaliserte ordre-listen fra _manual_orders.
    Default: alle ordre staff skal pakke — betalte web-ordre (Stripe/Vipps),
    manuelt opprettede admin-ordre, og Shopify-importerte historiske ordre.
    Ekskluderer kun ubetalte web-checkout-ordre (forlatte handlekurver).
    Settes only_paid=False for å få alle (admin-tools)."""
    if only_paid:
        paid = _paid_ordrenrs()
        source = []
        for o in _manual_orders:
            ordrenr  = str(o.get("ordrenr") or o.get("id") or "")
            status   = (o.get("status") or "").upper()
            ps_norm  = (o.get("paymentStatus") or "").lower()
            # Match admin-logikken: en ordre regnes som "oppgjort" (synlig for
            # staff/p-touch/pakke-iPad) med mindre paymentStatus eksplisitt sier
            # ubetalt. Tomt felt eller "free" → vises. Det hindrer at admin og
            # bestillingsside viser ulikt sett ordrer.
            is_unpaid_explicit = ps_norm in ("unpaid", "pending", "awaiting_payment")
            is_paid  = (
                ordrenr in paid
                or status in ("PAID", "PAID_OUT")
                or (not is_unpaid_explicit and ps_norm in ("paid", "free", ""))
            )
            # Sjekk både "source" og "kilde" — historiske Shopify-imports bruker "kilde",
            # mens nye webbestillinger fra havoyet.no/kasse setter "source".
            kilde    = (o.get("kilde") or "").lower()
            src      = (o.get("source") or "").lower()
            is_staff = (
                bool(o.get("manual"))
                or src in ("admin", "shopify")
                or "shopify" in kilde
                or "import" in kilde
                or kilde.startswith("admin")
            )
            # Ubetalte admin-fakturaer skal også med i staff-listen (ikke i
            # omsetning, men vi pakker og leverer dem). is_staff fanger dem opp.
            if is_paid or is_staff:
                source.append(o)
    else:
        source = list(_manual_orders)
    orders = [_normalize_manual_order(o) for o in source]
    orders.sort(key=lambda o: (o.get("delivery") or "9999-99-99"))
    return orders


@app.route("/api/orders")
def api_orders():
    """Aktive, BETALTE ordre fra ny.havoyet.no/kasse i iPad-shape.
    Sett ?include_unpaid=1 for å inkludere ubetalte (kun til admin-bruk)."""
    only_paid = request.args.get("include_unpaid") != "1"
    orders = _all_orders_normalized(only_paid=only_paid)
    return jsonify({
        "orders":    orders,
        "last_sync": _now_iso_utc(),
        "error":     None,
        "count":     len(orders),
        "source":    "havoyet.no",
        "filter":    "paid_only" if only_paid else "all",
    })


@app.route("/api/orders/<order_id>")
def api_order(order_id):
    match = next((o for o in _manual_orders
                  if str(o.get("ordrenr") or o.get("id")) == str(order_id)), None)
    if not match:
        return jsonify({"error": "Ikke funnet"}), 404
    return jsonify(_normalize_manual_order(match))


@app.route("/api/sync", methods=["POST"])
def api_sync():
    """Helsesjekk-endepunkt — kunde-checkout skriver direkte til _manual_orders,
    så det er ingenting å hente. Beholdt for bakoverkompatibilitet."""
    return jsonify({
        "ok":        True,
        "count":     len(_manual_orders),
        "last_sync": _now_iso_utc(),
        "error":     None,
        "source":    "havoyet.no",
    })


@app.route("/api/status")
def api_status():
    return jsonify({
        "source":    "havoyet.no",
        "last_sync": _now_iso_utc(),
        "count":     len(_manual_orders),
        "error":     None,
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
# Kunde-varsler-konfig: styres fra admin.html "Kunde-varsler"-seksjonen.
# Hver nøkkel: {enabled, channel:'email'|'sms'|'both', subject, body, hours_before?}
_CUSTOMER_NOTIFY_DEFAULTS = {
    "order_confirmation": {
        "enabled": True, "channel": "email",
        "subject": "Bekreftelse på bestilling #{ordrenr} — Havøyet",
        "body": ("Hei {navn},\n\nTusen takk for bestillingen din! Vi har "
                 "registrert ordre #{ordrenr}.\n\nLevering: {leveringsdag} kl. "
                 "{leveringstid}\n\nDu kan følge status og live ETA på «Min "
                 "side»: {kontolenke}\n\nSpørsmål? Svar på denne e-posten "
                 "eller send til erik@havoyet.no.\n\n— Havøyet"),
    },
    "delivery_notice": {
        "enabled": True, "channel": "both",
        "subject": "Bestillingen din #{ordrenr} er levert — Havøyet",
        "body": ("Hei {navn},\n\nBestillingen din #{ordrenr} er nå merket som "
                 "levert. Tusen takk for at du handlet hos Havøyet!\n\n"
                 "{kontolenke}\n\n— Havøyet"),
    },
    "status_update": {
        "enabled": True, "channel": "email",
        "subject": "Oppdatering på bestillingen din #{ordrenr} — Havøyet",
        "body": ("Hei {navn},\n\nBestillingen din #{ordrenr} har blitt "
                 "oppdatert.\n\nStatus: {status}\nLevering: {leveringsdag} "
                 "kl. {leveringstid}\n\nFølg bestillingen på {kontolenke}\n\n"
                 "— Havøyet"),
    },
    "delivery_reminder": {
        "enabled": False, "channel": "sms", "hours_before": 2,
        "subject": "Vi leverer bestillingen din i dag — Havøyet",
        "body": ("Hei {navn}! Vi er på vei med bestilling #{ordrenr} om "
                 "ca. {kontolenke}"),
    },
}
_customer_notify_config = {k: dict(v) for k, v in _CUSTOMER_NOTIFY_DEFAULTS.items()}
_customers = []  # [{id, navn, tlf, epost, adresse, kommentar, created_at}]
# Vipps-betalinger importert fra Vipps Bedrift CSV. Keyed by Vipps transaction ID
# slik at re-import av overlappende CSV ikke duplikerer rader.
# Hver verdi: {transaction_id, date, time, amount_ore, type, description, phone,
#              imported_at, source: 'vipps_csv'}
_vipps_imported_payments = {}
# Shopify Payments / kortbetalinger importert fra Shopify CSV-eksport.
# Keyed by synthetic ID (hash av transaction_date + order + amount).
_card_payments_imported = {}
_auth_users = []   # [{email, role, password_hash, must_set_password, created_at}]
_auth_sessions = {}  # legacy lookup — beholdes for bakoverkompatibilitet med eldre tokens
# ── NYHETSBREV-ABONNENTER (sannhetskilde — erstatter MailerLite per 2026-05-11) ──
# Hver entry: {id, email, navn, status, kilde, created_at, updated_at,
#              unsubscribed_at, tags[], mailerlite_id (legacy ref)}
# status: 'active' | 'unsubscribed' | 'bounced'
# kilde: 'website' | 'admin' | 'mailerlite-migration' | 'checkout'
_subscribers = []
# ── PRODUKT-RABATTER (knyttet til nyhetsbrev-sendinger eller manuelt opprettet) ──
# Hver entry: {id, handle, prosent (0-100), start (YYYY-MM-DD), slutt (YYYY-MM-DD),
#              beskrivelse, kun_nyhetsbrev (bool), aktiv (bool),
#              kilde_newsletter_id (str|null), created_at, updated_at}
_discounts = []
# Sesjoner utløper ikke lenger automatisk; brukeren forblir innlogget til de
# selv logger ut. Fjern token manuelt via /api/auth/logout for å invalidere det.
AUTH_SESSION_TTL = None

# ── STATELESS AUTH-TOKEN (HMAC-signed) ───────────────────────────────────────
# Tokens er nå selv-bekreftende slik at de overlever Render-restarts uten å
# trenge persistent disk. Sett SECRET_KEY i Render-env for eksplisitt kontroll;
# ellers utleder vi en stabil nøkkel fra Stripe/Vipps-secrets som allerede er
# satt på serveren (de roteres aldri ved deploy).
import hmac as _hmac_mod
import hashlib as _hashlib_mod
import base64 as _base64
import json as _json_mod

_AUTH_SECRET = os.environ.get("SECRET_KEY", "").strip()
if not _AUTH_SECRET:
    _seed = (STRIPE_SECRET_KEY or "") + (VIPPS_CLIENT_SECRET or "") + "havoyet-auth-2026-v1"
    if _seed.strip("havoyet-auth-2026-v1"):
        _AUTH_SECRET = _hashlib_mod.sha256(_seed.encode("utf-8")).hexdigest()
        print("[AUTH] SECRET_KEY ikke satt — utledet stabil nøkkel fra Stripe/Vipps-secrets")
    else:
        # Worst case: helt frisk Render uten Stripe/Vipps-keys konfigurert.
        # Bruk en hard-kodet konstant slik at sesjoner i det minste overlever
        # innenfor én container-instans. Brukeren bør sette SECRET_KEY.
        _AUTH_SECRET = "havoyet-fallback-secret-CHANGE-ME-via-SECRET_KEY-env"
        print("[AUTH] ADVARSEL: ingen SECRET_KEY/Stripe/Vipps — sett SECRET_KEY i Render env!")


def _make_stateless_token(email, role):
    payload = {"email": email, "role": role, "iat": int(time.time())}
    payload_json = _json_mod.dumps(payload, separators=(",", ":"), sort_keys=True)
    payload_b64 = _base64.urlsafe_b64encode(payload_json.encode("utf-8")).rstrip(b"=").decode("ascii")
    sig = _hmac_mod.new(_AUTH_SECRET.encode("utf-8"), payload_b64.encode("ascii"), "sha256").hexdigest()
    return f"hv1.{payload_b64}.{sig}"


def _verify_stateless_token(token):
    if not token or not token.startswith("hv1."):
        return None
    parts = token.split(".", 2)
    if len(parts) != 3:
        return None
    _, payload_b64, sig = parts
    expected = _hmac_mod.new(_AUTH_SECRET.encode("utf-8"), payload_b64.encode("ascii"), "sha256").hexdigest()
    if not _hmac_mod.compare_digest(sig, expected):
        return None
    try:
        padding = "=" * (-len(payload_b64) % 4)
        payload_json = _base64.urlsafe_b64decode(payload_b64 + padding).decode("utf-8")
        return _json_mod.loads(payload_json)
    except Exception:
        return None

def _save_sync_state():
    """Persist cross-device sync state to disk — atomisk skriving.

    Tidligere åpnet vi SYNC_STATE_FILE direkte i "w"-modus, noe som trunkerte
    fila før json.dump ble fullført. Hvis serialiseringen kastet (eller
    prosessen ble drept midt i), endte vi opp med en tom/korrupt fil →
    _auth_users gikk tapt ved neste oppstart → admin ble låst ute.

    Skriv først til en .tmp-fil og bytt så atomisk med os.replace, samme
    mønster som SUBSCRIPTIONS_FILE/ANALYTICS_FILE/CHAT_SESSIONS_FILE bruker.
    """
    try:
        payload = {
            "manual_orders":     _manual_orders,
            "hidden_orders":     _hidden_orders,
            "overrides":         _overrides,
            "packing_state":     _packing_state,
            "order_notes":       _order_notes,
            "product_overrides":   _product_overrides,
            "reviews":             _reviews,
            "customer_favorites":  _customer_favorites,
            "admin_notifiers":     _admin_notifiers,
            "customer_notify_config": _customer_notify_config,
            "customers":           _customers,
            "vipps_imported_payments": _vipps_imported_payments,
            "card_payments_imported":  _card_payments_imported,
            "auth_users":          _auth_users,
            "auth_sessions":       _auth_sessions,
            "subscribers":         _subscribers,
            "discounts":           _discounts,
        }
        tmp = SYNC_STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, SYNC_STATE_FILE)
    except Exception as e:
        # Ikke svelg feilen i stillhet — logg så vi ser om noe i payloaden
        # ikke er JSON-serialiserbart i fremtidige patcher.
        print(f"[SYNC-SAVE] FEIL ved persistens: {e}")
        try:
            tmp = SYNC_STATE_FILE + ".tmp"
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

def _restore_baseline_if_empty(name, current, file_basename):
    """Generic auto-restore fra committed baseline-snapshot. Returnerer
    den lastede strukturen hvis 'current' er tom OG snapshot-fila finnes,
    ellers None."""
    if current:
        return None
    baseline = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "data", file_basename)
    if not os.path.exists(baseline):
        return None
    try:
        with open(baseline, "r", encoding="utf-8") as f:
            data = json.load(f)
        size = len(data) if data else 0
        print(f"[STATE] Restored {size} {name} fra baseline")
        return data
    except Exception as e:
        print(f"[STATE] Kunne ikke lese {file_basename}: {e}")
        return None


def _restore_vipps_baseline():
    """Auto-restore Vipps-transaksjoner, kortbetalinger og kunder fra committed
    baseline-snapshots. Brukes som fall-back når Render-restart wiper /tmp."""
    global _vipps_imported_payments, _card_payments_imported, _customers
    restored_any = False

    vipps = _restore_baseline_if_empty("Vipps-transaksjoner",
                                       _vipps_imported_payments,
                                       "vipps_baseline.json")
    if vipps is not None:
        _vipps_imported_payments = vipps
        restored_any = True

    card = _restore_baseline_if_empty("kortbetalinger",
                                      _card_payments_imported,
                                      "card_payments_baseline.json")
    if card is not None:
        _card_payments_imported = card
        restored_any = True

    cust = _restore_baseline_if_empty("kunder",
                                      _customers,
                                      "customers_baseline.json")
    if cust is not None and isinstance(cust, list):
        _customers = cust
        restored_any = True

    if restored_any:
        _save_sync_state()


def _load_sync_state():
    """Load cross-device sync state from disk on startup."""
    global _manual_orders, _hidden_orders, _overrides, _packing_state, _order_notes, _product_overrides, _reviews, _customer_favorites, _admin_notifiers, _customer_notify_config, _customers, _vipps_imported_payments, _card_payments_imported, _auth_users, _auth_sessions, _subscribers, _discounts
    if not os.path.exists(SYNC_STATE_FILE):
        _seed_auth_users()
        _restore_vipps_baseline()
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
        # Slå sammen lagret kunde-varsler-konfig over defaults (per-felt fallback
        # så nye nøkler får fornuftige verdier ved oppgraderinger).
        saved_kvc = d.get("customer_notify_config") or {}
        merged_kvc = {k: dict(v) for k, v in _CUSTOMER_NOTIFY_DEFAULTS.items()}
        for k, v in saved_kvc.items():
            if k in merged_kvc and isinstance(v, dict):
                merged_kvc[k].update(v)
        _customer_notify_config = merged_kvc
        _customers          = d.get("customers", [])
        _vipps_imported_payments = d.get("vipps_imported_payments", {}) or {}
        _card_payments_imported  = d.get("card_payments_imported", {}) or {}
        # Auto-restore baseline hvis ingen Vipps/kort-data ble lastet
        if not _vipps_imported_payments or not _card_payments_imported:
            _restore_vipps_baseline()
        _auth_users         = d.get("auth_users", [])
        loaded_sessions     = d.get("auth_sessions", {}) or {}
        if AUTH_SESSION_TTL is None:
            _auth_sessions  = {
                tok: sess for tok, sess in loaded_sessions.items()
                if isinstance(sess, dict)
            }
        else:
            cutoff = int(time.time()) - AUTH_SESSION_TTL
            _auth_sessions  = {
                tok: sess for tok, sess in loaded_sessions.items()
                if isinstance(sess, dict) and int(sess.get("created_at", 0)) >= cutoff
            }
        _seed_auth_users()
        _subscribers       = d.get("subscribers", []) or []
        _discounts         = d.get("discounts", []) or []
        print(f"Lastet sync-state fra disk: {len(_packing_state)} pakket, {len(_manual_orders)} manuelle ordre, {len(_product_overrides)} produkt-overrides, {len(_reviews)} anmeldelser, {len(_admin_notifiers)} admin-mottakere, {len(_customers)} kunder, {len(_auth_users)} auth-brukere, {len(_auth_sessions)} aktive sesjoner, {len(_subscribers)} nyhetsbrev-abonnenter")
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


# ── PRODUKTLISTE (baseline + overrides) ────────────────────────────────────────
# Henter PRODUCTS-arrayen fra Vercel-nettsiden (admin.havoyet.no peker dit).
# havoyet.no selv er Shopify, så vi må bruke admin.havoyet.no eller
# Vercel-deploy-URL som baseline-kilde.
# Parser ut feltene vi trenger (slug/name/cat/price/kind/unitLabel/img/status),
# og merger gjeldende overrides på toppen. Caches 5 min så vi ikke hamrer Vercel.

_PRODUCTS_BASELINE_URL = os.environ.get(
    "PRODUCTS_BASELINE_URL",
    "https://admin.havoyet.no/components/data2.jsx",
)
_PRODUCTS_CACHE = {"data": None, "ts": 0.0}
_PRODUCTS_TTL = 300  # 5 minutter

# Strengfelter vi vil hente ut fra hver produkt-objekt-blokk i data2.jsx
_PRODUCT_STR_FIELDS = (
    "slug", "name", "cat", "kind", "unitLabel", "img", "tag", "desc",
    "origin", "weight", "shelf", "packed", "status",
)


def _parse_products_from_data2(text):
    """Parser PRODUCTS-arrayen fra data2.jsx (JS-syntaks) til en liste av dicts.
    Henter ut feltene vi bruker i bestillingssiden — ikke hele struct.
    """
    import re as _re
    m = _re.search(r"const\s+PRODUCTS\s*=\s*\[", text)
    if not m:
        return []
    start_idx = m.end() - 1  # peker på "["
    # Finn matchende "]" — naivt nok ettersom data2.jsx ikke har "]" i strenger her
    depth = 0
    end_idx = None
    in_str = None  # 'sq' / 'dq' / None
    esc = False
    for i in range(start_idx, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif (in_str == "sq" and ch == "'") or (in_str == "dq" and ch == '"'):
                in_str = None
            continue
        if ch == "'":
            in_str = "sq"
        elif ch == '"':
            in_str = "dq"
        elif ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                end_idx = i
                break
    if end_idx is None:
        return []
    body = text[start_idx + 1:end_idx]

    # Splitt body på top-level "}" — dvs. der depth (kun {}) treffer 0 igjen
    products = []
    depth = 0
    obj_start = None
    in_str = None
    esc = False
    for i, ch in enumerate(body):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif (in_str == "sq" and ch == "'") or (in_str == "dq" and ch == '"'):
                in_str = None
            continue
        if ch == "'":
            in_str = "sq"
        elif ch == '"':
            in_str = "dq"
        elif ch == "{":
            if depth == 0:
                obj_start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and obj_start is not None:
                products.append(body[obj_start:i + 1])
                obj_start = None

    out = []
    for chunk in products:
        prod = {}
        for key in _PRODUCT_STR_FIELDS:
            mm = _re.search(rf'\b{key}\s*:\s*"((?:[^"\\]|\\.)*)"', chunk)
            if not mm:
                mm = _re.search(rf"\b{key}\s*:\s*'((?:[^'\\]|\\.)*)'", chunk)
            if mm:
                # JS escape-sekvenser bevarer vi som de er — godt nok for visning
                prod[key] = mm.group(1)
        mm = _re.search(r"\bprice\s*:\s*(-?\d+(?:\.\d+)?)", chunk)
        if mm:
            num = mm.group(1)
            prod["price"] = float(num) if "." in num else int(num)
        if prod.get("slug") and prod.get("name"):
            prod.setdefault("status", "available")
            prod.setdefault("kind", "fish")
            out.append(prod)
    return out


def _get_products_baseline(force_refresh=False):
    global _PRODUCTS_CACHE
    now = time.time()
    if not force_refresh and _PRODUCTS_CACHE["data"] and (now - _PRODUCTS_CACHE["ts"] < _PRODUCTS_TTL):
        return [dict(p) for p in _PRODUCTS_CACHE["data"]]
    try:
        r = requests.get(_PRODUCTS_BASELINE_URL, timeout=8)
        r.raise_for_status()
        data = _parse_products_from_data2(r.text)
        if data:
            _PRODUCTS_CACHE = {"data": data, "ts": now}
            return [dict(p) for p in data]
    except Exception as e:
        print(f"[products] Klarte ikke hente baseline: {e}")
    # Fallback: returner cache uansett alder hvis vi har noe
    if _PRODUCTS_CACHE["data"]:
        return [dict(p) for p in _PRODUCTS_CACHE["data"]]
    return []


@app.route("/api/products/list", methods=["GET"])
def api_products_list():
    """Returnerer komplett produktliste (baseline + overrides anvendt).
    Brukes av varer.html på bestillingssiden til å redigere tilgjengelighet.
    Query: ?refresh=1 tvinger ny henting av data2.jsx."""
    force = request.args.get("refresh") in ("1", "true", "yes")
    products = _get_products_baseline(force_refresh=force)
    overrides = _product_overrides or {}
    for p in products:
        ov = overrides.get(p.get("slug"))
        if isinstance(ov, dict):
            p.update(ov)
    cats = []
    seen = set()
    for p in products:
        c = p.get("cat")
        if c and c not in seen:
            seen.add(c)
            cats.append(c)
    return jsonify({
        "ok": True,
        "count": len(products),
        "cats": cats,
        "products": products,
        "cached_age": int(time.time() - _PRODUCTS_CACHE["ts"]) if _PRODUCTS_CACHE["ts"] else None,
    })


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
# Public-site-URL brukes i kunde-mails (lenke til /konto-dashbord). Override via env
# i tilfelle prod-domenet endres senere.
PUBLIC_SITE_URL  = os.environ.get("PUBLIC_SITE_URL", "https://havoyet.no").rstrip("/")
# Resend (anbefalt — https://resend.com) — sett RESEND_API_KEY i .env
RESEND_API_KEY   = os.environ.get("RESEND_API_KEY", "")
RESEND_FROM      = os.environ.get("RESEND_FROM", "onboarding@resend.dev")  # default test-adresse
# SMTP (alternativ — Gmail o.l.)
SMTP_HOST        = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT        = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER        = os.environ.get("SMTP_USER", "")
SMTP_PASS        = os.environ.get("SMTP_PASS", "")
CONTACT_LOG_FILE = os.path.join(os.path.dirname(_BASE_DIR), "contact_messages.jsonl")


# ── E-POST-SIGNATUR ────────────────────────────────────────────────────────
# Brukes som footer på alle utgående e-poster. Endres her — propageres til alle
# avsendere (kontaktskjema, ordrebekreftelser, admin-varsler, statusoppdateringer).
_SIGNATURE_TEXT = """

--
Med vennlig hilsen,

Erik Øye
Daglig leder | Havøyet
Mobil: +47 416 39 788
Nettside: www.havoyet.no

Fersk fisk og skalldyr levert hjem i Bergen
"""

# Kortform til SMS-utgang. Twilio-segmentet er 160 tegn, så vi holder
# signaturen så liten som mulig (under 20 tegn inkl. linjeskift).
_SMS_SIGNATURE = "\n\nMvh,\nErik\nHavøyet"

_SIGNATURE_HTML = """
<table cellpadding="0" cellspacing="0" border="0" style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;color:#1a1a1a;font-size:14px;line-height:1.55;margin-top:28px;border-top:1px solid #e6e1d6;padding-top:18px;">
  <tr><td>
    <p style="margin:0 0 12px 0;font-style:italic;">Med vennlig hilsen,</p>
    <p style="margin:0;">
      <strong style="color:#1d6fc9;font-size:16px;">Erik Øye</strong><br/>
      Daglig leder | Havøyet<br/>
      Mobil: <a href="tel:+4741639788" style="color:#1d6fc9;text-decoration:none;">+47 416 39 788</a><br/>
      Nettside: <a href="https://www.havoyet.no" style="color:#1d6fc9;text-decoration:none;">www.havoyet.no</a>
    </p>
    <p style="margin:14px 0 16px 0;font-style:italic;">Fersk fisk og skalldyr levert hjem i Bergen</p>
    <a href="https://www.havoyet.no" style="text-decoration:none;display:inline-block;"><img src="https://admin.havoyet.no/assets/logo-email.png" alt="Havøyet — Bare fersk sjømat" width="240" style="display:block;max-width:240px;height:auto;border:0;" /></a>
  </td></tr>
</table>
"""

import re as _re_mod
_IMG_PLACEHOLDER_RE = _re_mod.compile(r"\[IMG:(https?://[^\s\]]+)\]")


def _strip_image_placeholders(body):
    """Fjern [IMG:url]-placeholders fra tekstversjon av e-post / SMS."""
    if not body:
        return body
    return _IMG_PLACEHOLDER_RE.sub("", body)


def _body_to_html(body):
    """Konverterer plain-text-body til enkel HTML — escape + linebreaks.
    Ekspanderer [IMG:https://...]-placeholders til <img>-tags slik at admin
    kan bygge inn produkt-/kvitterings-/bilder i kunde-mails fra
    Kunde-varsler-seksjonen."""
    import html as _html
    body = body or ""
    # Beskytt bilde-placeholders før HTML-escape, og bytt dem ut etterpå
    stash = []
    def _grab(match):
        stash.append(match.group(1))
        return f"\x00IMG{len(stash)-1}\x00"
    safe = _IMG_PLACEHOLDER_RE.sub(_grab, body)
    escaped = _html.escape(safe)
    def _expand(m):
        url = _html.escape(stash[int(m.group(1))], quote=True)
        return (
            f'</div><img src="{url}" alt="" '
            f'style="display:block;max-width:100%;height:auto;'
            f'border-radius:8px;margin:14px 0;border:0;" />'
            f'<div style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Helvetica,Arial,sans-serif;color:#1a1a1a;font-size:14px;line-height:1.55;white-space:pre-wrap;">'
        )
    expanded = _re_mod.sub(r"\x00IMG(\d+)\x00", _expand, escaped)
    return (
        '<div style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Helvetica,Arial,sans-serif;'
        f'color:#1a1a1a;font-size:14px;line-height:1.55;white-space:pre-wrap;">{expanded}</div>'
    )


def _send_via_resend(from_email, from_name, subject, body, to_email=None, reply_to=None):
    """Send via Resend API (enklest — bare API-nøkkel trengs).
    Legger automatisk ved signatur (text + html) på alle utgående e-poster."""
    try:
        text_body = _strip_image_placeholders(body or "") + _SIGNATURE_TEXT
        html_body = _body_to_html(body) + _SIGNATURE_HTML
        payload = {
            "from": f"Havøyet <{RESEND_FROM}>",
            "to": [to_email or CONTACT_TO],
            "subject": subject,
            "text": text_body,
            "html": html_body,
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
    """Send via SMTP (Gmail / annen SMTP-server).
    Sender multipart/alternative med både text- og HTML-versjon med signatur."""
    recipient = to_email or CONTACT_TO
    text_body = _strip_image_placeholders(body or "") + _SIGNATURE_TEXT
    html_body = _body_to_html(body) + _SIGNATURE_HTML
    msg = _MIMEMultipart("alternative")
    msg["From"]     = _formataddr((f"Havøyet – {from_name}", SMTP_USER))
    if reply_to or from_email:
        msg["Reply-To"] = reply_to or from_email
    msg["To"]       = recipient
    msg["Subject"]  = subject
    msg.attach(_MIMEText(text_body, "plain", "utf-8"))
    msg.attach(_MIMEText(html_body, "html",  "utf-8"))
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

    # Ingen sending konfigurert — IKKE returner suksess, kunden skal få beskjed
    # om at meldingen ikke kom fram. Disk-loggen beholdes som backup, men API
    # returnerer 5xx slik at kontakt-skjemaet viser feil i stedet for «sendt».
    print(f"[CONTACT] Ingen mail-tjeneste konfigurert — meldingen ble logget til {CONTACT_LOG_FILE}")
    print(f"[CONTACT] Sett RESEND_API_KEY (anbefalt) eller SMTP_USER/SMTP_PASS i .env")
    return False, "no-mail-service-configured"


# ── ADMIN-VARSLER ──────────────────────────────────────────────────────────────
# Send e-post + SMS til registrerte admin-mottakere ved nye/oppdaterte/leverte
# ordre og innkommende kontaktmeldinger.
ADMIN_EVENTS = ("new_order", "order_updated", "order_delivered", "new_message")
ADMIN_NOTIFY_LOG = os.path.join(os.path.dirname(_BASE_DIR), "admin_notifications.jsonl")

# Sveve (primær SMS-leverandør — norsk, gratis sender-ID-godkjenning)
SVEVE_USER   = os.environ.get("SVEVE_USER", "").strip()
SVEVE_PASS   = os.environ.get("SVEVE_PASS", "").strip()
SVEVE_SENDER = os.environ.get("SVEVE_SENDER", "Havøyet").strip()

# Twilio (fallback — beholdt for evt. retur til denne leverandøren)
TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN  = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM        = os.environ.get("TWILIO_FROM", "")  # f.eks. "+4790000000"

# ntfy.sh — gratis push-varsel til mobil. Mottaker installerer ntfy-appen og
# abonnerer på sin egen hemmelige topic. Default-server er ntfy.sh; kan
# overstyres via NTFY_SERVER for selv-hostet versjon.
NTFY_SERVER = os.environ.get("NTFY_SERVER", "https://ntfy.sh").rstrip("/")

# Telegram bot — gratis push med rik formatering. Opprettes via @BotFather
# på Telegram. Sett TELEGRAM_BOT_TOKEN i .env / Render-env. Hver admin-mottaker
# starter en chat med boten og registrerer sin chat_id i admin-UI.
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_API       = "https://api.telegram.org"


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


def _normalize_telegram_chat_id(raw):
    """Telegram chat_id er enten et tall (privat chat / kanal) eller en
    @kanalbrukernavn-streng. Aksepter begge. Returner str eller None."""
    if raw is None:
        return None
    t = str(raw).strip()
    if not t:
        return None
    if t.startswith("@"):
        # Kanal-brukernavn — Telegram krever bokstaver/tall/_ og 5–32 tegn
        import re as _re
        if _re.fullmatch(r"@[A-Za-z][A-Za-z0-9_]{4,31}", t):
            return t
        return None
    # Tall (kan være negativt for grupper). Tillat valgfritt fortegn.
    if t.lstrip("-").isdigit():
        return t
    return None


def _send_admin_telegram(chat_id, subject, body):
    """Send melding via Telegram-bot. Bruker Markdown-format med fet skrift
    på subject. Trimmer til 4096 tegn (Telegrams maks)."""
    if not TELEGRAM_BOT_TOKEN:
        return False, "telegram-not-configured"
    norm = _normalize_telegram_chat_id(chat_id)
    if not norm:
        return False, "telegram-invalid-chat-id"
    # Bygg en lesbar melding. Bruk MarkdownV2 ville krevd escaping av mange tegn,
    # så vi bruker plain HTML som er enklere og likevel støtter <b>.
    safe_subject = (subject or "Havøyet")
    for k, v in (("&", "&amp;"), ("<", "&lt;"), (">", "&gt;")):
        safe_subject = safe_subject.replace(k, v)
    safe_body = body or ""
    for k, v in (("&", "&amp;"), ("<", "&lt;"), (">", "&gt;")):
        safe_body = safe_body.replace(k, v)
    text = f"<b>{safe_subject}</b>\n\n<pre>{safe_body}</pre>"
    if len(text) > 4096:
        text = text[:4090] + "…</pre>"
    try:
        r = requests.post(
            f"{TELEGRAM_API}/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": norm,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if 200 <= r.status_code < 300:
            data = r.json()
            if data.get("ok"):
                return True, "sent-via-telegram"
            return False, f"telegram-api-error: {data.get('description','')[:200]}"
        return False, f"telegram-{r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, f"telegram-exception: {e}"


def _sanitize_sender_id(s):
    """Twilio Alphanumeric Sender ID må kun inneholde A-Z, 0-9, mellomrom (maks 11 tegn).
    Konverterer norske tegn til ASCII (ø→o, æ→a, å→a) og trimmer ulovlige tegn."""
    if not s:
        return s
    # Hvis det er et telefonnummer (+47…) eller starter med tall: la stå urørt.
    if s.startswith("+") or (s and s[0].isdigit()):
        return s
    mapping = str.maketrans({"ø": "o", "Ø": "O", "æ": "a", "Æ": "A", "å": "a", "Å": "A"})
    cleaned = s.translate(mapping)
    cleaned = "".join(c for c in cleaned if c.isalnum() or c == " ")
    return cleaned[:11]  # Twilio cap


def _send_via_sveve(to_phone, msg, sender):
    """Send SMS via Sveve (sveve.no). Returnerer (ok, detail)."""
    if not (SVEVE_USER and SVEVE_PASS):
        return False, "sveve-not-configured"
    # Sveve aksepterer både +47-prefix og uten plusstegn. Strip + for sikkerhets skyld.
    to_clean = to_phone.lstrip("+")
    try:
        r = requests.get(
            "https://sveve.no/SMS/SendMessage",
            params={
                "user": SVEVE_USER,
                "passwd": SVEVE_PASS,
                "to": to_clean,
                "msg": msg,
                "from": sender,
                "f": "json",  # be om JSON-respons
            },
            timeout=15,
        )
        if r.status_code != 200:
            return False, f"sveve-{r.status_code}: {r.text[:200]}"
        # Sveve returnerer JSON med {response: {msgOkCount, stdSMSCount, ...}} ved suksess
        # eller {response: {errors: [...]}} ved feil.
        try:
            data = r.json()
        except Exception:
            return False, f"sveve-bad-response: {r.text[:200]}"
        resp = data.get("response") or {}
        ok_count = int(resp.get("msgOkCount") or 0)
        if ok_count > 0:
            return True, "sent-via-sveve"
        errors = resp.get("errors") or []
        err_text = "; ".join(str(e.get("message", e)) for e in errors) if errors else r.text[:200]
        return False, f"sveve-error: {err_text}"
    except Exception as e:
        return False, f"sveve-exception: {e}"


def _send_admin_sms(to_phone, body):
    """Send SMS — prøver Sveve først (primær), så Twilio (fallback).
    Trimmer til 1 SMS-segment (160 tegn) for å holde kostnaden lav."""
    msg = body if len(body) <= 160 else body[:157] + "…"

    # 1) Sveve (primær)
    if SVEVE_USER and SVEVE_PASS:
        sender = SVEVE_SENDER or "Havoyet"
        ok, detail = _send_via_sveve(to_phone, msg, sender)
        if ok:
            return True, detail
        # Hvis Sveve er konfigurert men feilet, returner den feilen (ikke fall tilbake silent)
        # — med mindre Twilio også er tilgjengelig som backup
        if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_FROM):
            return False, detail

    # 2) Twilio (fallback)
    if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_FROM:
        sender = _sanitize_sender_id(TWILIO_FROM)
        try:
            r = requests.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json",
                data={"From": sender, "To": to_phone, "Body": msg},
                auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
                timeout=15,
            )
            if r.status_code in (200, 201):
                return True, "sent-via-twilio"
            return False, f"twilio-{r.status_code}: {r.text[:200]}"
        except Exception as e:
            return False, f"twilio-exception: {e}"

    return False, "sms-not-configured"


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


def _notify_customer_order_update(order, event, change_summary=""):
    """Send e-post til kunden når admin oppdaterer eller leverer bestillingen
    deres. Inneholder alltid lenke til /konto-dashbord så kunden kan følge
    status selv. Stille no-op hvis kunden mangler e-post.

    `event` er en av "order_updated" / "order_delivered" — styrer overskrift.
    `change_summary` er en kort beskrivelse av hva som endret seg
    (f.eks. "Status endret fra 'NEW' til 'IN_PROGRESS'").
    """
    if not isinstance(order, dict):
        return False, "no-order"
    kunde = order.get("kunde") or {}
    epost = (kunde.get("epost") or kunde.get("email") or "").strip()
    has_mail_service = bool(RESEND_API_KEY or (SMTP_USER and SMTP_PASS))
    # Tillater at funksjonen fortsetter selv uten e-post — SMS-grenen lengre nede
    # kan fortsatt kjøre hvis kunden har telefon og Twilio er konfigurert.

    nr     = order.get("ordrenr") or order.get("id") or "?"
    navn   = (kunde.get("navn") or kunde.get("name") or "").strip()
    status = order.get("status") or ""
    levdag = kunde.get("leveringsdag") or order.get("delivery") or ""
    levtid = kunde.get("leveringstid") or order.get("slot") or ""
    konto  = f"{PUBLIC_SITE_URL}/konto"

    # Sjekk Kunde-varsler-konfig — admin kan slå av enkelte event-typer eller
    # styre hvilken kanal (e-post / SMS / begge) som brukes.
    cfg_key = "delivery_notice" if event == "order_delivered" else "status_update"
    cfg = (_customer_notify_config or {}).get(cfg_key) or {}
    if not cfg.get("enabled", True):
        return False, f"disabled-by-config({cfg_key})"
    channel = cfg.get("channel", "both")
    allow_email = channel in ("email", "both")
    allow_sms   = channel in ("sms", "both")

    tmpl_vars = {
        "navn": navn or "kunde",
        "ordrenr": nr,
        "status": status,
        "leveringsdag": levdag,
        "leveringstid": levtid,
        "kontolenke": konto,
    }
    subject = _kv_render(cfg.get("subject"), **tmpl_vars) or (
        f"Bestillingen din #{nr} er levert — Havøyet" if event == "order_delivered"
        else f"Oppdatering på bestillingen din #{nr} — Havøyet"
    )
    body_template = cfg.get("body") or (
        "Hei {navn},\n\nBestillingen din #{ordrenr} er oppdatert.\n\n{kontolenke}\n\n— Havøyet"
    )
    body = _kv_render(body_template, **tmpl_vars)
    if change_summary:
        body += f"\n\n— Endringer: {change_summary.strip()}"

    # Send via Resend → SMTP fallback. Skipper helt hvis kunden mangler e-post
    # eller mail-tjeneste ikke er konfigurert — SMS-delen kan fortsatt fyre.
    mail_ok = False
    mail_detail = "skipped" if not (epost and has_mail_service and allow_email) else "no-mail-service"
    if epost and has_mail_service and allow_email:
        if RESEND_API_KEY:
            mail_ok, mail_detail = _send_via_resend(
                CONTACT_TO, "Havøyet", subject, body, to_email=epost, reply_to=CONTACT_TO,
            )
            if mail_ok:
                print(f"[CUSTOMER-MAIL] Resend → {epost}: {mail_detail}")
            else:
                print(f"[CUSTOMER-MAIL] Resend feilet, prøver SMTP: {mail_detail}")
        if not mail_ok and SMTP_USER and SMTP_PASS:
            mail_ok, mail_detail = _send_via_smtp(
                CONTACT_TO, "Havøyet", subject, body, to_email=epost, reply_to=CONTACT_TO,
            )
            print(f"[CUSTOMER-MAIL] SMTP → {epost}: {mail_detail}")

    # SMS-varsel — bare hvis kunden har telefon og Twilio er konfigurert.
    # Kunden kan opt-out via kunde.notify.sms = False på ordren eller kunden-objektet.
    sms_ok = False
    sms_detail = "no-phone-or-twilio"
    tlf = (kunde.get("tlf") or kunde.get("phone") or order.get("phone") or "").strip()
    notify_pref = (kunde.get("notify") or order.get("notify") or {})
    sms_opt_in = notify_pref.get("sms", True) and not notify_pref.get("opted_out", False)
    if tlf and sms_opt_in and allow_sms and TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_FROM:
        # Normaliser telefon til E.164-likt format (Twilio krever +CC...).
        clean_phone = tlf.replace(" ", "").replace("-", "")
        if clean_phone and not clean_phone.startswith("+"):
            # Anta norsk nummer hvis 8 siffer
            digits = "".join(ch for ch in clean_phone if ch.isdigit())
            if len(digits) == 8:
                clean_phone = "+47" + digits
        # Bruk body-malen fra Kunde-varsler-konfigen + kort SMS-signatur.
        # [IMG:url]-placeholders strippes (SMS støtter ikke bilder).
        sms_body = _strip_image_placeholders(body or "").strip()
        if sms_body:
            sms_text = sms_body + _SMS_SIGNATURE
        else:
            # Fallback hvis admin har slettet body-malen — hold noe minimalt
            # nyttig sammen med signaturen.
            if event == "order_delivered":
                sms_text = f"Havøyet: bestilling #{nr} er levert. {konto}{_SMS_SIGNATURE}"
            else:
                sms_text = f"Havøyet: bestilling #{nr} oppdatert. {konto}{_SMS_SIGNATURE}"
        # _send_admin_sms gjenbrukes — den trimmer til 160 tegn og sender via Twilio.
        sms_ok, sms_detail = _send_admin_sms(clean_phone, sms_text)
        print(f"[CUSTOMER-SMS] {clean_phone}: {sms_detail}")

    if mail_ok or sms_ok:
        return True, f"mail={mail_detail}, sms={sms_detail}"
    return False, f"mail={mail_detail}, sms={sms_detail}"


VALID_CHANNELS = ("email", "sms", "push", "telegram")


def _event_channels_for(notifier, event):
    """Returnerer settet med kanaler som skal fyre for (notifier, event).

    Bakoverkompat: hvis `event_channels` mangler for en event, brukes alle
    kanaler som er konfigurert på notifier-en (gammel oppførsel)."""
    ec = notifier.get("event_channels") or {}
    if event in ec and isinstance(ec[event], list):
        return {c for c in ec[event] if c in VALID_CHANNELS}
    # Default: alle kanaler som har en verdi
    fallback = set()
    if notifier.get("email"):            fallback.add("email")
    if notifier.get("phone"):            fallback.add("sms")
    if notifier.get("ntfy_topic"):       fallback.add("push")
    if notifier.get("telegram_chat_id"): fallback.add("telegram")
    return fallback


def _notify_admins(event, subject, body):
    """Send varsel til alle admin-mottakere som har valgt `event`. Hver mottaker
    kan styre per varseltype hvilke kanaler som fyrer (event_channels).
    Bakoverkompat: hvis event_channels ikke er satt, brukes alle konfigurerte
    kanaler (e-post + SMS + push + telegram)."""
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
                                "ntfy": n.get("ntfy_topic"),
                                "telegram": n.get("telegram_chat_id")} for n in matching],
            }, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[ADMIN-NOTIFY] Logg-feil: {e}")

    sms_text = _short_sms_for(event, subject, body)
    mail_sent = sms_sent = push_sent = tg_sent = 0
    mail_failed = sms_failed = push_failed = tg_failed = 0
    for n in matching:
        allowed = _event_channels_for(n, event)
        email = (n.get("email") or "").strip()
        phone = (n.get("phone") or "").strip()
        ntfy  = (n.get("ntfy_topic") or "").strip()
        tg    = (n.get("telegram_chat_id") or "").strip()
        if email and "email" in allowed:
            ok, detail = _send_admin_mail(email, subject, body)
            if ok: mail_sent += 1
            else:
                mail_failed += 1
                print(f"[ADMIN-NOTIFY] mail {email}: {detail}")
        if phone and "sms" in allowed:
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
        if ntfy and "push" in allowed:
            ok, detail = _send_admin_push(ntfy, subject, body)
            if ok: push_sent += 1
            else:
                push_failed += 1
                print(f"[ADMIN-NOTIFY] push {ntfy}: {detail}")
        if tg and "telegram" in allowed:
            ok, detail = _send_admin_telegram(tg, subject, body)
            if ok: tg_sent += 1
            else:
                tg_failed += 1
                print(f"[ADMIN-NOTIFY] telegram {tg}: {detail}")
    print(f"[ADMIN-NOTIFY] {event}: "
          f"mail={mail_sent}/{mail_sent+mail_failed}, "
          f"sms={sms_sent}/{sms_sent+sms_failed}, "
          f"push={push_sent}/{push_sent+push_failed}, "
          f"telegram={tg_sent}/{tg_sent+tg_failed}")


def _format_order_lines(order):
    """Tekstoppsummering av en ordre fra ny.havoyet.no/kasse."""
    nr = order.get("ordrenr") or order.get("name") or order.get("id") or "?"
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
        adr  = ""
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
        tg_raw    = (data.get("telegram_chat_id") or "").strip()
        name  = (data.get("name") or "").strip()
        events = data.get("events") or list(ADMIN_EVENTS)
        # Minst én kanal må være fylt ut
        if not email and not phone_raw and not ntfy_raw and not tg_raw:
            return jsonify({"error": "Du må fylle inn e-post, telefon, ntfy-topic eller Telegram chat-ID"}), 400
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
        tg_chat = ""
        if tg_raw:
            tg_norm = _normalize_telegram_chat_id(tg_raw)
            if not tg_norm:
                return jsonify({"error": "Ugyldig Telegram chat-ID (tall som 123456789 eller @kanalnavn)"}), 400
            tg_chat = tg_norm
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
            if tg_chat and n.get("telegram_chat_id", "") == tg_chat:
                return jsonify({"error": "Telegram chat-ID er allerede registrert"}), 409
        # Per-event-kanal-mapping. Hvis ikke angitt, defaulter dispatcheren
        # til alle konfigurerte kanaler (bakoverkompat).
        event_channels = {}
        raw_ec = data.get("event_channels") or {}
        if isinstance(raw_ec, dict):
            for k, v in raw_ec.items():
                if k not in ADMIN_EVENTS or not isinstance(v, list):
                    continue
                event_channels[k] = [c for c in v if c in VALID_CHANNELS]
        new = {
            "id": str(_uuid.uuid4()),
            "name": name,
            "email": email,
            "phone": phone,
            "ntfy_topic": ntfy,
            "telegram_chat_id": tg_chat,
            "events": events,
            "event_channels": event_channels,
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
            if "event_channels" in data:
                raw_ec = data.get("event_channels") or {}
                if isinstance(raw_ec, dict):
                    cleaned = {}
                    for k, v in raw_ec.items():
                        if k not in ADMIN_EVENTS or not isinstance(v, list):
                            continue
                        cleaned[k] = [c for c in v if c in VALID_CHANNELS]
                    n["event_channels"] = cleaned
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
            if "telegram_chat_id" in data:
                tg_raw = (data.get("telegram_chat_id") or "").strip()
                if tg_raw == "":
                    n["telegram_chat_id"] = ""
                else:
                    tg_norm = _normalize_telegram_chat_id(tg_raw)
                    if not tg_norm:
                        return jsonify({"error": "Ugyldig Telegram chat-ID"}), 400
                    n["telegram_chat_id"] = tg_norm
            # Sikkerhetssjekk: minst én kanal må gjenstå
            if not (n.get("email") or n.get("phone") or n.get("ntfy_topic") or n.get("telegram_chat_id")):
                return jsonify({"error": "Mottakeren må ha minst én kanal (e-post, telefon, ntfy eller Telegram)"}), 400
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
    tg_sent = tg_failed = 0
    errors = []  # liste over feildetaljer per kanal/mottaker for diagnostikk
    for n in targets:
        email = (n.get("email") or "").strip()
        phone = (n.get("phone") or "").strip()
        ntfy  = (n.get("ntfy_topic") or "").strip()
        tg    = (n.get("telegram_chat_id") or "").strip()
        if email:
            ok, detail = _send_admin_mail(
                email,
                "[Havøyet] Testvarsel fra admin",
                f"Dette er en test sendt {ts}.\n\n"
                f"Hvis du mottok denne e-posten er admin-varsler korrekt satt opp for {email}.",
            )
            if ok: mail_sent += 1
            else:
                mail_failed += 1
                errors.append({"channel": "email", "to": email, "detail": detail})
        if phone:
            norm = _normalize_phone(phone)
            if not norm:
                sms_failed += 1
                errors.append({"channel": "sms", "to": phone, "detail": "ugyldig telefonformat"})
            else:
                ok, detail = _send_admin_sms(norm, f"Havøyet: testvarsel {ts}")
                if ok: sms_sent += 1
                else:
                    sms_failed += 1
                    errors.append({"channel": "sms", "to": norm, "detail": detail})
        if ntfy:
            ok, detail = _send_admin_push(
                ntfy,
                "[Havøyet] Testvarsel",
                f"Push-varsel sendt {ts}.\n\nNår du ser dette på telefonen, fungerer admin-varsler.",
            )
            if ok: push_sent += 1
            else:
                push_failed += 1
                errors.append({"channel": "push", "to": ntfy, "detail": detail})
        if tg:
            ok, detail = _send_admin_telegram(
                tg,
                "[Havøyet] Testvarsel",
                f"Telegram-varsel sendt {ts}.\n\nNår du ser dette i Telegram, fungerer admin-varsler.",
            )
            if ok: tg_sent += 1
            else:
                tg_failed += 1
                errors.append({"channel": "telegram", "to": tg, "detail": detail})
    return jsonify({
        "ok": True,
        "mail_sent": mail_sent, "mail_failed": mail_failed,
        "sms_sent":  sms_sent,  "sms_failed":  sms_failed,
        "push_sent": push_sent, "push_failed": push_failed,
        "telegram_sent": tg_sent, "telegram_failed": tg_failed,
        "errors": errors,
        # Bakoverkompatibilitet
        "sent": mail_sent + sms_sent + push_sent + tg_sent,
        "failed": mail_failed + sms_failed + push_failed + tg_failed,
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
        "telegram": {
            "bot":       bool(TELEGRAM_BOT_TOKEN),
            "available": bool(TELEGRAM_BOT_TOKEN),
        },
    })


_KV_VALID_CHANNELS = ("email", "sms", "both")

# E-post-bilder lastet opp fra Kunde-varsler-editoren. Lagres på persistent disk
# og serveres som offentlige URL-er fra Flask-instansen — dvs. URL-er som
# e-postklienter (Gmail/Outlook) kan hente og embedde.
EMAIL_IMAGE_DIR = os.path.join(STATE_DIR, "email-images")
try:
    os.makedirs(EMAIL_IMAGE_DIR, exist_ok=True)
except Exception:
    pass

_ALLOWED_IMAGE_EXTS = {
    "image/jpeg": "jpg", "image/jpg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
    "image/gif": "gif",
}


@app.route("/email-images/<path:filename>")
def serve_email_image(filename):
    """Server opplastede e-post-bilder fra persistent disk."""
    # Forhindre directory-traversal — Flask normaliserer "/path", men ekstra
    # forsvarslag her skader ikke.
    if "/" in filename or filename.startswith(".."):
        return jsonify({"error": "Ugyldig filnavn"}), 400
    return send_from_directory(EMAIL_IMAGE_DIR, filename)


@app.route("/api/admin/upload-email-image", methods=["POST"])
def api_admin_upload_email_image():
    """Last opp et bilde som kan brukes i kunde-mail-maler.

    Aksepterer multipart/form-data med felt `file`. Returnerer en offentlig
    URL som admin-UI setter inn som `[IMG:url]`-placeholder i mal-body-en.
    """
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "Mangler fil (felt 'file')"}), 400
    mime = (f.mimetype or "").lower()
    ext = _ALLOWED_IMAGE_EXTS.get(mime)
    if not ext:
        return jsonify({
            "error": f"Ugyldig filtype ({mime}). Tillatt: JPG, PNG, WebP, GIF."
        }), 400
    # Generer et tilfeldig, kollisjonsfritt filnavn
    fname = f"{secrets.token_urlsafe(12).replace('-','_')}.{ext}"
    target = os.path.join(EMAIL_IMAGE_DIR, fname)
    try:
        f.save(target)
    except Exception as e:
        return jsonify({"error": f"Lagring feilet: {e}"}), 500
    # Bygg offentlig URL — bruk request.host_url så det fungerer både lokalt
    # og bak Cloudflare/Render. Strip trailing slash.
    base = (request.host_url or "").rstrip("/")
    public_url = f"{base}/email-images/{fname}"
    return jsonify({
        "ok": True,
        "filename": fname,
        "url": public_url,
        "placeholder": f"[IMG:{public_url}]",
    })


@app.route("/api/admin/customer-notify-config", methods=["GET", "PUT"])
def api_admin_customer_notify_config():
    """Hent og oppdater Kunde-varsler-konfigurasjonen.

    Lagres som del av sync-state slik at den overlever Render-restarter (gitt
    persistent disk på /var/data). Defaults i `_CUSTOMER_NOTIFY_DEFAULTS`
    brukes for ukjente felt — så nye nøkler får fornuftige verdier ved
    oppgraderinger.
    """
    global _customer_notify_config
    if request.method == "GET":
        return jsonify(_customer_notify_config)

    data = request.get_json(force=True, silent=True) or {}
    if not isinstance(data, dict):
        return jsonify({"error": "Forventer JSON-objekt"}), 400

    merged = {k: dict(v) for k, v in _CUSTOMER_NOTIFY_DEFAULTS.items()}
    for key, defaults in _CUSTOMER_NOTIFY_DEFAULTS.items():
        incoming = data.get(key)
        if not isinstance(incoming, dict):
            # Ingen oppdatering — behold lagret verdi (fall tilbake til default)
            merged[key].update(_customer_notify_config.get(key, {}))
            continue
        # Behold tidligere lagrede felt som ikke ble sendt inn på nytt
        merged[key].update(_customer_notify_config.get(key, {}))
        # Valider og overskriv per felt
        if "enabled" in incoming:
            merged[key]["enabled"] = bool(incoming["enabled"])
        if "channel" in incoming and incoming["channel"] in _KV_VALID_CHANNELS:
            merged[key]["channel"] = incoming["channel"]
        if "subject" in incoming and isinstance(incoming["subject"], str):
            merged[key]["subject"] = incoming["subject"][:300].strip()
        if "body" in incoming and isinstance(incoming["body"], str):
            merged[key]["body"] = incoming["body"][:4000]
        if "hours_before" in incoming and "hours_before" in defaults:
            try:
                hb = float(incoming["hours_before"])
                if 0 < hb <= 168:  # max 1 uke
                    merged[key]["hours_before"] = hb
            except (TypeError, ValueError):
                pass

    _customer_notify_config = merged
    _save_sync_state()
    return jsonify({"ok": True, "config": _customer_notify_config})


def _kv_render(template, **vars):
    """Sikker .format-erstatning som ikke krasjer på ukjente {placeholders}."""
    if not template:
        return ""
    out = template
    for k, v in vars.items():
        out = out.replace("{" + k + "}", str(v) if v is not None else "")
    return out


@app.route("/api/admin/telegram/setup", methods=["GET"])
def api_admin_telegram_setup():
    """Hjelp-endepunkt for å finne chat-IDen din.

    1. Bruker oppretter bot via @BotFather og setter TELEGRAM_BOT_TOKEN.
    2. Bruker åpner Telegram, søker opp boten og trykker "Start" / sender
       en melding (f.eks. "hei").
    3. Bruker åpner admin-UI som kaller dette endepunktet — vi spør
       Telegram getUpdates og returnerer alle chat-IDer som har snakket
       med boten siden sist.
    """
    if not TELEGRAM_BOT_TOKEN:
        return jsonify({"ok": False, "error": "TELEGRAM_BOT_TOKEN er ikke satt på serveren."}), 400
    try:
        r = requests.get(
            f"{TELEGRAM_API}/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
            params={"timeout": 0, "limit": 50},
            timeout=10,
        )
        if not (200 <= r.status_code < 300):
            return jsonify({"ok": False, "error": f"Telegram API svarte {r.status_code}: {r.text[:200]}"}), 502
        data = r.json()
        if not data.get("ok"):
            return jsonify({"ok": False, "error": data.get("description", "Ukjent Telegram-feil")}), 502
        # Trekk ut unike chats fra siste meldinger
        chats = {}
        for upd in data.get("result", []):
            msg = upd.get("message") or upd.get("edited_message") or {}
            chat = msg.get("chat") or {}
            cid = chat.get("id")
            if cid is None:
                continue
            key = str(cid)
            if key not in chats:
                first = (chat.get("first_name") or "").strip()
                last  = (chat.get("last_name") or "").strip()
                user  = (chat.get("username") or "").strip()
                title = (chat.get("title") or "").strip()
                full = title or " ".join(p for p in (first, last) if p) or (f"@{user}" if user else "")
                chats[key] = {
                    "chat_id": key,
                    "name":    full or "(ukjent)",
                    "type":    chat.get("type", ""),
                    "username": user,
                    "last_text": (msg.get("text") or "")[:80],
                }
        return jsonify({
            "ok": True,
            "chats": list(chats.values()),
            "hint": "Send en melding til boten på Telegram, så dukker chat-IDen opp her. "
                    "Trykk på den for å fylle ut feltet automatisk.",
        })
    except Exception as e:
        return jsonify({"ok": False, "error": f"Kunne ikke hente updates: {e}"}), 502


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

    # Krev gyldig ISO-leveringsdato (YYYY-MM-DD) og leveringstid. Eldre flyter sendte
    # ukedag-navn ("torsdag") som ikke kan plottes på en kalender — det blokkeres her
    # så bestillingssiden får riktig dato på alle nye ordre.
    lev_dag = (kunde.get("leveringsdag") or "").strip()
    lev_tid = (kunde.get("leveringstid") or "").strip()
    import re as _re
    if not _re.match(r"^\d{4}-\d{2}-\d{2}$", lev_dag):
        return jsonify({"ok": False, "error": "Velg en konkret leveringsdato før du fullfører bestillingen"}), 400
    if not lev_tid:
        return jsonify({"ok": False, "error": "Velg leveringstid før du fullfører bestillingen"}), 400

    # Sørg for at ordrenummer finnes
    if not data.get("ordrenr"):
        data["ordrenr"] = "H" + _uuid.uuid4().hex[:6].upper()
    if not data.get("dato"):
        data["dato"] = datetime.now().strftime("%Y-%m-%d")
    if not data.get("status"):
        data["status"] = "NEW"

    # Idempotent: hvis samme ordrenr er postet før (f.eks. AWAITING_PAYMENT
    # pre-lagring fulgt av PAID-oppdatering), slå sammen i stedet for å
    # opprette duplikat. Eksisterende felter bevares unntatt der nye verdier
    # er ikke-tomme — slik at status-flyt AWAITING_PAYMENT → PAID skrives
    # over riktig vei.
    target_id = str(data["ordrenr"])
    existing_idx = None
    for i, o in enumerate(_manual_orders):
        if str(o.get("ordrenr") or o.get("id") or "").strip() == target_id:
            existing_idx = i
            break
    if existing_idx is not None:
        merged = dict(_manual_orders[existing_idx])
        for k, v in data.items():
            # Tillat status å oppdateres fra AWAITING_PAYMENT → PAID/NEW etc.
            # Ikke skriv over med tom/None.
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            merged[k] = v
        # Ikke send admin-varsel på re-POST av samme ordre — det er bare
        # en status-oppdatering, ikke en ny bestilling.
        _manual_orders[existing_idx] = merged
        _save_sync_state()
        return jsonify({"ok": True, "ordrenr": target_id, "updated": True})

    # Ny ordre — legg til i state
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

    # Send admin-varsel (e-post / SMS / ntfy / Telegram) til registrerte mottakere.
    # Dette er separat fra kvitterings-mailen som går til kunden via _send_contact_mail
    # og sikrer at f.eks. Telegram-varsel går ut umiddelbart når bestillingen kommer inn.
    try:
        _notify_admins(
            "new_order",
            f"[Havøyet] Ny bestilling #{data['ordrenr']} — {navn} ({data.get('sum', 0)} kr)",
            "\n".join(lines),
        )
    except Exception as e:
        print(f"[ADMIN-NOTIFY] new_order varsel feilet: {e}")

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
            change_summary = f"Status endret fra '{old_status}' til '{new_status}'."
            is_delivered = (
                str(new_status).upper() in ("DONE", "LEVERT")
                or "lever" in str(new_status).lower()
            )
            if is_delivered:
                _notify_admins(
                    "order_delivered",
                    f"[Havøyet] Bestilling #{nr} er levert",
                    change_summary + "\n" + "=" * 54 + "\n\n" + _format_order_lines(o),
                )
                _notify_customer_order_update(o, "order_delivered", change_summary)
            elif old_status != new_status:
                _notify_admins(
                    "order_updated",
                    f"[Havøyet] Bestilling #{nr} oppdatert",
                    change_summary + "\n" + "=" * 54 + "\n\n" + _format_order_lines(o),
                )
                _notify_customer_order_update(o, "order_updated", change_summary)
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
            change_summary = (
                f"Status endret fra '{old_status}' til '{new_status}'."
                if status_changed else "Bestillingen ble oppdatert i admin."
            )
            # Varsler kjøres best-effort: en feil i SMTP/Twilio/Telegram skal
            # IKKE føre til at lagringen returnerer 5xx — ordren er allerede
            # persistert via _save_sync_state() over.
            try:
                if became_delivered:
                    _notify_admins(
                        "order_delivered",
                        f"[Havøyet] Bestilling #{nr} er levert",
                        change_summary + "\n" + "=" * 54 + "\n\n" + _format_order_lines(o),
                    )
                    _notify_customer_order_update(o, "order_delivered", change_summary)
                else:
                    _notify_admins(
                        "order_updated",
                        f"[Havøyet] Bestilling #{nr} oppdatert",
                        change_summary + "\n" + "=" * 54 + "\n\n" + _format_order_lines(o),
                    )
                    _notify_customer_order_update(o, "order_updated", change_summary)
            except Exception as _notify_err:
                print(f"[PATCH-NOTIFY] Varsel feilet (ordren ble lagret OK): {_notify_err}")
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

# ─── Vipps Bedrift CSV/PDF-import (drag-drop fra Økonomi-fanen) ────────────
import csv as _csv
import io as _io
import re as _re
import hashlib as _hashlib

# Mulige kolonnenavn fra portal.vipps.no — vi tester i prioritert rekkefølge
_VIPPS_CSV_FIELDS = {
    "transaction_id": ["Transaksjons-ID", "Transaction ID", "TransactionId", "Reference", "Referanse", "ID"],
    "date":           ["Dato", "Date", "Salgsdato", "Booking date", "Bokført dato"],
    "time":           ["Tidspunkt", "Time", "Klokkeslett"],
    "amount":         ["Beløp", "Amount", "Sum", "Total"],
    "type":           ["Type", "Transaksjonstype", "Transaction type"],
    "description":    ["Beskrivelse", "Description", "Notat", "Note", "Melding"],
    "phone":          ["Telefon", "Phone", "Telefonnummer", "Customer phone"],
    "name":           ["Navn", "Name", "Kunde", "Customer"],
}

def _csv_get(row, key):
    """Hent felt fra CSV-rad ved å prøve alle kjente kolonnenavn."""
    for col in _VIPPS_CSV_FIELDS.get(key, []):
        if col in row and row[col] is not None and str(row[col]).strip():
            return str(row[col]).strip()
    return ""

def _parse_amount_ore(raw):
    """Konverter '1 234,50' / '1234.50' / '1234,50 kr' → øre (int)."""
    if not raw:
        return 0
    s = str(raw).strip().replace("kr", "").replace("NOK", "").replace(" ", "").replace(" ", "")
    s = s.replace(",", ".")
    try:
        return int(round(float(s) * 100))
    except (ValueError, TypeError):
        return 0


def _parse_vipps_pdf(pdf_bytes):
    """Parse Vipps Bedriftsportal-PDF til list av transaksjons-dicts.
    PDF-en har ikke transaksjons-ID-er, så vi bygger en stabil synthetic ID
    fra hash av (dato+tid+beløp+melding) — det gjør re-import av samme PDF trygt.

    Format eksempel (utdrag fra portal.vippsmobilepay.com):
        27.04.2026,
        17:10
        Havøyet AS         Vipps        Belastet     -44,33    1 740,00
                           betaling hos
                           Havøyet AS
    """
    try:
        import pypdf
    except ImportError:
        raise RuntimeError("pypdf ikke installert (kjør: pip install pypdf)")

    reader = pypdf.PdfReader(_io.BytesIO(pdf_bytes))
    full_text = "\n".join(p.extract_text() or "" for p in reader.pages)

    # Splitt på datolinjer: "DD.MM.YYYY,"
    date_pat = _re.compile(r"\b(\d{2}\.\d{2}\.\d{4}),?\s*\n?\s*(\d{1,2}:\d{2})", _re.MULTILINE)

    # Finn alle dato-anker først, slå sammen tekst mellom dem til én transaksjon
    matches = list(date_pat.finditer(full_text))
    transactions = []
    for i, m in enumerate(matches):
        date_str = m.group(1)
        time_str = m.group(2)
        block_start = m.end()

        # Hopp over side-footere: "DD.MM.YYYY, HH:MM Transaksjoner | Bedriftsportalen"
        # er PDF-ens egen genereringstidspunkt, ikke en transaksjon.
        next_chunk = full_text[block_start:block_start+80]
        if "Bedriftsportalen" in next_chunk or "vippsmobilepay" in next_chunk or "Transaksjoner |" in next_chunk:
            continue

        block_end = matches[i+1].start() if i+1 < len(matches) else len(full_text)
        block = full_text[block_start:block_end]

        # Status: "Belastet" = paid/captured, "Refundert" = refund
        status = "Belastet"
        if _re.search(r"\bRefundert\b", block, _re.IGNORECASE):
            status = "Refundert"
        elif _re.search(r"\bBelastet\b", block, _re.IGNORECASE):
            status = "Belastet"
        elif _re.search(r"\bAvbrutt\b", block, _re.IGNORECASE):
            status = "Avbrutt"

        # Beløp: alle tallgrupper "1 234,56" eller "1234,56" — siste er typisk total
        amounts = _re.findall(r"-?\d{1,3}(?:\s\d{3})*(?:,\d{2})?(?!\d)", block)
        amounts = [a for a in amounts if "," in a or len(a) >= 3]
        amount_str = amounts[-1] if amounts else "0"
        fee_str = amounts[-2] if len(amounts) >= 2 else ""

        # Navn + masked telefon: f.eks. "Arvid\nMellingen\n+47****9707"
        # Navnet kan strekke seg over flere linjer i Vipps PDF.
        name = ""
        phone_masked = ""
        nm = _re.search(r"((?:[A-ZÆØÅ][A-Za-zÆØÅæøå\-]+\s*\n?\s*){1,4})(\+47\*+\d{4})", block)
        if nm:
            name = _re.sub(r"\s+", " ", nm.group(1)).strip()
            phone_masked = nm.group(2)

        # Melding: tekst etter "Havøyet AS" på samme/neste linje, før status
        # Forenklet: ta linjer som ikke er navn/status/beløp
        msg_match = _re.search(r"Havøyet AS\s+(.+?)(?:\s+(?:Belastet|Refundert|Avbrutt))", block, _re.DOTALL)
        description = ""
        if msg_match:
            description = _re.sub(r"\s+", " ", msg_match.group(1)).strip()
            if description.startswith("Vipps"):
                description = "Vipps-betaling"

        # Synthetic ID: hash av (dato + tid + beløp + beskrivelse + navn)
        seed = f"{date_str}|{time_str}|{amount_str}|{description}|{name}"
        synth_id = "vipps-pdf-" + _hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]

        # Konverter dato DD.MM.YYYY → YYYY-MM-DD
        try:
            dd, mm, yyyy = date_str.split(".")
            date_iso = f"{yyyy}-{mm}-{dd}"
        except Exception:
            date_iso = date_str

        amount_ore = _parse_amount_ore(amount_str)
        # Skille mellom direkte Vipps (kunde sender til oss via Vipps-app) og
        # ePayment fra nettsiden. Heuristikk: hvis raden har navn+telefon er
        # det direkte; ellers er det "Vipps-betaling hos Havøyet AS" fra epay.
        payment_channel = "direct" if (name and phone_masked) else "website"
        transactions.append({
            "transaction_id":  synth_id,
            "date":            date_iso,
            "time":            time_str,
            "amount_ore":      amount_ore,
            "amount_kr":       amount_ore / 100.0,
            "type":            "Kjøp" if status == "Belastet" else status,
            "status":          status,
            "description":     description,
            "phone":           phone_masked,
            "name":            name,
            "fee_kr":          _parse_amount_ore(fee_str) / 100.0 if fee_str else 0.0,
            "payment_channel": payment_channel,  # "direct" eller "website"
            "imported_at":     datetime.now().isoformat(),
            "source":          "vipps_pdf",
        })
    return transactions


@app.route("/api/vipps/import-csv", methods=["POST"])
def api_vipps_import_csv():
    """Tar imot Vipps Bedrift CSV- eller PDF-eksport.
    Dedupliserer mot _vipps_imported_payments på transaksjons-ID slik at
    re-import av overlappende periode er trygt.

    CSV: dedup på Vipps' egen Transaksjons-ID-kolonne.
    PDF: dedup på synthetic ID (hash av dato+tid+beløp+melding+navn) siden
         Vipps Bedriftsportal-PDF ikke har transaksjons-ID-er.

    Body: multipart/form-data med felt 'file'."""
    global _vipps_imported_payments

    if not (request.files and "file" in request.files):
        return jsonify({"error": "Ingen fil i 'file'-feltet"}), 400

    file = request.files["file"]
    filename = (file.filename or "").lower()
    raw = file.read()

    if not raw:
        return jsonify({"error": "Tom fil"}), 400

    is_pdf = filename.endswith(".pdf") or raw[:4] == b"%PDF"

    parsed_records = []
    if is_pdf:
        try:
            parsed_records = _parse_vipps_pdf(raw)
        except RuntimeError as e:
            return jsonify({"error": str(e)}), 503
        except Exception as e:
            return jsonify({"error": f"Kan ikke lese PDF: {e}"}), 400
        if not parsed_records:
            return jsonify({"error": "Fant ingen transaksjoner i PDF — er dette en Vipps Bedriftsportal-eksport?"}), 400
    else:
        # CSV-flyt
        try:
            csv_text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            csv_text = raw.decode("latin-1", errors="replace")

        sample = csv_text[:2048]
        try:
            dialect = _csv.Sniffer().sniff(sample, delimiters=";,\t")
        except _csv.Error:
            class _D: delimiter = ";"
            dialect = _D()

        reader = _csv.DictReader(_io.StringIO(csv_text), delimiter=dialect.delimiter)
        rows = list(reader)
        if not rows:
            return jsonify({"error": "Ingen rader i CSV"}), 400

        for row in rows:
            tx_id = _csv_get(row, "transaction_id")
            if not tx_id:
                continue
            amt_raw = _csv_get(row, "amount")
            amount_ore = _parse_amount_ore(amt_raw)
            parsed_records.append({
                "transaction_id": tx_id,
                "date":           _csv_get(row, "date"),
                "time":           _csv_get(row, "time"),
                "amount_ore":     amount_ore,
                "amount_kr":      amount_ore / 100.0,
                "type":           _csv_get(row, "type") or "Kjøp",
                "description":    _csv_get(row, "description"),
                "phone":          _csv_get(row, "phone"),
                "name":           _csv_get(row, "name"),
                "imported_at":    datetime.now().isoformat(),
                "source":         "vipps_csv",
            })

    # Dedup + lagre
    added, dup, skipped, total_ore = 0, 0, 0, 0
    new_records = []
    for rec in parsed_records:
        tx_id = rec.get("transaction_id")
        if not tx_id:
            skipped += 1
            continue
        if tx_id in _vipps_imported_payments:
            dup += 1
            continue
        _vipps_imported_payments[tx_id] = rec
        new_records.append(rec)
        if rec.get("amount_ore", 0) > 0:
            total_ore += rec["amount_ore"]
        added += 1

    if added:
        _save_sync_state()
        # Skriv også oppdatert snapshot til data/vipps_baseline.json — denne
        # blir lest ved Render-restart hvis /tmp er wipet. Gir permanent
        # lagring uten persistent disk, så lenge fila er sjekket inn i git.
        try:
            _baseline_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                          "data", "vipps_baseline.json")
            os.makedirs(os.path.dirname(_baseline_path), exist_ok=True)
            with open(_baseline_path, "w", encoding="utf-8") as bf:
                json.dump(_vipps_imported_payments, bf, ensure_ascii=False, indent=2)
            _commit_baseline_to_github(_baseline_path,
                                       f"Vipps-baseline: auto-update etter import (+{added} nye)")
        except Exception as e:
            print(f"[BASELINE] Kunne ikke lagre baseline: {e}")

    return jsonify({
        "ok": True,
        "format":     "pdf" if is_pdf else "csv",
        "added":      added,
        "duplicates": dup,
        "skipped":    skipped,
        "total_rows": len(parsed_records),
        "total_amount_kr": total_ore / 100.0,
        "new":        new_records[:50],
    })


def _commit_baseline_to_github(file_path, message):
    """Commit oppdatert baseline-fil til GitHub via REST API.
    Krever GITHUB_TOKEN env var med 'repo'-scope. Gjør INGENTING hvis token mangler.
    Aktiveres ved å sette GITHUB_TOKEN på Render — én engangs-oppsett."""
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    repo  = os.environ.get("GITHUB_REPO", "erikoye/havoyet-bestilling").strip()
    branch = os.environ.get("GITHUB_BRANCH", "main").strip()
    if not token:
        return  # auto-commit deaktivert — bruker må sette GITHUB_TOKEN
    rel_path = "data/" + os.path.basename(file_path)
    api = f"https://api.github.com/repos/{repo}/contents/{rel_path}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "havoyet-flask",
    }
    try:
        # Hent nåværende SHA (kreves for update)
        sha = None
        r = requests.get(api, headers=headers, params={"ref": branch}, timeout=15)
        if r.status_code == 200:
            sha = r.json().get("sha")
        with open(file_path, "rb") as f:
            content_b64 = _base64.b64encode(f.read()).decode("ascii")
        payload = {
            "message": message,
            "content": content_b64,
            "branch":  branch,
        }
        if sha:
            payload["sha"] = sha
        r = requests.put(api, headers=headers, json=payload, timeout=20)
        if r.status_code in (200, 201):
            print(f"[BASELINE] ✓ Auto-committed {rel_path} til GitHub")
        else:
            print(f"[BASELINE] GitHub API svarte {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[BASELINE] GitHub auto-commit feilet: {e}")


# ─── Shopify Payments / kortbetaling CSV-import ────────────────────────────
@app.route("/api/card-payments/import-csv", methods=["POST"])
def api_card_payments_import_csv():
    """Import Shopify Payments CSV-eksport (payment_transactions_export*.csv).
    Format-kolonner: Transaction Date, Type, Order, Card Brand, Amount, Fee, Net.
    Dedup på synthetic ID (hash av dato+order+amount+type)."""
    global _card_payments_imported
    if not (request.files and "file" in request.files):
        return jsonify({"error": "Ingen fil i 'file'-feltet"}), 400
    raw = request.files["file"].read()
    if not raw:
        return jsonify({"error": "Tom fil"}), 400
    try:
        csv_text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        csv_text = raw.decode("latin-1", errors="replace")

    sample = csv_text[:2048]
    try:
        dialect = _csv.Sniffer().sniff(sample, delimiters=",;\t")
    except _csv.Error:
        class _D: delimiter = ","
        dialect = _D()
    reader = _csv.DictReader(_io.StringIO(csv_text), delimiter=dialect.delimiter)
    rows = list(reader)
    if not rows:
        return jsonify({"error": "Ingen rader i CSV"}), 400

    added, dup, skipped = 0, 0, 0
    new_records = []
    for row in rows:
        date_raw = (row.get("Transaction Date") or row.get("Date") or "").strip()
        order   = (row.get("Order") or "").strip()
        type_   = (row.get("Type") or "charge").strip().lower()
        brand   = (row.get("Card Brand") or "").strip()
        amount  = row.get("Amount") or "0"
        fee     = row.get("Fee") or "0"
        net     = row.get("Net") or "0"
        if not date_raw or not amount:
            skipped += 1
            continue
        # Konverter dato "2026-04-27 14:08:56 +0200" → ISO-dato
        date_iso = date_raw[:10]
        time_str = date_raw[11:16] if len(date_raw) >= 16 else ""
        try:
            amount_kr = float(str(amount).replace(",", "."))
            fee_kr    = float(str(fee).replace(",", "."))
            net_kr    = float(str(net).replace(",", "."))
        except (ValueError, TypeError):
            skipped += 1
            continue
        # Synthetic ID — stabil mellom imports
        seed = f"{date_raw}|{order}|{type_}|{amount}"
        synth_id = "card-" + _hashlib_mod.sha1(seed.encode("utf-8")).hexdigest()[:16]
        if synth_id in _card_payments_imported:
            dup += 1
            continue
        rec = {
            "transaction_id": synth_id,
            "date":           date_iso,
            "time":           time_str,
            "order":          order,
            "type":           "Refusjon" if type_ == "refund" else "Kjøp",
            "brand":          brand,
            "amount_ore":     int(round(amount_kr * 100)),
            "amount_kr":      amount_kr,
            "fee_kr":         fee_kr,
            "net_kr":         net_kr,
            "imported_at":    datetime.now().isoformat(),
            "source":         "shopify_card",
        }
        _card_payments_imported[synth_id] = rec
        new_records.append(rec)
        added += 1

    if added:
        _save_sync_state()
        try:
            _baseline_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                          "data", "card_payments_baseline.json")
            os.makedirs(os.path.dirname(_baseline_path), exist_ok=True)
            with open(_baseline_path, "w", encoding="utf-8") as bf:
                json.dump(_card_payments_imported, bf, ensure_ascii=False, indent=2)
            _commit_baseline_to_github(_baseline_path,
                                       f"Card-baseline: auto-update etter import (+{added} nye)")
        except Exception as e:
            print(f"[BASELINE] Kunne ikke lagre card-baseline: {e}")

    total_kr = sum(r["amount_kr"] for r in new_records if r["type"] == "Kjøp")
    return jsonify({
        "ok":               True,
        "added":            added,
        "duplicates":       dup,
        "skipped":          skipped,
        "total_rows":       len(rows),
        "total_amount_kr":  round(total_kr, 2),
        "new":              new_records[:50],
    })


@app.route("/api/card-payments/imported")
def api_card_payments_imported():
    items = list(_card_payments_imported.values())
    items.sort(key=lambda r: (r.get("date") or "", r.get("time") or ""), reverse=True)
    gross = sum(r.get("amount_ore", 0) for r in items if r.get("type") == "Kjøp") / 100.0
    refund = sum(r.get("amount_ore", 0) for r in items if r.get("type") == "Refusjon") / 100.0
    return jsonify({
        "count":     len(items),
        "items":     items,
        "gross_kr":  round(gross, 2),
        "refund_kr": round(refund, 2),
        "net_kr":    round(gross - refund, 2),
    })


@app.route("/api/card-payments/imported/<tx_id>", methods=["DELETE"])
def api_card_payments_delete(tx_id):
    global _card_payments_imported
    if tx_id in _card_payments_imported:
        del _card_payments_imported[tx_id]
        _save_sync_state()
        return jsonify({"ok": True})
    return jsonify({"error": "Ikke funnet"}), 404


@app.route("/api/vipps/imported")
def api_vipps_imported():
    """Liste alle Vipps-betalinger importert fra CSV. Sortert nyest først."""
    items = list(_vipps_imported_payments.values())
    items.sort(key=lambda r: (r.get("date") or "", r.get("time") or ""), reverse=True)
    return jsonify({
        "count":  len(items),
        "items":  items,
        "total_amount_kr": sum(r.get("amount_ore", 0) for r in items) / 100.0,
    })


@app.route("/api/vipps/imported/<tx_id>", methods=["DELETE"])
def api_vipps_imported_delete(tx_id):
    global _vipps_imported_payments
    if tx_id in _vipps_imported_payments:
        del _vipps_imported_payments[tx_id]
        _save_sync_state()
        return jsonify({"ok": True})
    return jsonify({"error": "Ikke funnet"}), 404


# ─── Økonomi/statistikk-endepunkt ─────────────────────────────────────────
@app.route("/api/economy/stats")
def api_economy_stats():
    try:
        return _api_economy_stats_impl()
    except Exception as _ec_e:
        import traceback as _tb
        print(f"[economy/stats] ERROR: {_ec_e}\n{_tb.format_exc()}")
        return jsonify({"error": str(_ec_e), "totals": {}, "by_year": [], "period": {}}), 200


def _api_economy_stats_impl():
    """Aggregert statistikk for økonomi-fanen.
    Query-parametre:
      ?year=YYYY         filtrer til ett kalenderår (default: hittil i år)
      ?from=YYYY-MM-DD   custom periode-start (overstyrer year)
      ?to=YYYY-MM-DD     custom periode-slutt (default: i dag)

    Responsen inkluderer alltid:
      - totals: hittil-i-året (eller valgt periode)
      - by_year: per-år-aggregat for ALLE kilder (gir historikk så langt bak data finnes)
      - filter: hvilken periode som ble brukt
    """
    today = datetime.now().date()
    # this_week_kr er nå rullerende siste 7 dager (i dag minus 6 = totalt 7 inkl. i dag),
    # slik at kortet alltid viser et like langt vindu uavhengig av ukedag.
    start_week = today - timedelta(days=6)
    start_month = today.replace(day=1)
    start_year = today.replace(month=1, day=1)

    # Parse query-params for custom periode
    q_year = request.args.get("year")
    q_from = request.args.get("from")
    q_to   = request.args.get("to")
    period_from, period_to = start_year, today
    period_label = f"Hittil i {today.year}"
    # Hardgrense: ingen periode kan gå lenger tilbake enn januar 2025
    EARLIEST = date(2025, 1, 1)
    try:
        if q_from:
            period_from = datetime.strptime(q_from, "%Y-%m-%d").date()
            period_to   = datetime.strptime(q_to, "%Y-%m-%d").date() if q_to else today
            period_label = f"{period_from.strftime('%d.%m.%Y')} – {period_to.strftime('%d.%m.%Y')}"
        elif q_year:
            y = int(q_year)
            if y < 2025:
                y = 2025
            period_from = date(y, 1, 1)
            period_to   = date(y, 12, 31)
            if y == today.year:
                period_to = today
                period_label = f"Hittil i {y}"
            else:
                period_label = str(y)
    except (ValueError, TypeError):
        pass
    # Clamp uansett — selv om noe ber om eldre data, kuttes det her
    if period_from < EARLIEST:
        period_from = EARLIEST
        if q_from:
            period_label = f"01.01.2025 – {period_to.strftime('%d.%m.%Y')}"

    def _parse_date(date_str):
        if not date_str:
            return None
        s = str(date_str).strip()[:10]
        # Aksepter både YYYY-MM-DD (vår normaliserte form) og DD.MM.YYYY (ikke-normalisert PDF)
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None

    def _in_range(date_str, since):
        d = _parse_date(date_str)
        if d is None:
            return False
        # Inkluderer fremtidige datoer ikke (forhindrer at typo/feil-data blåser opp ukens-tall)
        return since <= d <= today

    # Nettside-ordre (kun betalte teller for omsetning, men "oppgjort" =
    # paid+free teller for innkjøpskost — gratis-ordre koster oss like mye)
    paid_set = _paid_ordrenrs()
    settled_set = _settled_ordrenrs()
    web_orders = [o for o in _manual_orders
                  if str(o.get("ordrenr") or o.get("id")) in paid_set]
    cost_orders = [o for o in _manual_orders
                   if str(o.get("ordrenr") or o.get("id")) in settled_set]
    # Bygg kost-map én gang for å unngå å iterere produktlisten per ordre
    _cost_map = _build_product_cost_map()
    def _order_total_kr(o):
        try:
            return float(o.get("sum") or o.get("total") or 0)
        except (TypeError, ValueError):
            return 0.0
    def _order_date(o):
        return o.get("dato") or o.get("created_at") or ""

    web_total       = sum(_order_total_kr(o) for o in web_orders)
    web_total_week  = sum(_order_total_kr(o) for o in web_orders if _in_range(_order_date(o), start_week))
    web_total_month = sum(_order_total_kr(o) for o in web_orders if _in_range(_order_date(o), start_month))
    web_total_year  = sum(_order_total_kr(o) for o in web_orders if _in_range(_order_date(o), start_year))

    # Vipps CSV/PDF-import — skille direkte (rader med navn+telefon) fra
    # nettside-ePayment (rader uten navn — "Vipps-betaling hos Havøyet AS")
    vipps_imported = list(_vipps_imported_payments.values())
    vipps_direct  = [r for r in vipps_imported if r.get("payment_channel") == "direct"]
    vipps_website = [r for r in vipps_imported if r.get("payment_channel") != "direct"]

    def _sum_kr(rows, since=None):
        if since is None:
            return sum((r.get("amount_ore") or 0) for r in rows) / 100.0
        return sum((r.get("amount_ore") or 0) for r in rows if _in_range(r.get("date"), since)) / 100.0

    vipps_total       = _sum_kr(vipps_imported)
    vipps_total_week  = _sum_kr(vipps_imported, start_week)
    vipps_total_month = _sum_kr(vipps_imported, start_month)
    vipps_total_year  = _sum_kr(vipps_imported, start_year)

    # Stripe ePayment
    stripe_paid = [p for p in _stripe_load_payments().values() if p.get("state") in _STRIPE_PAID_STATES]
    stripe_total = sum((p.get("amount") or 0) for p in stripe_paid) / 100.0

    # Vipps ePayment (fra _vipps_payments — vår egen API-flyt)
    vipps_epay = [p for p in _vipps_load_payments().values() if p.get("state") in _VIPPS_PAID_STATES]
    vipps_epay_total = sum((p.get("amount") or 0) for p in vipps_epay) / 100.0

    # Kortbetalinger fra Shopify Payments CSV-import (charge minus refund)
    card_imported = list(_card_payments_imported.values())
    def _card_signed_kr(r):
        v = (r.get("amount_ore") or 0) / 100.0
        return -v if r.get("type") == "Refusjon" else v
    def _card_sum(rows, since=None):
        if since is None:
            return sum(_card_signed_kr(r) for r in rows)
        return sum(_card_signed_kr(r) for r in rows if _in_range(r.get("date"), since))
    card_total       = _card_sum(card_imported)
    card_total_week  = _card_sum(card_imported, start_week)
    card_total_month = _card_sum(card_imported, start_month)
    card_total_year  = _card_sum(card_imported, start_year)

    # I dag — kun rader med dato == today
    web_total_today    = sum(_order_total_kr(o) for o in web_orders if _parse_date(_order_date(o)) == today)
    vipps_total_today  = sum((r.get("amount_ore") or 0) for r in vipps_imported if _parse_date(r.get("date")) == today) / 100.0
    card_total_today   = sum(_card_signed_kr(r) for r in card_imported if _parse_date(r.get("date")) == today)

    grand_total       = web_total + vipps_total + card_total
    grand_total_today = web_total_today + vipps_total_today + card_total_today
    grand_total_week  = web_total_week + vipps_total_week + card_total_week
    grand_total_month = web_total_month + vipps_total_month + card_total_month
    grand_total_year  = web_total_year + vipps_total_year + card_total_year

    # === PERIODE-FILTRERTE SUMMER (basert på from/to fra query-params) ===
    def _in_period(date_str):
        d = _parse_date(date_str)
        return d is not None and period_from <= d <= period_to

    web_period_rows   = [o for o in web_orders if _in_period(_order_date(o))]
    vipps_period_rows = [r for r in vipps_imported if _in_period(r.get("date"))]
    direct_period     = [r for r in vipps_period_rows if r.get("payment_channel") == "direct"]
    website_period    = [r for r in vipps_period_rows if r.get("payment_channel") != "direct"]

    card_period_rows = [r for r in card_imported if _in_period(r.get("date"))]
    # Refusjoner trekkes fra totalsum, men telles ikke som transaksjoner i snittet.
    card_period_charges = [r for r in card_period_rows if r.get("type") != "Refusjon"]
    period_web_kr   = sum(_order_total_kr(o) for o in web_period_rows)
    period_vipps_kr = sum((r.get("amount_ore") or 0) for r in vipps_period_rows) / 100.0
    period_card_kr  = sum(_card_signed_kr(r) for r in card_period_rows)
    period_total_kr = period_web_kr + period_vipps_kr + period_card_kr

    # === INNKJØPSKOST ===
    # Telles på alle "oppgjorte" ordrer (paid + free), uavhengig av kilde-rad
    # i Vipps/kort-CSV (de har ingen item-data å regne på). Hvis kunden har
    # paid en ordre med ID som matches via paid_set, er den allerede med.
    period_cost_rows = [o for o in cost_orders if _in_period(_order_date(o))]
    period_cost_kr = sum(_order_cost_kr(o, _cost_map) for o in period_cost_rows)
    cost_total_year  = sum(_order_cost_kr(o, _cost_map) for o in cost_orders if _in_range(_order_date(o), start_year))
    cost_total_month = sum(_order_cost_kr(o, _cost_map) for o in cost_orders if _in_range(_order_date(o), start_month))
    cost_total_week  = sum(_order_cost_kr(o, _cost_map) for o in cost_orders if _in_range(_order_date(o), start_week))
    cost_total_today = sum(_order_cost_kr(o, _cost_map) for o in cost_orders if _parse_date(_order_date(o)) == today)
    # Snitt-ordresum for valgt periode
    period_charge_count = len(web_period_rows) + len(vipps_period_rows) + len(card_period_charges)
    period_avg_kr = (period_total_kr / period_charge_count) if period_charge_count > 0 else 0.0

    # === ÅR-OVERSIKT (alle år hvor vi har data) ===
    by_year = {}
    for o in web_orders:
        d = _parse_date(_order_date(o))
        if d:
            by_year.setdefault(d.year, {"web_kr": 0.0, "vipps_kr": 0.0, "card_kr": 0.0, "count": 0})
            by_year[d.year].setdefault("card_kr", 0.0)
            by_year[d.year]["web_kr"]   += _order_total_kr(o)
            by_year[d.year]["count"]    += 1
    for r in vipps_imported:
        d = _parse_date(r.get("date"))
        if d:
            by_year.setdefault(d.year, {"web_kr": 0.0, "vipps_kr": 0.0, "card_kr": 0.0, "count": 0})
            by_year[d.year].setdefault("card_kr", 0.0)
            by_year[d.year]["vipps_kr"] += (r.get("amount_ore") or 0) / 100.0
            by_year[d.year]["count"]    += 1
    for r in card_imported:
        d = _parse_date(r.get("date"))
        if d:
            by_year.setdefault(d.year, {"web_kr": 0.0, "vipps_kr": 0.0, "card_kr": 0.0, "count": 0})
            by_year[d.year].setdefault("card_kr", 0.0)
            by_year[d.year]["card_kr"] += _card_signed_kr(r)
            by_year[d.year]["count"]   += 1
    # Filtrer bort år før 2025 (ingen data eldre enn januar 2025)
    years_list = sorted([y for y in by_year.keys() if y >= 2025], reverse=True)
    by_year_out = [{
        "year":      y,
        "total_kr":  round(by_year[y]["web_kr"] + by_year[y]["vipps_kr"] + by_year[y].get("card_kr", 0.0), 2),
        "web_kr":    round(by_year[y]["web_kr"], 2),
        "vipps_kr":  round(by_year[y]["vipps_kr"], 2),
        "card_kr":   round(by_year[y].get("card_kr", 0.0), 2),
        "count":     by_year[y]["count"],
    } for y in years_list]

    return jsonify({
        "as_of": datetime.now().isoformat(),
        "year":  today.year,
        "period": {
            "from":      period_from.strftime("%Y-%m-%d"),
            "to":        period_to.strftime("%Y-%m-%d"),
            "label":     period_label,
            "total_kr":  round(period_total_kr, 2),
            "web_kr":    round(period_web_kr, 2),
            "vipps_kr":  round(period_vipps_kr, 2),
            "vipps_direct_kr":  round(sum((r.get("amount_ore") or 0) for r in direct_period) / 100.0, 2),
            "vipps_website_kr": round(sum((r.get("amount_ore") or 0) for r in website_period) / 100.0, 2),
            "vipps_count":      len(vipps_period_rows),
            "web_count":        len(web_period_rows),
            "card_kr":          round(period_card_kr, 2),
            "card_count":       len(card_period_rows),
            "avg_kr":           round(period_avg_kr, 2),
            "total_count":      period_charge_count,
            "cost_kr":          round(period_cost_kr, 2),
            "cost_count":       len(period_cost_rows),
        },
        "by_year": by_year_out,
        "totals": {
            "all_time_kr":  round(grand_total, 2),
            "today_kr":     round(grand_total_today, 2),
            "this_week_kr": round(grand_total_week, 2),
            "this_month_kr": round(grand_total_month, 2),
            "this_year_kr":  round(grand_total_year, 2),
        },
        "cost": {
            "this_year_kr":  round(cost_total_year, 2),
            "this_month_kr": round(cost_total_month, 2),
            "this_week_kr":  round(cost_total_week, 2),
            "today_kr":      round(cost_total_today, 2),
            "settled_count": len(cost_orders),
        },
        "web": {
            "count":          len(web_orders),
            "all_time_kr":    round(web_total, 2),
            "this_week_kr":   round(web_total_week, 2),
            "this_month_kr":  round(web_total_month, 2),
            "this_year_kr":   round(web_total_year, 2),
        },
        "vipps_csv": {
            "count":          len(vipps_imported),
            "all_time_kr":    round(vipps_total, 2),
            "this_week_kr":   round(vipps_total_week, 2),
            "this_month_kr":  round(vipps_total_month, 2),
            "this_year_kr":   round(vipps_total_year, 2),
        },
        "vipps_direct": {
            "count":          len(vipps_direct),
            "all_time_kr":    round(_sum_kr(vipps_direct), 2),
            "this_week_kr":   round(_sum_kr(vipps_direct, start_week), 2),
            "this_month_kr":  round(_sum_kr(vipps_direct, start_month), 2),
        },
        "vipps_website": {
            "count":          len(vipps_website),
            "all_time_kr":    round(_sum_kr(vipps_website), 2),
            "this_week_kr":   round(_sum_kr(vipps_website, start_week), 2),
            "this_month_kr":  round(_sum_kr(vipps_website, start_month), 2),
        },
        "card_payments": {
            "count":          len(card_imported),
            "all_time_kr":    round(card_total, 2),
            "this_week_kr":   round(card_total_week, 2),
            "this_month_kr":  round(card_total_month, 2),
            "this_year_kr":   round(card_total_year, 2),
        },
        "stripe": {
            "count":          len(stripe_paid),
            "all_time_kr":    round(stripe_total, 2),
        },
        "vipps_epay": {
            "count":          len(vipps_epay),
            "all_time_kr":    round(vipps_epay_total, 2),
        },
        "customers_count": len(_customers),
    })


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


# ─── STRIPE CHECKOUT (kort-betaling, parallelt med Vipps) ─────────────────────
def _stripe_configured():
    return bool(_stripe and STRIPE_SECRET_KEY)

@app.route("/api/stripe/config", methods=["GET"])
def api_stripe_config():
    """Eksponerer Stripe publishable key til frontend (trygt — den er offentlig)."""
    return jsonify({
        "ok": bool(STRIPE_PUBLISHABLE_KEY),
        "publishableKey": STRIPE_PUBLISHABLE_KEY,
        "configured": _stripe_configured() and bool(STRIPE_PUBLISHABLE_KEY),
    })


# ─── ABONNEMENT (Stripe Subscriptions for sjømatkasse) ────────────────────────
SUBSCRIPTIONS_FILE = os.path.join(STATE_DIR, "havoyet_subscriptions.json")
_subscriptions     = {}   # subscription_id → metadata

def _load_subscriptions():
    global _subscriptions
    if not os.path.exists(SUBSCRIPTIONS_FILE):
        return
    try:
        with open(SUBSCRIPTIONS_FILE, "r", encoding="utf-8") as f:
            _subscriptions = json.load(f) or {}
        print(f"[SUBS] Lastet {len(_subscriptions)} abonnementer")
    except Exception as e:
        print(f"[SUBS] Kunne ikke laste: {e}")

def _save_subscriptions():
    try:
        tmp = SUBSCRIPTIONS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_subscriptions, f, ensure_ascii=False)
        os.replace(tmp, SUBSCRIPTIONS_FILE)
    except Exception:
        pass

@app.route("/api/subscription/create", methods=["POST"])
def api_subscription_create():
    """Oppretter Stripe-Customer (eller henter eksisterende på e-post) + månedlig
    Subscription. Returnerer client_secret for første invoice slik at frontend
    kan bekrefte kortet. Stripe trekker automatisk hver måned etterpå."""
    if not _stripe_configured():
        return jsonify({"error": "Kortbetaling er ikke konfigurert"}), 503
    data    = request.get_json(silent=True) or {}
    amount  = int(data.get("amount", 0))     # i øre — månedlig beløp
    if amount < 100:
        return jsonify({"error": "Beløp må være minst 1 kr"}), 400
    kunde   = data.get("kunde") or {}
    kasse   = data.get("kasse") or {}
    email   = (kunde.get("epost") or "").strip().lower()
    if not email:
        return jsonify({"error": "E-post kreves"}), 400
    description = data.get("description") or "Sjømatkasse — månedlig abonnement"
    try:
        existing = _stripe.Customer.list(email=email, limit=1)
        if existing.data:
            customer = existing.data[0]
        else:
            customer = _stripe.Customer.create(
                email = email,
                name  = kunde.get("navn") or None,
                phone = (kunde.get("tlf") or "").replace(" ", "") or None,
                metadata = {"havoyet_kunde": "1"},
            )
        # Subscription.items.price_data støtter ikke product_data inline →
        # opprett Product + Price først, så referer til Price-ID.
        product = _stripe.Product.create(
            name = description,
            metadata = {"havoyet_kasse": (kasse.get("size") or "")[:30]},
        )
        price = _stripe.Price.create(
            currency    = "nok",
            unit_amount = amount,
            recurring   = {"interval": "month", "interval_count": 1},
            product     = product.id,
        )
        subscription = _stripe.Subscription.create(
            customer = customer.id,
            items    = [{"price": price.id}],
            payment_behavior      = "default_incomplete",
            payment_settings      = {
                "save_default_payment_method": "on_subscription",
                "payment_method_types": ["card"],
            },
            expand   = ["latest_invoice.payment_intent"],
            metadata = {
                "havoyet_kasse_config": json.dumps(kasse, ensure_ascii=False)[:500],
                "kunde_navn":           (kunde.get("navn") or "")[:200],
                "kunde_tlf":            (kunde.get("tlf") or "")[:30],
                "leveringsadresse":     (kunde.get("adresse") or "")[:200],
                "leveringspostnr":      (kunde.get("postnr") or "")[:10],
                "leveringssted":        (kunde.get("sted") or "")[:60],
                "leveringsdag":         (kunde.get("leveringsdag") or "")[:30],
                "kommentar":            (kunde.get("kommentar") or "")[:300],
            },
        )
    except Exception as e:
        import traceback as _tb
        _tb.print_exc()
        return jsonify({"error": f"Kunne ikke opprette abonnement: {e}"}), 502

    # Robust ekstraksjon av client_secret — hent invoicen + PaymentIntent eksplisitt
    # for å håndtere ulike Stripe SDK-versjoner som varierer i expand-formatet.
    client_secret = None
    try:
        latest = subscription.latest_invoice
        invoice_id = latest if isinstance(latest, str) else (getattr(latest, "id", None) if latest else None)
        if invoice_id:
            invoice = _stripe.Invoice.retrieve(invoice_id, expand=["payment_intent", "confirmation_secret"])
            pi = getattr(invoice, "payment_intent", None)
            if isinstance(pi, str):
                pi = _stripe.PaymentIntent.retrieve(pi)
            if pi is not None:
                client_secret = getattr(pi, "client_secret", None)
            if not client_secret:
                conf = getattr(invoice, "confirmation_secret", None)
                if conf is not None:
                    client_secret = getattr(conf, "client_secret", None)
    except Exception as _e:
        import traceback as _tb
        _tb.print_exc()
        print(f"[SUBS] Kunne ikke hente client_secret: {_e}")

    _subscriptions[subscription.id] = {
        "subscription_id":    subscription.id,
        "customer_id":        customer.id,
        "email":              email,
        "amount":             amount,
        "currency":           "nok",
        "interval":           "month",
        "status":             subscription.status,
        "current_period_end": getattr(subscription, "current_period_end", None),
        "kunde":              kunde,
        "kasse":              kasse,
        "description":        description,
        "created_at":         int(time.time()),
        "last_charged_at":    None,
        "charges_count":      0,
    }
    _save_subscriptions()

    return jsonify({
        "ok":             True,
        "subscriptionId": subscription.id,
        "customerId":     customer.id,
        "clientSecret":   client_secret,
        "status":         subscription.status,
    })

def _subscription_admin_required():
    user, _ = _user_from_request()
    if not user:
        return None, (jsonify({"ok": False, "error": "Ikke innlogget"}), 401)
    if user.get("role") != "admin":
        return None, (jsonify({"ok": False, "error": "Bare admin"}), 403)
    return user, None

@app.route("/api/subscription/list", methods=["GET"])
def api_subscription_list():
    user, err = _subscription_admin_required()
    if err: return err
    rows = sorted(_subscriptions.values(), key=lambda s: -(s.get("created_at") or 0))
    return jsonify({"ok": True, "rows": rows})

@app.route("/api/subscription/<sub_id>", methods=["DELETE"])
def api_subscription_cancel(sub_id):
    user, err = _subscription_admin_required()
    if err: return err
    if not _stripe_configured():
        return jsonify({"error": "Stripe ikke konfigurert"}), 503
    try:
        cancelled = _stripe.Subscription.cancel(sub_id)
    except Exception as e:
        return jsonify({"error": f"Kunne ikke kansellere: {e}"}), 502
    if sub_id in _subscriptions:
        _subscriptions[sub_id]["status"]       = cancelled.status
        _subscriptions[sub_id]["cancelled_at"] = int(time.time())
        _save_subscriptions()
    return jsonify({"ok": True, "status": cancelled.status})


@app.route("/api/subscription/admin-test-create", methods=["POST"])
def api_subscription_admin_test_create():
    """Admin-only: opprett et SYNTHETISK test-abonnement (ingen Stripe).
    Brukes kun for å se hvordan Min side rendrer aktive abonnement.
    Subscription-id får prefiks 'test_' så det er enkelt å rydde opp."""
    user, err = _subscription_admin_required()
    if err: return err
    data  = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    if not email:
        return jsonify({"ok": False, "error": "E-post kreves"}), 400
    amount   = int(data.get("amount", 1490_00))   # default: 1490 kr/mnd (i øre)
    kasse    = data.get("kasse") or {"name": "Sjømatkasse — 2 personer", "size": "2pers"}
    desc     = data.get("description") or "Sjømatkasse — månedlig abonnement (TEST)"
    now      = int(time.time())
    sub_id   = f"test_{int(time.time()*1000)}"
    next_period = now + 30 * 24 * 60 * 60   # ~30 dager fram
    _subscriptions[sub_id] = {
        "subscription_id":    sub_id,
        "customer_id":        f"test_cus_{now}",
        "email":              email,
        "amount":             amount,
        "currency":           "nok",
        "interval":           "month",
        "status":             "active",
        "current_period_end": next_period,
        "kunde":              {"epost": email, "navn": data.get("navn") or "Test Testesen"},
        "kasse":              kasse,
        "description":        desc,
        "created_at":         now,
        "last_charged_at":    now,
        "charges_count":      1,
        "is_test":            True,
    }
    _save_subscriptions()
    return jsonify({"ok": True, "subscription_id": sub_id, "row": _subscriptions[sub_id]})

@app.route("/api/subscription/admin-test/<sub_id>", methods=["DELETE"])
def api_subscription_admin_test_delete(sub_id):
    """Admin-only: slett et synthetisk test-abonnement. Kun id med prefiks 'test_'."""
    user, err = _subscription_admin_required()
    if err: return err
    if not sub_id.startswith("test_"):
        return jsonify({"ok": False, "error": "Bare test-abonnement (id må starte med 'test_')"}), 400
    if sub_id not in _subscriptions:
        return jsonify({"ok": False, "error": "Ikke funnet"}), 404
    del _subscriptions[sub_id]
    _save_subscriptions()
    return jsonify({"ok": True})


# ─── Kunde-vendte subscription-endepunkter (Min side) ──────────────────────────
# Sikkerhet: kunden identifiseres via e-post i body/query. Operasjoner sjekker
# at e-posten matcher subscription. Tidsfrist-regler håndheves serversiden.
SKIP_DEADLINE_DAYS    = 14   # Hopp over leveranse — minst 14 dager før
CANCEL_REFUND_DEADLINE_DAYS = 7  # Refusjon ved kansellering — minst 7 dager før neste trekk

def _subscription_for_email(sub_id, email):
    """Returner sub om eposten matcher, ellers (None, response)."""
    sub = _subscriptions.get(sub_id)
    if not sub:
        return None, (jsonify({"error": "Ikke funnet"}), 404)
    if (sub.get("email") or "").lower() != (email or "").strip().lower():
        return None, (jsonify({"error": "Tilhører ikke denne e-posten"}), 403)
    return sub, None

def _next_charge_ts(sub):
    """Beste estimat for neste-trekk-tidspunkt (ms). Bruker current_period_end
    fra Stripe om vi har det, ellers en måned etter siste trekk."""
    if sub.get("current_period_end"):
        return int(sub["current_period_end"]) * 1000
    last = sub.get("last_charged_at") or sub.get("created_at") or 0
    return int(last) * 1000 + 30 * 24 * 60 * 60 * 1000

@app.route("/api/subscription/mine", methods=["GET"])
def api_subscription_mine():
    """Lister abonnement(er) for én e-post. Brukes på Min side."""
    email = (request.args.get("email") or "").strip().lower()
    if not email:
        return jsonify({"ok": True, "rows": []})
    rows = []
    for sub in _subscriptions.values():
        if (sub.get("email") or "").lower() != email: continue
        rows.append({
            "subscription_id":   sub.get("subscription_id"),
            "status":            sub.get("status"),
            "amount":            sub.get("amount"),
            "description":       sub.get("description"),
            "kasse":             sub.get("kasse"),
            "interval":          sub.get("interval"),
            "created_at":        sub.get("created_at"),
            "last_charged_at":   sub.get("last_charged_at"),
            "next_charge_at":    int(_next_charge_ts(sub) / 1000),
            "charges_count":     sub.get("charges_count") or 0,
            "skipped_dates":     sub.get("skipped_dates") or [],
            "cancel_at_period_end": bool(sub.get("cancel_at_period_end")),
        })
    rows.sort(key=lambda r: -(r.get("created_at") or 0))
    return jsonify({"ok": True, "rows": rows})

@app.route("/api/subscription/<sub_id>/skip", methods=["POST"])
def api_subscription_skip(sub_id):
    """Kunden hopper over neste leveranse. Krever min 2 ukers varsel.
    Pauser Stripe-collection til perioden etter den hoppede leveransen."""
    if not _stripe_configured():
        return jsonify({"error": "Stripe ikke konfigurert"}), 503
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    sub, err = _subscription_for_email(sub_id, email)
    if err: return err
    next_ms  = _next_charge_ts(sub)
    days_left = (next_ms - int(time.time() * 1000)) / 86400000.0
    if days_left < SKIP_DEADLINE_DAYS:
        return jsonify({
            "ok": False,
            "error": f"For sent å hoppe over neste levering. Frist: {SKIP_DEADLINE_DAYS} dager før (du er {days_left:.1f} dager unna).",
            "deadline_days": SKIP_DEADLINE_DAYS,
            "days_left": round(days_left, 1),
        }), 400
    # Pauser Stripe-collection til etter skip-perioden
    try:
        resume_at = int((next_ms + 24 * 60 * 60 * 1000) / 1000)  # dagen etter neste trekk
        _stripe.Subscription.modify(sub_id, pause_collection={"behavior": "void", "resumes_at": resume_at})
    except Exception as e:
        return jsonify({"error": f"Kunne ikke pause: {e}"}), 502
    sub.setdefault("skipped_dates", []).append(int(next_ms / 1000))
    _save_subscriptions()
    return jsonify({"ok": True, "skipped_at": int(next_ms / 1000), "next_normal_charge": resume_at + 30 * 86400})

@app.route("/api/subscription/<sub_id>/customer-cancel", methods=["POST"])
def api_subscription_customer_cancel(sub_id):
    """Kunden kansellerer selv. Refusjon hvis > 1 uke før neste trekk;
    ellers kansellering uten refusjon (siste trekk gjelder)."""
    if not _stripe_configured():
        return jsonify({"error": "Stripe ikke konfigurert"}), 503
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    sub, err = _subscription_for_email(sub_id, email)
    if err: return err
    next_ms  = _next_charge_ts(sub)
    days_left = (next_ms - int(time.time() * 1000)) / 86400000.0
    refund_eligible = days_left >= CANCEL_REFUND_DEADLINE_DAYS
    try:
        if refund_eligible:
            # Kanseller umiddelbart + refunder siste vellykkede betaling
            cancelled = _stripe.Subscription.cancel(sub_id)
            try:
                # Refunder siste invoice om den finnes
                invoices = _stripe.Invoice.list(subscription=sub_id, limit=1)
                if invoices.data and invoices.data[0].status == "paid" and invoices.data[0].payment_intent:
                    _stripe.Refund.create(payment_intent=invoices.data[0].payment_intent)
            except Exception as _e:
                print(f"[SUBS] Refund-feil: {_e}")
            sub["status"]       = cancelled.status
            sub["cancelled_at"] = int(time.time())
            sub["refunded"]     = True
        else:
            # Sett til kanseller ved periode-slutt — siste trekk gjennomføres
            modified = _stripe.Subscription.modify(sub_id, cancel_at_period_end=True)
            sub["status"]               = modified.status
            sub["cancel_at_period_end"] = True
            sub["cancelled_at"]         = int(time.time())
            sub["refunded"]             = False
    except Exception as e:
        return jsonify({"error": f"Kunne ikke kansellere: {e}"}), 502
    _save_subscriptions()
    return jsonify({
        "ok": True,
        "refunded":        refund_eligible,
        "days_left":       round(days_left, 1),
        "refund_deadline": CANCEL_REFUND_DEADLINE_DAYS,
        "message": (
            "Abonnementet er kansellert og siste trekk er refundert."
            if refund_eligible else
            f"Abonnementet er kansellert, men det siste trekket ({sub.get('amount',0)/100:.0f} kr) blir gjennomført fordi det er mindre enn {CANCEL_REFUND_DEADLINE_DAYS} dager til neste trekk."
        ),
    })


@app.route("/api/subscription/<sub_id>/sync-price", methods=["POST"])
def api_subscription_sync_price(sub_id):
    """Synkroniser abonnement-pris til ønsket beløp (rekomputert i frontend
    fra dagens nettside-priser × original rabatt-prosent). Lager ny Stripe
    Price + bytter price-itemet på subscriptionen. Endringen slår inn ved
    neste fornyelse — ingen ekstra trekk skjer her.
    Body: {email, amount} (amount i ØRE; valgfri — frontend kan utelate hvis
    backend skal rekomputere selv basert på lagret kasse-meta)."""
    if not _stripe_configured():
        return jsonify({"error": "Stripe ikke konfigurert"}), 503
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    sub, err = _subscription_for_email(sub_id, email)
    if err: return err
    new_amount_kr = data.get("amount_kr")
    new_amount    = data.get("amount")
    # Aksepter både kroner og øre — clamp til rimelig intervall
    if new_amount_kr is not None and new_amount is None:
        new_amount = int(round(float(new_amount_kr) * 100))
    if not isinstance(new_amount, (int, float)) or new_amount < 5000 or new_amount > 5000000:
        return jsonify({"error": "Ugyldig beløp (må være 50–50 000 kr)"}), 400
    new_amount = int(new_amount)
    try:
        # Hent gjeldende subscription fra Stripe for product-id og items
        stripe_sub = _stripe.Subscription.retrieve(sub_id, expand=["items.data.price"])
        items = stripe_sub.get("items", {}).get("data", []) if isinstance(stripe_sub, dict) else stripe_sub.items.data
        if not items:
            return jsonify({"error": "Subscriptionen har ingen items"}), 502
        item = items[0]
        old_price = item.get("price") if isinstance(item, dict) else item.price
        product_id = (old_price.get("product") if isinstance(old_price, dict) else old_price.product) if old_price else None
        if not product_id:
            # Fallback: opprett nytt product
            product = _stripe.Product.create(name=sub.get("description") or "Sjømatkasse abonnement")
            product_id = product.id
        # Opprett ny price med samme intervall (måned)
        new_price = _stripe.Price.create(
            currency="nok",
            unit_amount=new_amount,
            recurring={"interval": "month", "interval_count": 1},
            product=product_id,
        )
        item_id = item.get("id") if isinstance(item, dict) else item.id
        # Bytt subscription-itemet til den nye prisen — proration_behavior=none
        # gjør at endringen kun gjelder fremover, ingen umiddelbar diff-belastning.
        _stripe.Subscription.modify(
            sub_id,
            items=[{"id": item_id, "price": new_price.id}],
            proration_behavior="none",
        )
        sub["amount"] = new_amount
        sub["last_price_sync_at"] = int(time.time())
        _save_subscriptions()
        return jsonify({"ok": True, "amount": new_amount, "message": "Abonnement-pris synkronisert. Endringen slår inn ved neste trekk."})
    except Exception as e:
        import traceback as _tb
        _tb.print_exc()
        return jsonify({"error": f"Kunne ikke synke pris: {e}"}), 502


@app.route("/api/checkout/card-payment-intent", methods=["POST"])
def api_checkout_card_payment_intent():
    """Stripe Elements-flyt: oppretter en PaymentIntent og returnerer client_secret
    så frontenden kan bekrefte betaling med kortdata in-line uten redirect."""
    if not _stripe_configured():
        return jsonify({"error": "Kortbetaling er ikke konfigurert"}), 503
    data = request.get_json(silent=True) or {}
    ordrenr  = data.get("ordrenr") or ("H" + str(int(time.time() * 1000))[-8:])
    amount   = int(data.get("amount", 0))   # i øre
    if amount <= 0:
        return jsonify({"error": "Ugyldig beløp"}), 400
    customer = data.get("customer") or {}
    try:
        intent = _stripe.PaymentIntent.create(
            amount=amount,
            currency="nok",
            description=f"Havøyet ordre {ordrenr}",
            receipt_email=customer.get("email") or None,
            automatic_payment_methods={"enabled": True, "allow_redirects": "never"},
            metadata={
                "ordrenr": str(ordrenr),
                "kunde_navn": str(customer.get("name", ""))[:200],
                "kunde_tlf":  str(customer.get("phoneNumber", ""))[:30],
            },
        )
    except Exception as e:
        return jsonify({"error": f"Kunne ikke opprette PaymentIntent: {e}"}), 502

    payments = _stripe_load_payments()
    payments[intent.id] = {
        "ordrenr":        ordrenr,
        "amount":         amount,
        "state":          "CREATED",
        "kind":           "elements",
        "created_at":     time.time(),
        "payment_intent": intent.id,
    }
    _stripe_save_payments(payments)

    return jsonify({
        "ok":           True,
        "clientSecret": intent.client_secret,
        "paymentIntent": intent.id,
        "ordrenr":      ordrenr,
    })

def _stripe_load_payments():
    if os.path.exists(STRIPE_PAYMENTS_FILE):
        try:
            with open(STRIPE_PAYMENTS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _stripe_save_payments(data):
    try:
        with open(STRIPE_PAYMENTS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass

@app.route("/api/checkout/card-init", methods=["POST"])
def api_checkout_card_init():
    """Oppretter en Stripe Checkout-sesjon for kortbetaling.
    Body: { ordrenr, amount (i øre), customer: { email, name }, returnUrl }"""
    if not _stripe_configured():
        return jsonify({"error": "Kortbetaling er ikke konfigurert (mangler Stripe-nøkler på serveren)"}), 503
    data = request.get_json(silent=True) or {}
    ordrenr = data.get("ordrenr") or ("H" + str(int(time.time() * 1000))[-8:])
    amount  = int(data.get("amount", 0))  # i øre (1 NOK = 100 øre)
    if amount <= 0:
        return jsonify({"error": "Ugyldig beløp"}), 400
    customer  = data.get("customer") or {}
    base_url  = (data.get("returnUrl") or request.host_url).rstrip("/")
    success_url = f"{base_url}/kasse?ordre={ordrenr}&card_session_id={{CHECKOUT_SESSION_ID}}"
    cancel_url  = f"{base_url}/kasse?ordre={ordrenr}&card_cancelled=1"

    try:
        session = _stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card"],
            line_items=[{
                "price_data": {
                    "currency": "nok",
                    "unit_amount": amount,
                    "product_data": {
                        "name": f"Havøyet ordre {ordrenr}",
                        "description": "Sjømat fra Havøyet",
                    },
                },
                "quantity": 1,
            }],
            success_url=success_url,
            cancel_url=cancel_url,
            customer_email=customer.get("email") or None,
            metadata={
                "ordrenr": str(ordrenr),
                "kunde_navn": str(customer.get("name", ""))[:200],
                "kunde_tlf":  str(customer.get("phoneNumber", ""))[:30],
            },
            locale="nb",
        )
    except Exception as e:
        return jsonify({"error": f"Kunne ikke opprette Stripe-sesjon: {e}"}), 502

    payments = _stripe_load_payments()
    payments[session.id] = {
        "ordrenr": ordrenr,
        "amount":  amount,
        "state":   "CREATED",
        "kind":    "card",
        "created_at": time.time(),
        "session_id": session.id,
        "payment_intent": session.payment_intent,
    }
    _stripe_save_payments(payments)

    return jsonify({
        "ok":         True,
        "url":        session.url,
        "sessionId":  session.id,
        "ordrenr":    ordrenr,
    })

@app.route("/api/checkout/card-status/<session_id>", methods=["GET"])
def api_checkout_card_status(session_id):
    """Polling-endepunkt — sjekker om kortbetalingen er fullført."""
    if not _stripe_configured():
        return jsonify({"error": "Kortbetaling er ikke konfigurert"}), 503
    try:
        session = _stripe.checkout.Session.retrieve(session_id)
    except Exception as e:
        return jsonify({"error": f"Kunne ikke hente Stripe-sesjon: {e}"}), 502
    payments = _stripe_load_payments()
    rec = payments.get(session_id, {})
    new_state = "PAID" if session.payment_status == "paid" else session.status.upper()
    rec["state"] = new_state
    rec["last_check"] = time.time()
    rec["payment_intent"] = session.payment_intent
    payments[session_id] = rec
    _stripe_save_payments(payments)
    return jsonify({
        "ok":      True,
        "state":   new_state,
        "ordrenr": rec.get("ordrenr"),
        "paid":    session.payment_status == "paid",
    })

@app.route("/api/webhooks/stripe", methods=["POST"])
def api_webhook_stripe():
    """Stripe sender hendelser hit (f.eks. checkout.session.completed)."""
    if not _stripe_configured():
        return jsonify({"error": "Stripe ikke konfigurert"}), 503
    payload    = request.data
    sig_header = request.headers.get("Stripe-Signature", "")

    # Hvis webhook-secret er konfigurert, verifiser signaturen
    if STRIPE_WEBHOOK_SECRET:
        try:
            event = _stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
        except Exception as e:
            return jsonify({"error": f"Ugyldig signatur: {e}"}), 400
    else:
        # Uten secret — kun for utvikling/test
        try:
            event = json.loads(payload.decode("utf-8") or "{}")
        except Exception:
            return jsonify({"error": "Ugyldig payload"}), 400

    etype = event.get("type") if isinstance(event, dict) else event["type"]
    obj   = (event.get("data") if isinstance(event, dict) else event["data"]).get("object", {}) or {}

    if etype == "checkout.session.completed":
        sess_id  = obj.get("id")
        ordrenr  = (obj.get("metadata") or {}).get("ordrenr")
        amount   = obj.get("amount_total") or 0
        paid     = obj.get("payment_status") == "paid"
        payments = _stripe_load_payments()
        rec = payments.get(sess_id, {})
        rec.update({
            "state":   "PAID" if paid else "COMPLETED",
            "ordrenr": ordrenr or rec.get("ordrenr"),
            "amount":  amount,
            "paid_at": time.time() if paid else rec.get("paid_at"),
            "payment_intent": obj.get("payment_intent"),
        })
        payments[sess_id] = rec
        _stripe_save_payments(payments)
        if paid and ordrenr:
            _notify_admins(
                "payment_received",
                f"[Havøyet] Kortbetaling mottatt #{ordrenr}",
                f"Beløp: {amount/100:.2f} kr (kort via Stripe)\nOrdre: {ordrenr}\nSession: {sess_id}",
            )
    elif etype == "checkout.session.expired":
        sess_id  = obj.get("id")
        payments = _stripe_load_payments()
        if sess_id in payments:
            payments[sess_id]["state"] = "EXPIRED"
            _stripe_save_payments(payments)

    elif etype == "payment_intent.succeeded":
        # PaymentIntent-flyt (Stripe Elements på ny.havoyet.no/kasse). Avgjørende
        # sikkerhetsnett: hvis frontend krasjer eller mister nett etter
        # confirmCardPayment, vil denne webhooken sørge for at ordren likevel
        # ender opp som PAID i admin/staff-listen og at varsel går ut.
        pi_id    = obj.get("id")
        ordrenr  = (obj.get("metadata") or {}).get("ordrenr")
        amount   = obj.get("amount") or obj.get("amount_received") or 0
        payments = _stripe_load_payments()
        rec = payments.get(pi_id, {})
        rec.update({
            "state":          "PAID",
            "ordrenr":        ordrenr or rec.get("ordrenr"),
            "amount":         amount,
            "paid_at":        time.time(),
            "payment_intent": pi_id,
            "kind":           rec.get("kind") or "elements",
        })
        payments[pi_id] = rec
        _stripe_save_payments(payments)
        if ordrenr:
            # Speil PAID-status inn i selve ordren så admin/staff-lista plukker
            # den opp uavhengig av om frontend rakk å POSTe /api/orders/new.
            order_found = False
            for o in _manual_orders:
                if str(o.get("ordrenr") or o.get("id") or "").strip() == str(ordrenr):
                    o["status"] = "PAID"
                    o["paymentStatus"] = "paid"
                    o["paid_at"] = datetime.now().isoformat()
                    order_found = True
                    break
            if order_found:
                _save_sync_state()
                _notify_admins(
                    "payment_received",
                    f"[Havøyet] Kortbetaling mottatt #{ordrenr}",
                    f"Beløp: {amount/100:.2f} kr (kort via Stripe Elements)\n"
                    f"Ordre: {ordrenr}\nPaymentIntent: {pi_id}",
                )
            else:
                # Kortet ble trukket, men selve ordre-objektet finnes ikke i
                # _manual_orders. Det betyr at frontend feilet etter Stripe-bekreftelsen
                # (nettglipp, lukket fane, JS-feil). Send ALARM til admin så betalingen
                # ikke forsvinner i tomrommet — admin kan opprette ordren manuelt
                # basert på Stripe-dashboardet.
                _notify_admins(
                    "payment_orphan",
                    f"[Havøyet] BETALING UTEN ORDRE #{ordrenr}",
                    f"Stripe har trukket {amount/100:.2f} kr men ordren finnes ikke "
                    f"i Flask. Sjekk Stripe-dashboardet for kunde-info og opprett "
                    f"ordren manuelt.\n\nOrdrenr: {ordrenr}\nPaymentIntent: {pi_id}\n"
                    f"Stripe-link: https://dashboard.stripe.com/payments/{pi_id}",
                )

    elif etype == "payment_intent.payment_failed":
        # Marker som FAILED og varsle hvis det er knyttet til en ordre.
        pi_id    = obj.get("id")
        ordrenr  = (obj.get("metadata") or {}).get("ordrenr")
        err_msg  = ((obj.get("last_payment_error") or {}).get("message")
                    or "Ukjent feil")
        payments = _stripe_load_payments()
        rec = payments.get(pi_id, {})
        rec.update({
            "state":          "FAILED",
            "ordrenr":        ordrenr or rec.get("ordrenr"),
            "failed_at":      time.time(),
            "error":          err_msg,
            "payment_intent": pi_id,
        })
        payments[pi_id] = rec
        _stripe_save_payments(payments)

    elif etype == "invoice.payment_succeeded":
        # Månedlig trekk på abonnement gikk gjennom
        sub_id = obj.get("subscription")
        amt    = obj.get("amount_paid") or 0
        if sub_id and sub_id in _subscriptions:
            sub = _subscriptions[sub_id]
            sub["status"]          = "active"
            sub["last_charged_at"] = int(time.time())
            sub["charges_count"]   = (sub.get("charges_count") or 0) + 1
            sub["last_invoice_id"] = obj.get("id")
            sub["last_amount"]     = amt
            _save_subscriptions()
            _notify_admins(
                "subscription_charge",
                f"[Havøyet] Abonnement trukket ({amt/100:.0f} kr)",
                f"Kunde: {sub.get('email')}\nBeløp: {amt/100:.2f} kr\nSubscription: {sub_id}\nTrekk #{sub['charges_count']}",
            )

    elif etype == "invoice.payment_failed":
        sub_id = obj.get("subscription")
        if sub_id and sub_id in _subscriptions:
            _subscriptions[sub_id]["status"] = "past_due"
            _save_subscriptions()
            _notify_admins(
                "subscription_failed",
                f"[Havøyet] Abonnementsbetaling FEILET",
                f"Kunde: {_subscriptions[sub_id].get('email')}\nSubscription: {sub_id}\nKortet ble avvist — kunden bør oppdatere kort.",
            )

    elif etype == "customer.subscription.deleted":
        sub_id = obj.get("id")
        if sub_id in _subscriptions:
            _subscriptions[sub_id]["status"]       = "cancelled"
            _subscriptions[sub_id]["cancelled_at"] = int(time.time())
            _save_subscriptions()

    return jsonify({"received": True})


# ─── ADMIN STRIPE-MIRROR ──────────────────────────────────────────────────────
# Speiler live-data fra Stripe-API-et inn i admin slik at admin og Stripe alltid
# viser identisk informasjon. Stripe forblir authoritative — ingen lokal kopi
# her (den eksisterende _stripe_load_payments cachen brukes kun for webhook-flyt).

def _admin_required_stripe():
    """Felles auth-helper for admin-Stripe-API. Returnerer (user, error_response)."""
    user, _ = _user_from_request()
    if not user:
        return None, (jsonify({"ok": False, "error": "Ikke innlogget"}), 401)
    if user.get("role") != "admin":
        return None, (jsonify({"ok": False, "error": "Bare admin"}), 403)
    return user, None


def _stripe_payment_to_dict(pi):
    """Konverter Stripe PaymentIntent til kompakt admin-form."""
    obj = pi if isinstance(pi, dict) else pi.to_dict()
    charges = (obj.get("charges") or {}).get("data") or []
    last_charge = charges[0] if charges else {}
    return {
        "id":               obj.get("id"),
        "amount":           obj.get("amount"),
        "amount_received":  obj.get("amount_received"),
        "currency":         obj.get("currency"),
        "status":           obj.get("status"),
        "created":          obj.get("created"),
        "description":      obj.get("description"),
        "customer":         obj.get("customer"),
        "metadata":         obj.get("metadata") or {},
        "receipt_email":    obj.get("receipt_email") or last_charge.get("receipt_email"),
        "receipt_url":      last_charge.get("receipt_url"),
        "payment_method":   obj.get("payment_method_types") or [],
        "refunded":         last_charge.get("refunded", False),
        "amount_refunded":  last_charge.get("amount_refunded", 0),
        "invoice":          obj.get("invoice"),
    }


@app.route("/api/admin/stripe/payments", methods=["GET"])
def api_admin_stripe_payments():
    """Liste de siste live-betalingene direkte fra Stripe."""
    user, err = _admin_required_stripe()
    if err: return err
    if not _stripe_configured():
        return jsonify({"ok": False, "error": "Stripe ikke konfigurert"}), 503
    try:
        limit = min(int(request.args.get("limit", 50) or 50), 100)
    except Exception:
        limit = 50
    starting_after = request.args.get("starting_after") or None
    try:
        kwargs = {"limit": limit}
        if starting_after: kwargs["starting_after"] = starting_after
        pis = _stripe.PaymentIntent.list(**kwargs)
        rows = [_stripe_payment_to_dict(pi) for pi in pis.data]
        return jsonify({
            "ok":       True,
            "rows":     rows,
            "has_more": pis.has_more,
            "next_cursor": rows[-1]["id"] if rows and pis.has_more else None,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": f"Stripe-feil: {e}"}), 502


@app.route("/api/admin/stripe/payments/<pi_id>", methods=["GET"])
def api_admin_stripe_payment_detail(pi_id):
    """Detaljer for én PaymentIntent — inkludert charges, refunds og kunde."""
    user, err = _admin_required_stripe()
    if err: return err
    if not _stripe_configured():
        return jsonify({"ok": False, "error": "Stripe ikke konfigurert"}), 503
    try:
        pi = _stripe.PaymentIntent.retrieve(
            pi_id,
            expand=["charges.data.balance_transaction", "customer", "invoice", "latest_charge"],
        )
        return jsonify({"ok": True, "payment": pi.to_dict()})
    except Exception as e:
        return jsonify({"ok": False, "error": f"Stripe-feil: {e}"}), 502


@app.route("/api/admin/stripe/payments/<pi_id>/refund", methods=["POST"])
def api_admin_stripe_refund(pi_id):
    """Utfør refusjon — full eller delvis (amount i øre)."""
    user, err = _admin_required_stripe()
    if err: return err
    if not _stripe_configured():
        return jsonify({"ok": False, "error": "Stripe ikke konfigurert"}), 503
    body   = request.get_json(silent=True) or {}
    amount = body.get("amount")    # i øre, valgfritt — None = full refusjon
    reason = body.get("reason")    # 'requested_by_customer' | 'duplicate' | 'fraudulent'
    try:
        kwargs = {"payment_intent": pi_id}
        if amount: kwargs["amount"] = int(amount)
        if reason: kwargs["reason"] = reason
        refund = _stripe.Refund.create(**kwargs)
        _notify_admins(
            "refund_issued",
            f"[Havøyet] Refusjon utstedt ({refund.amount/100:.2f} kr)",
            f"PaymentIntent: {pi_id}\nRefund: {refund.id}\nÅrsak: {reason or '—'}\nUtført av: {user.get('email')}",
        )
        return jsonify({"ok": True, "refund": refund.to_dict()})
    except Exception as e:
        return jsonify({"ok": False, "error": f"Refusjon feilet: {e}"}), 502


def _find_payment_intent_for_order(ordrenr):
    """Slår opp Stripe PaymentIntent-id for en gitt ordrenr i lokal payments-fil.
    Returnerer (intent_id, rec_dict) eller (None, None) hvis ikke funnet."""
    payments = _stripe_load_payments() or {}
    for pi_id, rec in payments.items():
        if str(rec.get("ordrenr") or "").strip() == str(ordrenr).strip():
            return pi_id, rec
    return None, None


@app.route("/api/admin/orders/<ordrenr>/refund", methods=["POST"])
def api_admin_order_refund(ordrenr):
    """Refunder en kundeordre direkte via Stripe.
    Body: { amount_ore: int, reason?: str, note?: str, lines?: list }
    Backend slår opp Stripe payment_intent fra ordrenr og utfører refusjonen.
    """
    user, err = _admin_required_stripe()
    if err: return err
    if not _stripe_configured():
        return jsonify({"ok": False, "error": "Stripe ikke konfigurert"}), 503
    body = request.get_json(silent=True) or {}
    try:
        amount_ore = int(body.get("amount_ore") or 0)
    except (TypeError, ValueError):
        amount_ore = 0
    if amount_ore <= 0:
        return jsonify({"ok": False, "error": "Ugyldig beløp"}), 400
    reason = body.get("reason") or None
    note   = (body.get("note") or "").strip() or None
    lines  = body.get("lines") or []

    pi_id, payment_rec = _find_payment_intent_for_order(ordrenr)
    if not pi_id:
        return jsonify({
            "ok": False,
            "error": f"Fant ingen Stripe-betaling for ordre {ordrenr}. "
                     f"Refusjon kan kun utføres for kortbetalinger gjort via nettsiden."
        }), 404

    try:
        kwargs = {"payment_intent": pi_id, "amount": amount_ore}
        if reason: kwargs["reason"] = reason
        refund = _stripe.Refund.create(**kwargs)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Refusjon feilet: {e}"}), 502

    # Logg refusjonen mot ordren i _manual_orders så den vises i admin
    refund_record = {
        "id":             refund.id,
        "amount_ore":     refund.amount,
        "amount_kr":      round(refund.amount / 100, 2),
        "reason":         reason,
        "note":           note,
        "lines":          lines,
        "status":         refund.status,
        "payment_intent": pi_id,
        "created_at":     time.time(),
        "by":             (user or {}).get("email"),
    }
    try:
        global _manual_orders
        for o in _manual_orders:
            if str(o.get("ordrenr") or "").strip() == str(ordrenr).strip():
                refunds = o.setdefault("refunds", [])
                refunds.append(refund_record)
                # Oppdater status hvis full refusjon
                paid_amount = int((payment_rec or {}).get("amount") or 0)
                refunded_total = sum(int(r.get("amount_ore") or 0) for r in refunds)
                if paid_amount and refunded_total >= paid_amount:
                    o["status"] = "REFUNDED"
                break
        _save_sync_state()
    except Exception as e:
        print(f"[refund] Klarte ikke oppdatere _manual_orders for {ordrenr}: {e}")

    _notify_admins(
        "refund_issued",
        f"[Havøyet] Refusjon utstedt ({refund.amount/100:.2f} kr) — ordre {ordrenr}",
        f"Ordrenr: {ordrenr}\nPaymentIntent: {pi_id}\nRefund: {refund.id}\n"
        f"Beløp: {refund.amount/100:.2f} kr\nÅrsak: {reason or '—'}\n"
        f"Notat: {note or '—'}\nUtført av: {(user or {}).get('email')}",
    )
    return jsonify({"ok": True, "refund": refund.to_dict(), "ordrenr": ordrenr})


@app.route("/api/admin/stripe/subscriptions", methods=["GET"])
def api_admin_stripe_subscriptions():
    """Liste alle abonnementer direkte fra Stripe (ikke kun lokale)."""
    user, err = _admin_required_stripe()
    if err: return err
    if not _stripe_configured():
        return jsonify({"ok": False, "error": "Stripe ikke konfigurert"}), 503
    status = (request.args.get("status") or "all").lower()
    try:
        limit = min(int(request.args.get("limit", 50) or 50), 100)
    except Exception:
        limit = 50
    try:
        kwargs = {"limit": limit, "expand": ["data.customer", "data.latest_invoice"]}
        if status != "all":
            kwargs["status"] = status
        subs = _stripe.Subscription.list(**kwargs)
        rows = []
        for s in subs.data:
            d    = s.to_dict()
            cust = d.get("customer") or {}
            item = ((d.get("items") or {}).get("data") or [{}])[0]
            price = item.get("price", {}) or {}
            rows.append({
                "id":                  d.get("id"),
                "status":              d.get("status"),
                "customer":            cust.get("id") if isinstance(cust, dict) else cust,
                "customer_email":      cust.get("email") if isinstance(cust, dict) else None,
                "customer_name":       cust.get("name")  if isinstance(cust, dict) else None,
                "current_period_end":  d.get("current_period_end"),
                "cancel_at_period_end":d.get("cancel_at_period_end"),
                "created":             d.get("created"),
                "metadata":            d.get("metadata") or {},
                "amount":              price.get("unit_amount"),
                "currency":            price.get("currency"),
                "interval":            (price.get("recurring") or {}).get("interval"),
            })
        return jsonify({"ok": True, "rows": rows, "has_more": subs.has_more})
    except Exception as e:
        return jsonify({"ok": False, "error": f"Stripe-feil: {e}"}), 502


@app.route("/api/admin/stripe/subscriptions/<sub_id>/cancel", methods=["POST"])
def api_admin_stripe_subscription_cancel(sub_id):
    """Kanseller abonnement — umiddelbart eller ved periodens slutt (default)."""
    user, err = _admin_required_stripe()
    if err: return err
    if not _stripe_configured():
        return jsonify({"ok": False, "error": "Stripe ikke konfigurert"}), 503
    body          = request.get_json(silent=True) or {}
    at_period_end = bool(body.get("at_period_end", True))
    try:
        if at_period_end:
            sub = _stripe.Subscription.modify(sub_id, cancel_at_period_end=True)
        else:
            sub = _stripe.Subscription.cancel(sub_id)
        # Speil til lokal cache hvis vi har den
        if sub_id in _subscriptions:
            _subscriptions[sub_id]["status"] = sub.status
            if not at_period_end:
                _subscriptions[sub_id]["cancelled_at"] = int(time.time())
            _save_subscriptions()
        return jsonify({
            "ok":                   True,
            "status":               sub.status,
            "cancel_at_period_end": sub.cancel_at_period_end,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": f"Kansellering feilet: {e}"}), 502


@app.route("/api/admin/stripe/customers", methods=["GET"])
def api_admin_stripe_customers():
    """Liste kunder direkte fra Stripe (kan filtreres på e-post)."""
    user, err = _admin_required_stripe()
    if err: return err
    if not _stripe_configured():
        return jsonify({"ok": False, "error": "Stripe ikke konfigurert"}), 503
    try:
        limit = min(int(request.args.get("limit", 50) or 50), 100)
    except Exception:
        limit = 50
    email = request.args.get("email") or None
    try:
        kwargs = {"limit": limit}
        if email: kwargs["email"] = email
        customers = _stripe.Customer.list(**kwargs)
        rows = [{
            "id":         d.get("id"),
            "email":      d.get("email"),
            "name":       d.get("name"),
            "phone":      d.get("phone"),
            "created":    d.get("created"),
            "balance":    d.get("balance"),
            "currency":   d.get("currency"),
            "delinquent": d.get("delinquent"),
            "metadata":   d.get("metadata") or {},
        } for d in [c.to_dict() for c in customers.data]]
        return jsonify({"ok": True, "rows": rows, "has_more": customers.has_more})
    except Exception as e:
        return jsonify({"ok": False, "error": f"Stripe-feil: {e}"}), 502


@app.route("/api/admin/stripe/disputes", methods=["GET"])
def api_admin_stripe_disputes():
    """Liste uavklarte saker / chargebacks."""
    user, err = _admin_required_stripe()
    if err: return err
    if not _stripe_configured():
        return jsonify({"ok": False, "error": "Stripe ikke konfigurert"}), 503
    try:
        limit = min(int(request.args.get("limit", 50) or 50), 100)
    except Exception:
        limit = 50
    try:
        disputes = _stripe.Dispute.list(limit=limit)
        rows = [{
            "id":             d.get("id"),
            "amount":         d.get("amount"),
            "currency":       d.get("currency"),
            "status":         d.get("status"),
            "reason":         d.get("reason"),
            "created":        d.get("created"),
            "due_by":         (d.get("evidence_details") or {}).get("due_by"),
            "charge":         d.get("charge"),
            "payment_intent": d.get("payment_intent"),
        } for d in [x.to_dict() for x in disputes.data]]
        return jsonify({"ok": True, "rows": rows, "has_more": disputes.has_more})
    except Exception as e:
        return jsonify({"ok": False, "error": f"Stripe-feil: {e}"}), 502


@app.route("/api/admin/stripe/balance", methods=["GET"])
def api_admin_stripe_balance():
    """Tilgjengelig + ventende balanse på Havøyets Stripe-konto."""
    user, err = _admin_required_stripe()
    if err: return err
    if not _stripe_configured():
        return jsonify({"ok": False, "error": "Stripe ikke konfigurert"}), 503
    try:
        b = _stripe.Balance.retrieve()
        return jsonify({
            "ok":        True,
            "available": [{"amount": x.amount, "currency": x.currency} for x in (b.available or [])],
            "pending":   [{"amount": x.amount, "currency": x.currency} for x in (b.pending or [])],
        })
    except Exception as e:
        return jsonify({"ok": False, "error": f"Stripe-feil: {e}"}), 502


# ── AUTH (admin-brukere + sesjoner) ───────────────────────────────────────────
# Brukere lagres på disk via _save_sync_state. Sesjoner er kun i minnet — overlever
# ikke restart, men det er greit (klienten ber bare om login på nytt).

_AUTH_SEED = [
    # env_hash_key: navnet på env-var som overstyrer passord-hashen. Settes denne
    # i Render brukes verdien fra env-var i stedet for default_hash under.
    # default_hash: fallback brukt når env-var er tom OG ingen lagret password_hash
    # finnes på disk (f.eks. etter at Render-/tmp er blanket). Dette er pbkdf2-hash
    # av et fast standardpassord — sett ADMIN_PASSWORD_HASH i Render om du vil bytte.
    {"email":        "erik@havoyet.no",
     "role":         "admin",
     "env_hash_key": "ADMIN_PASSWORD_HASH",
     "default_hash": "pbkdf2:sha256:1000000$Dm6U42Oy58sz18gi$7875ac305ba424251042f3cebcd7f92a2b608c544b1433358262c1612a45e6e5"},
    {"email":        "stian@havoyet.no",
     "role":         "user",
     "env_hash_key": "USER_PASSWORD_HASH",
     "default_hash": None},
]

def _seed_auth_users():
    """Sett opp standard-brukere ved oppstart. Prioritet for passord-hash:
       1) Miljøvariabel (env_hash_key)  — best, lett å rotere uten redeploy
       2) default_hash fra _AUTH_SEED   — fallback, garanterer at admin alltid
                                          kan logge inn selv etter Render-/tmp-wipe
       3) None → must_set_password=True (bare hvis hverken env eller default finnes)"""
    global _auth_users
    by_email = {u.get("email", "").lower(): u for u in _auth_users}
    for s in _AUTH_SEED:
        env_hash    = os.environ.get(s.get("env_hash_key") or "") or None
        default_h   = s.get("default_hash") or None
        seed_hash   = env_hash or default_h
        existing    = by_email.get(s["email"].lower())
        if existing is None:
            _auth_users.append({
                "email": s["email"],
                "role": s["role"],
                "password_hash": seed_hash,
                "must_set_password": not bool(seed_hash),
                "created_at": int(time.time()),
            })
        elif seed_hash and not existing.get("password_hash"):
            # Eksisterende bruker uten hash (etter en /tmp-wipe gjenoppstår
            # med must_set_password=True). Fyll inn fra env eller default.
            existing["password_hash"] = seed_hash
            existing["must_set_password"] = False

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

    # Foretrukket: stateless HMAC-token (overlever Render-restart)
    payload = _verify_stateless_token(token)
    if payload:
        user = _find_user(payload.get("email"))
        if user:
            return user, token

    # Bakoverkompatibel: gammel _auth_sessions-lookup
    sess = _auth_sessions.get(token)
    if not sess:
        return None, None
    user = _find_user(sess.get("email"))
    if not user:
        _auth_sessions.pop(token, None)
        return None, None
    return user, token

def _public_user(u):
    email = u.get("email") or ""
    # _is_active_subscriber defineres lenger ned i fila (i NYHETSBREV-ABONNENTER-
    # seksjonen). Defer-lookup med getattr for å unngå NameError ved import-rekkefølge.
    is_sub_fn = globals().get("_is_active_subscriber")
    is_subscriber = bool(is_sub_fn(email)) if (is_sub_fn and email) else False
    return {
        "email": email,
        "role": u.get("role"),
        "mustSetPassword": bool(u.get("must_set_password")),
        "hasPassword": bool(u.get("password_hash")),
        "createdAt": u.get("created_at"),
        "isNewsletterSubscriber": is_subscriber,
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
    token = _make_stateless_token(user["email"], user["role"])
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
    token = _make_stateless_token(user["email"], user["role"])
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
        _save_sync_state()
    return jsonify({"ok": True})

# ─── Glemt passord (passordreset via e-post) ────────────────────────────────
_pwd_reset_tokens = {}   # token → {email, expires_at}
PWD_RESET_TTL = 60 * 60  # 1 time

@app.route("/api/auth/forgot-password", methods=["POST"])
def api_auth_forgot_password():
    """Genererer en reset-token og sender e-post med link. Avslører ALDRI om
    e-posten finnes (returnerer alltid ok=true), for å unngå enumeration."""
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "Ugyldig e-post"}), 400
    user = _find_user(email)
    if user:
        # Rydd gamle tokens for samme bruker
        for tok in list(_pwd_reset_tokens.keys()):
            if _pwd_reset_tokens[tok].get("email") == email:
                _pwd_reset_tokens.pop(tok, None)
        token = secrets.token_urlsafe(32)
        _pwd_reset_tokens[token] = {
            "email": email,
            "expires_at": int(time.time()) + PWD_RESET_TTL,
        }
        # Send e-post med reset-link
        origin = (data.get("origin") or "").rstrip("/") or "https://havoyet.no"
        link = f"{origin}/reset-passord?token={token}"
        body = (
            f"Hei,\n\n"
            f"Vi mottok en forespørsel om å tilbakestille passordet for kontoen din ({email}).\n\n"
            f"Klikk på lenken under for å velge nytt passord. Lenken er gyldig i 1 time.\n\n"
            f"{link}\n\n"
            f"Hvis du ikke ba om dette, kan du trygt ignorere denne e-posten — "
            f"passordet ditt forblir uendret.\n\n"
            f"Hilsen Havøyet"
        )
        try:
            if RESEND_API_KEY:
                _send_via_resend("", "Havøyet", "Tilbakestill passord", body, to_email=email)
            elif SMTP_USER and SMTP_PASS:
                _send_via_smtp(SMTP_USER, "Havøyet", "Tilbakestill passord", body, to_email=email)
            else:
                print(f"[PWD-RESET] Ingen mail-konfig — link: {link}")
        except Exception as e:
            print(f"[PWD-RESET] Kunne ikke sende e-post: {e}")
    # Svar likt uavhengig av om e-posten finnes
    return jsonify({"ok": True, "message": "Hvis e-posten er registrert, har vi sendt deg en lenke."})

@app.route("/api/auth/reset-password", methods=["POST"])
def api_auth_reset_password():
    """Bruker reset-token + nytt passord."""
    data = request.get_json(silent=True) or {}
    token = (data.get("token") or "").strip()
    new_pwd = data.get("newPassword") or ""
    if not token: return jsonify({"ok": False, "error": "Mangler token"}), 400
    if len(new_pwd) < 8:
        return jsonify({"ok": False, "error": "Passordet må være minst 8 tegn"}), 400
    rec = _pwd_reset_tokens.get(token)
    if not rec or rec.get("expires_at", 0) < int(time.time()):
        _pwd_reset_tokens.pop(token, None)
        return jsonify({"ok": False, "error": "Lenken er utløpt eller ugyldig. Be om en ny."}), 400
    email = rec["email"]
    user = _find_user(email)
    if not user:
        _pwd_reset_tokens.pop(token, None)
        return jsonify({"ok": False, "error": "Bruker finnes ikke"}), 404
    user["password_hash"] = generate_password_hash(new_pwd)
    user["must_set_password"] = False
    _pwd_reset_tokens.pop(token, None)
    _save_sync_state()
    # Logg automatisk inn
    sess_token = secrets.token_urlsafe(32)
    _auth_sessions[sess_token] = {
        "email": user["email"], "role": user["role"], "created_at": int(time.time()),
    }
    _save_sync_state()
    return jsonify({"ok": True, "token": sess_token, "user": _public_user(user)})


@app.route("/api/customer/auth/register", methods=["POST"])
def api_customer_auth_register():
    """Selvbetjent kunde-registrering med passord. Oppretter ny konto og logger inn."""
    data = request.get_json(force=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "Ugyldig e-post"}), 400
    if len(password) < 8:
        return jsonify({"ok": False, "error": "Passordet må være minst 8 tegn"}), 400
    if _find_user(email):
        return jsonify({"ok": False, "error": "E-posten er allerede registrert. Logg inn i stedet."}), 409
    new_user = {
        "email": email,
        "role": "customer",
        "password_hash": generate_password_hash(password),
        "must_set_password": False,
        "created_at": int(time.time()),
    }
    _auth_users.append(new_user)
    token = secrets.token_urlsafe(32)
    _auth_sessions[token] = {
        "email": new_user["email"],
        "role": new_user["role"],
        "created_at": int(time.time()),
    }
    _save_sync_state()
    return jsonify({"ok": True, "token": token, "user": _public_user(new_user)})

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
    # Generer reset-token + send invitasjons-mail til ny bruker
    mail_status = "skipped"
    try:
        for tok in list(_pwd_reset_tokens.keys()):
            if _pwd_reset_tokens[tok].get("email") == email:
                _pwd_reset_tokens.pop(tok, None)
        invite_token = secrets.token_urlsafe(32)
        _pwd_reset_tokens[invite_token] = {
            "email": email,
            "expires_at": int(time.time()) + PWD_RESET_TTL,
        }
        origin = (data.get("origin") or request.headers.get("Origin") or "").rstrip("/")
        if not origin:
            origin = "https://admin.havoyet.no"
        link = f"{origin}/reset-passord?token={invite_token}"
        ok, detail = _send_user_invite_mail(email, link, role)
        mail_status = "sent" if ok else f"failed:{detail}"
        _log_admin_mail(actor.get("email", ""), email, "Velkommen til Havøyet admin", link, ok, detail)
    except Exception as e:
        mail_status = f"exception:{e}"
        print(f"[USER-INVITE] Kunne ikke sende mail: {e}")
    return jsonify({"ok": True, "user": _public_user(new_user), "mail_status": mail_status})

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
    mail_status = None
    if data.get("resetPassword"):
        target["password_hash"] = None
        target["must_set_password"] = True
        for tok in list(_auth_sessions.keys()):
            if _auth_sessions[tok].get("email", "").lower() == target["email"].lower():
                _auth_sessions.pop(tok, None)
        # Generer reset-token + send mail
        try:
            for tok in list(_pwd_reset_tokens.keys()):
                if _pwd_reset_tokens[tok].get("email") == target["email"].lower():
                    _pwd_reset_tokens.pop(tok, None)
            reset_token = secrets.token_urlsafe(32)
            _pwd_reset_tokens[reset_token] = {
                "email": target["email"].lower(),
                "expires_at": int(time.time()) + PWD_RESET_TTL,
            }
            origin = (data.get("origin") or request.headers.get("Origin") or "").rstrip("/")
            if not origin:
                origin = "https://admin.havoyet.no"
            link = f"{origin}/reset-passord?token={reset_token}"
            ok, detail = _send_user_password_reset_mail(target["email"], link)
            mail_status = "sent" if ok else f"failed:{detail}"
            _log_admin_mail(actor.get("email", ""), target["email"], "Tilbakestill passord", link, ok, detail)
        except Exception as e:
            mail_status = f"exception:{e}"
            print(f"[USER-RESET] Kunne ikke sende mail: {e}")
    _save_sync_state()
    resp = {"ok": True, "user": _public_user(target)}
    if mail_status is not None:
        resp["mail_status"] = mail_status
    return jsonify(resp)


# ── ADMIN → KUNDE/BRUKER MAIL ─────────────────────────────────────────────────
# Lar admin sende ad-hoc e-post fra Kunde-/Bruker-kort i admin-UI, samt
# masseutsending til alle kunder (utenom MailerLite-nyhetsbrev).
ADMIN_MAIL_LOG = os.path.join(os.path.dirname(_BASE_DIR), "admin_outgoing_mail.jsonl")


def _log_admin_mail(actor_email, to_email, subject, body, ok, detail):
    try:
        with open(ADMIN_MAIL_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "at": datetime.now().isoformat(),
                "from_actor": actor_email,
                "to": to_email,
                "subject": subject,
                "body_preview": (body or "")[:300],
                "ok": ok,
                "detail": detail,
            }, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[ADMIN-MAIL] Kunne ikke logge: {e}")


def _admin_collect_customer_emails(only_with_orders=False):
    """Returnerer unike e-postadresser fra kunder + ordre."""
    seen = set()
    out = []
    if not only_with_orders:
        for c in _customers:
            e = (c.get("epost") or "").strip().lower()
            if e and "@" in e and e not in seen:
                seen.add(e)
                out.append(e)
    for o in _manual_orders:
        e = (o.get("epost") or "").strip().lower()
        if not e:
            cust = o.get("customer") or {}
            e = (cust.get("email") or "").strip().lower()
        if e and "@" in e and e not in seen:
            seen.add(e)
            out.append(e)
    return out


@app.route("/api/admin/send-mail", methods=["POST"])
def api_admin_send_mail():
    """Send én e-post til én adresse. Krever innlogget admin/user.

    Body: { to_email, subject, body }
    """
    actor, _ = _user_from_request()
    if not actor:
        return jsonify({"ok": False, "error": "Ikke innlogget"}), 401
    data = request.get_json(force=True) or {}
    to_email = (data.get("to_email") or data.get("to") or "").strip().lower()
    subject = (data.get("subject") or "").strip()
    body = data.get("body") or ""
    if not to_email or "@" not in to_email:
        return jsonify({"ok": False, "error": "Ugyldig e-postadresse"}), 400
    if not subject:
        return jsonify({"ok": False, "error": "Mangler emne"}), 400
    if not body.strip():
        return jsonify({"ok": False, "error": "Mangler innhold"}), 400
    ok, detail = _send_admin_mail(to_email, subject, body)
    _log_admin_mail(actor.get("email", ""), to_email, subject, body, ok, detail)
    if not ok:
        return jsonify({"ok": False, "error": f"Kunne ikke sende: {detail}"}), 502
    return jsonify({"ok": True, "detail": detail})


@app.route("/api/admin/send-mail/bulk-customers", methods=["POST"])
def api_admin_send_mail_bulk_customers():
    """Send samme e-post til alle (eller utvalgte) kunder.

    Body: { subject, body, only_with_orders?: bool, dry_run?: bool }
    """
    actor, _ = _user_from_request()
    if not actor:
        return jsonify({"ok": False, "error": "Ikke innlogget"}), 401
    if actor.get("role") != "admin":
        return jsonify({"ok": False, "error": "Bare admin kan masseutsende"}), 403
    data = request.get_json(force=True) or {}
    subject = (data.get("subject") or "").strip()
    body = data.get("body") or ""
    only_with_orders = bool(data.get("only_with_orders"))
    dry_run = bool(data.get("dry_run"))
    if not subject:
        return jsonify({"ok": False, "error": "Mangler emne"}), 400
    if not body.strip():
        return jsonify({"ok": False, "error": "Mangler innhold"}), 400
    recipients = _admin_collect_customer_emails(only_with_orders=only_with_orders)
    if dry_run:
        return jsonify({"ok": True, "dry_run": True, "count": len(recipients), "recipients": recipients})
    sent, failed, failures = 0, 0, []
    for rcpt in recipients:
        try:
            ok, detail = _send_admin_mail(rcpt, subject, body)
        except Exception as e:
            ok, detail = False, f"exception: {e}"
        _log_admin_mail(actor.get("email", ""), rcpt, subject, body, ok, detail)
        if ok:
            sent += 1
        else:
            failed += 1
            failures.append({"to": rcpt, "detail": detail})
    return jsonify({
        "ok": True,
        "sent": sent,
        "failed": failed,
        "total": len(recipients),
        "failures": failures[:20],  # cap så respons ikke vokser
    })


def _send_user_invite_mail(email, link, role):
    """Send invitasjons-/passord-link til ny admin-bruker."""
    role_label = "administrator" if role == "admin" else "bruker"
    body = (
        f"Hei,\n\n"
        f"Du er invitert som {role_label} i Havøyet admin-panelet.\n\n"
        f"Klikk lenken under for å velge passord og logge inn. Lenken er "
        f"gyldig i 1 time:\n\n{link}\n\n"
        f"Hvis du ikke forventet denne invitasjonen, kan du trygt ignorere "
        f"denne e-posten.\n\n"
        f"Hilsen Havøyet"
    )
    return _send_admin_mail(email, "Velkommen til Havøyet admin", body)


def _send_user_password_reset_mail(email, link):
    """Send passord-reset-link til eksisterende admin-bruker."""
    body = (
        f"Hei,\n\n"
        f"En administrator har tilbakestilt passordet for kontoen din "
        f"({email}).\n\n"
        f"Klikk lenken under for å velge nytt passord. Lenken er gyldig i "
        f"1 time:\n\n{link}\n\n"
        f"Hvis du ikke forventet dette, kontakt erik@havoyet.no.\n\n"
        f"Hilsen Havøyet"
    )
    return _send_admin_mail(email, "Tilbakestill passord — Havøyet admin", body)


# ── ANALYTICS (kundebevegelse-tracking) ───────────────────────────────────────
# Lagrer events fra nettsiden når besøkende har samtykket til markedsføring.
# Aggregerer drop-off, funnel, klikk-heatmap og navigasjonsstier.
ANALYTICS_FILE         = os.path.join(STATE_DIR, "havoyet_analytics.json")
ANALYTICS_MAX_EVENTS   = 50000   # FIFO-cap så filen ikke vokser ubegrenset
ANALYTICS_LOCK         = threading.Lock()
_analytics             = {"events": [], "sessions": {}}
_last_analytics_save   = [0.0]   # liste for muterbar closure-state

def _load_analytics():
    global _analytics
    if not os.path.exists(ANALYTICS_FILE):
        return
    try:
        with open(ANALYTICS_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        _analytics["events"]   = d.get("events", []) or []
        _analytics["sessions"] = d.get("sessions", {}) or {}
        print(f"[ANALYTICS] Lastet {len(_analytics['events'])} events, {len(_analytics['sessions'])} sesjoner")
    except Exception as e:
        print(f"[ANALYTICS] Kunne ikke laste: {e}")

def _save_analytics():
    try:
        tmp = ANALYTICS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_analytics, f, ensure_ascii=False)
        os.replace(tmp, ANALYTICS_FILE)
    except Exception:
        pass

def _maybe_persist_analytics(force=False):
    now = time.time()
    if force or now - _last_analytics_save[0] > 5:
        _last_analytics_save[0] = now
        _save_analytics()

def _analytics_record_event(ev):
    """Append event, oppdater session-summary."""
    is_funnel = False
    with ANALYTICS_LOCK:
        events = _analytics["events"]
        events.append(ev)
        if len(events) > ANALYTICS_MAX_EVENTS:
            del events[: len(events) - ANALYTICS_MAX_EVENTS]
        sid = ev.get("sid")
        if sid:
            sess = _analytics["sessions"].setdefault(sid, {
                "did":           ev.get("did"),
                "started_at":    ev.get("ts"),
                "last_event_at": ev.get("ts"),
                "first_path":    ev.get("path"),
                "last_path":     ev.get("path"),
                "pages":         [],
                "events":        0,
                "referrer":      ev.get("referrer", ""),
                "user_agent":    ev.get("ua", ""),
                "funnel":        {},
            })
            sess["last_event_at"] = ev.get("ts") or sess.get("last_event_at")
            sess["events"]        = (sess.get("events") or 0) + 1
            if ev.get("path"):
                sess["last_path"] = ev["path"]
            if ev.get("type") == "pageview" and ev.get("path"):
                pages = sess.setdefault("pages", [])
                if not pages or pages[-1] != ev["path"]:
                    pages.append(ev["path"])
                    if len(pages) > 50:
                        del pages[:-50]
            if ev.get("type") == "funnel_step" and ev.get("step"):
                sess.setdefault("funnel", {})[ev["step"]] = ev.get("ts")
                is_funnel = True
    # Funnel-steg er konverterings-kritiske → lagres umiddelbart.
    # Andre events (click/scroll/pageview/exit) throttles for å unngå I/O-press.
    _maybe_persist_analytics(force=is_funnel)

def _analytics_admin_required():
    user, _ = _user_from_request()
    if not user:
        return None, (jsonify({"ok": False, "error": "Ikke innlogget"}), 401)
    if user.get("role") != "admin":
        return None, (jsonify({"ok": False, "error": "Bare admin"}), 403)
    return user, None

@app.route("/api/analytics/event", methods=["POST", "OPTIONS"])
def api_analytics_event():
    """Offentlig endpoint — godtar batch eller enkel event fra nettside-tracker.
    Klienten styrer samtykke; serveren aksepterer alt som har riktig struktur."""
    if request.method == "OPTIONS":
        return ("", 204)
    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        data = {}
    if not data and request.data:
        try:
            data = json.loads(request.data.decode("utf-8"))
        except Exception:
            return jsonify({"ok": False, "error": "Bad JSON"}), 400
    events = data.get("events") if isinstance(data, dict) else None
    if events is None and isinstance(data, dict) and data.get("type"):
        events = [data]
    if not isinstance(events, list):
        return jsonify({"ok": False, "error": "events må være liste"}), 400
    ua = (request.headers.get("User-Agent") or "")[:200]
    accepted = 0
    for raw in events[:100]:
        if not isinstance(raw, dict):
            continue
        t = raw.get("type")
        if t not in ("pageview", "click", "scroll", "exit", "funnel_step"):
            continue
        ev = {
            "type": t,
            "sid":  str(raw.get("sid") or "")[:64],
            "did":  str(raw.get("did") or "")[:64],
            "path": str(raw.get("path") or "")[:200],
            "ts":   int(raw.get("ts") or time.time() * 1000),
            "ua":   ua,
        }
        try:
            if t == "click":
                ev["x_pct"]  = max(0.0, min(100.0, float(raw.get("x_pct") or 0)))
                ev["y_pct"]  = max(0.0, min(100.0, float(raw.get("y_pct") or 0)))
                ev["target"] = str(raw.get("target") or "")[:120]
            elif t == "scroll":
                ev["depth_pct"] = max(0, min(100, int(raw.get("depth_pct") or 0)))
            elif t == "exit":
                ev["time_ms"]    = max(0, int(raw.get("time_ms") or 0))
                ev["max_scroll"] = max(0, min(100, int(raw.get("max_scroll") or 0)))
            elif t == "funnel_step":
                step = str(raw.get("step") or "")[:40]
                if not step:
                    continue
                ev["step"] = step
                ev["meta"] = str(raw.get("meta") or "")[:200]
            elif t == "pageview":
                ev["referrer"] = str(raw.get("referrer") or "")[:200]
        except Exception:
            continue
        _analytics_record_event(ev)
        accepted += 1
    return jsonify({"ok": True, "accepted": accepted})

@app.route("/api/analytics/summary", methods=["GET"])
def api_analytics_summary():
    user, err = _analytics_admin_required()
    if err: return err
    now = int(time.time() * 1000)
    cutoff_24h = max(0, now - 24 * 60 * 60 * 1000)
    cutoff_7d  = max(0, now - 7 * 24 * 60 * 60 * 1000)
    events     = _analytics.get("events", []) or []
    sessions   = _analytics.get("sessions", {}) or {}
    pageviews  = sum(1 for e in events if e.get("type") == "pageview")
    pv_24h     = sum(1 for e in events if e.get("type") == "pageview" and e.get("ts", 0) >= cutoff_24h)
    pv_7d      = sum(1 for e in events if e.get("type") == "pageview" and e.get("ts", 0) >= cutoff_7d)
    sess_24h   = sum(1 for s in sessions.values() if (s.get("started_at") or 0) >= cutoff_24h)
    devices    = len({s.get("did") for s in sessions.values() if s.get("did")})
    clicks     = sum(1 for e in events if e.get("type") == "click")
    return jsonify({
        "ok": True,
        "totals":   {"events": len(events), "sessions": len(sessions),
                     "devices": devices, "pageviews": pageviews, "clicks": clicks},
        "last_24h": {"pageviews": pv_24h, "sessions": sess_24h},
        "last_7d":  {"pageviews": pv_7d},
    })

@app.route("/api/analytics/funnel", methods=["GET"])
def api_analytics_funnel():
    user, err = _analytics_admin_required()
    if err: return err
    steps  = ["session_start", "view_pdp", "add_to_cart", "begin_checkout", "order_complete"]
    counts = {s: 0 for s in steps}
    for sess in (_analytics.get("sessions", {}) or {}).values():
        counts["session_start"] += 1
        f = sess.get("funnel") or {}
        for s in steps[1:]:
            if s in f:
                counts[s] += 1
    rows = []
    base = counts[steps[0]] or 1
    for i, s in enumerate(steps):
        n = counts[s]
        rows.append({
            "step":       s,
            "count":      n,
            "rate":       round(n / (counts[steps[i-1]] or 1) * 100, 1) if i > 0 else 100.0,
            "rate_total": round(n / base * 100, 1),
        })
    return jsonify({"ok": True, "steps": rows})

@app.route("/api/analytics/dropoff", methods=["GET"])
def api_analytics_dropoff():
    user, err = _analytics_admin_required()
    if err: return err
    cnt = {}
    for sess in (_analytics.get("sessions", {}) or {}).values():
        if (sess.get("funnel") or {}).get("order_complete"):
            continue
        p = sess.get("last_path") or "(ukjent)"
        cnt[p] = cnt.get(p, 0) + 1
    rows = sorted(cnt.items(), key=lambda kv: -kv[1])[:20]
    return jsonify({"ok": True, "rows": [{"path": p, "count": n} for p, n in rows]})

@app.route("/api/analytics/pages", methods=["GET"])
def api_analytics_pages():
    user, err = _analytics_admin_required()
    if err: return err
    pv, ck, ex, t_ms, scr = {}, {}, {}, {}, {}
    for ev in (_analytics.get("events", []) or []):
        p, t = ev.get("path") or "", ev.get("type")
        if   t == "pageview": pv[p] = pv.get(p, 0) + 1
        elif t == "click":    ck[p] = ck.get(p, 0) + 1
        elif t == "exit":
            ex[p] = ex.get(p, 0) + 1
            t_ms.setdefault(p, []).append(int(ev.get("time_ms") or 0))
            scr.setdefault(p, []).append(int(ev.get("max_scroll") or 0))
    rows = []
    for p in sorted(pv.keys(), key=lambda p: -pv[p])[:30]:
        tl = t_ms.get(p, []); sl = scr.get(p, [])
        rows.append({
            "path":           p,
            "pageviews":      pv.get(p, 0),
            "clicks":         ck.get(p, 0),
            "exits":          ex.get(p, 0),
            "avg_time_s":     round(sum(tl)/len(tl)/1000, 1) if tl else 0,
            "avg_scroll_pct": round(sum(sl)/len(sl), 1)      if sl else 0,
        })
    return jsonify({"ok": True, "rows": rows})

@app.route("/api/analytics/heatmap", methods=["GET"])
def api_analytics_heatmap():
    user, err = _analytics_admin_required()
    if err: return err
    path   = request.args.get("path", "/")
    grid_n = 20
    if not path or len(path) > 200:
        return jsonify({"ok": True, "path": path, "grid": [[]], "total": 0, "grid_size": grid_n})
    grid   = [[0] * grid_n for _ in range(grid_n)]
    total  = 0
    for ev in (_analytics.get("events", []) or []):
        if ev.get("type") != "click" or ev.get("path") != path:
            continue
        gx = min(grid_n - 1, int(float(ev.get("x_pct") or 0) / 100 * grid_n))
        gy = min(grid_n - 1, int(float(ev.get("y_pct") or 0) / 100 * grid_n))
        grid[gy][gx] += 1
        total += 1
    return jsonify({"ok": True, "path": path, "grid": grid, "total": total, "grid_size": grid_n})

@app.route("/api/analytics/paths", methods=["GET"])
def api_analytics_paths():
    user, err = _analytics_admin_required()
    if err: return err
    seq_count = {}
    for sess in (_analytics.get("sessions", {}) or {}).values():
        pages = sess.get("pages") or []
        if not pages:
            continue
        seq = " → ".join(pages[:5])
        seq_count[seq] = seq_count.get(seq, 0) + 1
    rows = sorted(seq_count.items(), key=lambda kv: -kv[1])[:20]
    return jsonify({"ok": True, "rows": [{"path": s, "count": n} for s, n in rows]})

@app.route("/api/analytics/sessions", methods=["GET"])
def api_analytics_sessions():
    user, err = _analytics_admin_required()
    if err: return err
    items = sorted((_analytics.get("sessions", {}) or {}).items(),
                   key=lambda kv: -(kv[1].get("started_at") or 0))[:50]
    rows  = []
    for sid, s in items:
        rows.append({
            "sid":           sid,
            "did":           s.get("did"),
            "started_at":    s.get("started_at"),
            "last_event_at": s.get("last_event_at"),
            "events":        s.get("events", 0),
            "first_path":    s.get("first_path"),
            "last_path":     s.get("last_path"),
            "page_count":    len(s.get("pages") or []),
            "referrer":      s.get("referrer"),
            "ua":            (s.get("user_agent") or "")[:80],
            "funnel":        s.get("funnel") or {},
        })
    return jsonify({"ok": True, "rows": rows})

@app.route("/api/analytics/clear", methods=["POST"])
def api_analytics_clear():
    user, err = _analytics_admin_required()
    if err: return err
    with ANALYTICS_LOCK:
        _analytics["events"]   = []
        _analytics["sessions"] = {}
        _maybe_persist_analytics(force=True)
    with REPLAY_LOCK:
        _replays.clear()
        _save_replays()
    return jsonify({"ok": True})


# ── SESSION REPLAY (rrweb-events per sesjon) ─────────────────────────────────
# Lagrer rrweb-events per session-id slik at admin kan se hele kundereisen som
# et videoopptak. Hver sesjon capped til REPLAY_MAX_EVENTS_PER_SID; totalt antall
# sesjoner capped til REPLAY_MAX_SESSIONS (FIFO på sist oppdatert).
REPLAY_FILE                   = os.path.join(STATE_DIR, "havoyet_replays.json")
REPLAY_MAX_SESSIONS           = 60
REPLAY_MAX_EVENTS_PER_SID     = 5000
REPLAY_LOCK                   = threading.Lock()
_replays                      = {}    # sid → {"did","path","updated_at","events":[...]}
_last_replay_save             = [0.0]

def _load_replays():
    global _replays
    if not os.path.exists(REPLAY_FILE):
        return
    try:
        with open(REPLAY_FILE, "r", encoding="utf-8") as f:
            _replays = json.load(f) or {}
        total = sum(len(s.get("events") or []) for s in _replays.values())
        print(f"[REPLAY] Lastet {len(_replays)} sesjoner ({total} events)")
    except Exception as e:
        print(f"[REPLAY] Kunne ikke laste: {e}")

def _save_replays():
    try:
        tmp = REPLAY_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_replays, f, ensure_ascii=False)
        os.replace(tmp, REPLAY_FILE)
    except Exception:
        pass

def _maybe_persist_replays(force=False):
    # Klienten batcher allerede events (60/4s), så vi lagrer på hvert POST.
    # Atomisk skriving (tmp + replace) holder filen konsistent selv ved crash.
    _last_replay_save[0] = time.time()
    _save_replays()

@app.route("/api/analytics/replay", methods=["POST", "OPTIONS"])
def api_analytics_replay_post():
    """Offentlig — godtar batch av rrweb-events fra nettside-tracker."""
    if request.method == "OPTIONS":
        return ("", 204)
    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        data = {}
    if not data and request.data:
        try:
            data = json.loads(request.data.decode("utf-8"))
        except Exception:
            return jsonify({"ok": False, "error": "Bad JSON"}), 400
    sid    = str(data.get("sid") or "")[:64]
    did    = str(data.get("did") or "")[:64]
    path   = str(data.get("path") or "")[:200]
    events = data.get("events")
    if not sid or not isinstance(events, list) or not events:
        return jsonify({"ok": False, "error": "sid og events kreves"}), 400
    with REPLAY_LOCK:
        sess = _replays.setdefault(sid, {
            "did": did, "path": path,
            "started_at":   int(time.time() * 1000),
            "updated_at":   int(time.time() * 1000),
            "events":       [],
        })
        sess["updated_at"] = int(time.time() * 1000)
        if not sess.get("did") and did: sess["did"] = did
        if path: sess["path"] = path
        # Legg til events, og cap på antall
        ev_list = sess.setdefault("events", [])
        for e in events[:1000]:  # max 1000 events per request
            if isinstance(e, dict):
                ev_list.append(e)
        if len(ev_list) > REPLAY_MAX_EVENTS_PER_SID:
            del ev_list[: len(ev_list) - REPLAY_MAX_EVENTS_PER_SID]
        # FIFO på antall sesjoner
        if len(_replays) > REPLAY_MAX_SESSIONS:
            old = sorted(_replays.items(), key=lambda kv: kv[1].get("updated_at", 0))
            for k, _ in old[: len(_replays) - REPLAY_MAX_SESSIONS]:
                _replays.pop(k, None)
    _maybe_persist_replays()
    return jsonify({"ok": True, "stored": len(events)})

@app.route("/api/analytics/replay", methods=["GET"])
def api_analytics_replay_get():
    user, err = _analytics_admin_required()
    if err: return err
    sid = request.args.get("sid", "").strip()
    if not sid:
        # Liste-modus: returner sammendrag for alle sesjoner med replay
        rows = []
        for k, v in _replays.items():
            rows.append({
                "sid":         k,
                "did":         v.get("did"),
                "path":        v.get("path"),
                "started_at":  v.get("started_at"),
                "updated_at":  v.get("updated_at"),
                "event_count": len(v.get("events") or []),
            })
        rows.sort(key=lambda r: -(r["updated_at"] or 0))
        return jsonify({"ok": True, "rows": rows[:60]})
    sess = _replays.get(sid)
    if not sess:
        return jsonify({"ok": False, "error": "sesjon ikke funnet"}), 404
    return jsonify({
        "ok":         True,
        "sid":        sid,
        "did":        sess.get("did"),
        "path":       sess.get("path"),
        "started_at": sess.get("started_at"),
        "updated_at": sess.get("updated_at"),
        "events":     sess.get("events") or [],
    })

@app.route("/api/analytics/replay", methods=["DELETE"])
def api_analytics_replay_delete():
    user, err = _analytics_admin_required()
    if err: return err
    sid = request.args.get("sid", "").strip()
    with REPLAY_LOCK:
        if sid:
            _replays.pop(sid, None)
        else:
            _replays.clear()
        _maybe_persist_replays(force=True)
    return jsonify({"ok": True})


# ── CHATBOT ───────────────────────────────────────────────────────────────────
# Self-service chat på nettsiden. Kunde får AI-svar; når AI er usikker
# foreslår den å koble til menneske → kunde svarer Ja → e-post til Erik+Stian
# og samtalen åpnes i admin-panelet for to-veis svar.
#
# Persistent storage på samme STATE_DIR som annen sync-state. Ingen separate
# load/save funksjoner — vi fsync-er per skriving (fil er liten).
CHAT_SESSIONS_FILE  = os.path.join(STATE_DIR, "havoyet_chat_sessions.json")
CHAT_KNOWLEDGE_FILE = os.path.join(STATE_DIR, "havoyet_chat_knowledge.json")
CHAT_ARCHIVE_FILE   = os.path.join(STATE_DIR, "havoyet_chat_archive.jsonl")
CHAT_HUMAN_RECIPIENTS = ["erik@havoyet.no", "stian@havoyet.no"]
_chat_sessions = {}     # id → {id, customer:{name,email}, messages:[], status, created_at, updated_at, escalated, last_admin_read, last_customer_read}
_chat_knowledge = []    # [{q, a, learned_at, session_id}]
_chat_lock = threading.Lock()


def _load_chat_state():
    global _chat_sessions, _chat_knowledge
    try:
        if os.path.exists(CHAT_SESSIONS_FILE):
            with open(CHAT_SESSIONS_FILE, "r", encoding="utf-8") as f:
                _chat_sessions = json.load(f) or {}
    except Exception as e:
        print(f"[CHAT] Kunne ikke laste sessions: {e}")
        _chat_sessions = {}
    try:
        if os.path.exists(CHAT_KNOWLEDGE_FILE):
            with open(CHAT_KNOWLEDGE_FILE, "r", encoding="utf-8") as f:
                _chat_knowledge = json.load(f) or []
    except Exception as e:
        print(f"[CHAT] Kunne ikke laste knowledge: {e}")
        _chat_knowledge = []


def _save_chat_sessions():
    try:
        tmp = CHAT_SESSIONS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_chat_sessions, f, ensure_ascii=False)
        os.replace(tmp, CHAT_SESSIONS_FILE)
    except Exception as e:
        print(f"[CHAT] save sessions feilet: {e}")


def _save_chat_knowledge():
    try:
        tmp = CHAT_KNOWLEDGE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_chat_knowledge, f, ensure_ascii=False)
        os.replace(tmp, CHAT_KNOWLEDGE_FILE)
    except Exception as e:
        print(f"[CHAT] save knowledge feilet: {e}")


# Append-only "for-alltid"-arkiv. Hver hendelse i chat (kunde/AI/admin-melding,
# Q&A trening, escalering, sletting) blir én linje i JSONL-fila. Aldri muteres.
# Selv om en samtale slettes fra aktiv state, beholdes hele historikken her.
_chat_archive_lock = threading.Lock()


def _archive_chat_event(kind, payload):
    try:
        ev = {
            "id": "a_" + secrets.token_urlsafe(10),
            "ts": datetime.now().isoformat(),
            "kind": kind,
            "data": payload,
        }
        line = json.dumps(ev, ensure_ascii=False) + "\n"
        with _chat_archive_lock:
            with open(CHAT_ARCHIVE_FILE, "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
    except Exception as e:
        print(f"[CHAT-ARCHIVE] append feilet ({kind}): {e}")


def _read_chat_archive(limit=None, kind_filter=None, session_id=None):
    out = []
    try:
        if not os.path.exists(CHAT_ARCHIVE_FILE):
            return out
        with _chat_archive_lock:
            with open(CHAT_ARCHIVE_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        ev = json.loads(line)
                    except Exception:
                        continue
                    if kind_filter and ev.get("kind") != kind_filter:
                        continue
                    if session_id:
                        sid_in = (ev.get("data") or {}).get("session_id")
                        if sid_in != session_id:
                            continue
                    out.append(ev)
    except Exception as e:
        print(f"[CHAT-ARCHIVE] read feilet: {e}")
    if limit:
        try:
            out = out[-int(limit):]
        except Exception:
            pass
    return out


def _chat_archive_stats():
    count = 0
    bytes_total = 0
    try:
        if os.path.exists(CHAT_ARCHIVE_FILE):
            bytes_total = os.path.getsize(CHAT_ARCHIVE_FILE)
            with open(CHAT_ARCHIVE_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        count += 1
    except Exception:
        pass
    return {"events": count, "bytes": bytes_total}


def _chat_new_id():
    return secrets.token_urlsafe(12)


def _chat_session_summary(s):
    msgs = s.get("messages") or []
    last = msgs[-1] if msgs else None
    customer_msgs = [m for m in msgs if m.get("role") == "customer"]
    last_customer_at = customer_msgs[-1].get("at") if customer_msgs else None
    return {
        "id": s.get("id"),
        "customer": s.get("customer", {}),
        "status": s.get("status", "open"),
        "escalated": bool(s.get("escalated")),
        "created_at": s.get("created_at"),
        "updated_at": s.get("updated_at"),
        "last_message": (last or {}).get("text", "")[:200],
        "last_message_at": (last or {}).get("at"),
        "last_message_role": (last or {}).get("role"),
        "last_customer_at": last_customer_at,
        "message_count": len(msgs),
        "unread_for_admin": int(s.get("unread_for_admin", 0)),
    }


def _notify_customer_on_admin_reply(session, msg_text):
    """Sender e-post (alltid hvis e-post er oppgitt) + SMS (hvis telefon er
    oppgitt OG twilio er konfigurert) til kunden når admin svarer i chat.
    Skipper varsling hvis kunden er aktiv akkurat nå (har pollet < 30 sek
    siden) — da ser de meldingen i widget uansett."""
    cust = session.get("customer") or {}
    email = (cust.get("email") or "").strip()
    phone = (cust.get("phone") or "").strip()
    notify_pref = cust.get("notify") or {}
    if notify_pref.get("opted_out"):
        return
    # Skip hvis kunden er aktivt på chat-siden
    last_read = session.get("last_customer_read")
    if last_read:
        try:
            dt = datetime.fromisoformat(last_read)
            if (datetime.now() - dt).total_seconds() < 30:
                return
        except Exception:
            pass
    sid = session.get("id")
    name = (cust.get("name") or "Hei").split(" ")[0] or "Hei"
    short = (msg_text or "").strip()
    if len(short) > 240:
        short = short[:237] + "…"
    chat_url = "https://www.havoyet.no/?reopen-chat=" + str(sid)

    if email and notify_pref.get("email", True):
        subj = "Nytt svar i din Havøyet-chat"
        body = (
            f"{name},\n\n"
            f"Du har fått et nytt svar i chatten din med oss:\n\n"
            f'"{short}"\n\n'
            f"Åpne chatten for å svare:\n{chat_url}\n\n"
            f"— Havøyet"
        )
        try:
            _send_admin_mail(email, subj, body)
        except Exception as e:
            print(f"[CHAT] kunde-mail feilet: {e}")

    if phone and notify_pref.get("sms", True):
        sms = f"Havøyet: nytt chat-svar — \"{short[:90]}\"... Åpne: {chat_url}"
        try:
            _send_admin_sms(phone, sms)
        except Exception as e:
            print(f"[CHAT] kunde-sms feilet: {e}")


def _send_human_handoff_mail(session, customer_question):
    """Sender mail til erik+stian når AI escalerer en samtale."""
    name = (session.get("customer") or {}).get("name") or "Ukjent"
    email = (session.get("customer") or {}).get("email") or ""
    sid = session.get("id")
    subj = f"[Havøyet chat] {name} ber om hjelp"
    history_text = ""
    for m in (session.get("messages") or [])[-12:]:
        who = {"customer": name or "Kunde", "ai": "Bot", "admin": "Admin"}.get(m.get("role"), m.get("role"))
        history_text += f"{who}: {m.get('text','')}\n\n"
    body = (
        f"Ny chat-henvendelse fra havoyet.no\n"
        f"{'='*54}\n\n"
        f"Navn:    {name}\n"
        f"E-post:  {email or '(ikke oppgitt)'}\n"
        f"Tid:     {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        f"Kundens spørsmål:\n{'-'*54}\n{customer_question}\n{'-'*54}\n\n"
        f"Samtalehistorikk:\n{'-'*54}\n{history_text}\n"
        f"Åpne i admin: https://www.havoyet.no/admin.html#chat\n"
        f"Session-ID: {sid}\n"
    )
    sent_any = False
    for to in CHAT_HUMAN_RECIPIENTS:
        try:
            ok, _ = _send_admin_mail(to, subj, body)
            if ok:
                sent_any = True
        except Exception as e:
            print(f"[CHAT] mail til {to} feilet: {e}")
    return sent_any


@app.route("/api/chat/sessions", methods=["GET", "POST"])
def api_chat_sessions():
    """GET (admin): liste alle samtaler. POST: opprett ny samtale."""
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        cust = data.get("customer") or {}
        with _chat_lock:
            sid = _chat_new_id()
            now = datetime.now().isoformat()
            sess = {
                "id": sid,
                "customer": {
                    "name":  (cust.get("name") or "").strip()[:80],
                    "email": (cust.get("email") or "").strip()[:120],
                    "phone": (cust.get("phone") or "").strip()[:30],
                },
                "messages": [],
                "status": "open",
                "escalated": False,
                "created_at": now,
                "updated_at": now,
                "unread_for_admin": 0,
                "last_customer_read": now,
            }
            _chat_sessions[sid] = sess
            _save_chat_sessions()
        _archive_chat_event("session_created", {
            "session_id": sid,
            "customer": sess.get("customer", {}),
            "created_at": sess.get("created_at"),
        })
        return jsonify({"ok": True, "session": sess})

    # GET → admin-liste (krever auth)
    user, _ = _user_from_request()
    if not user:
        return jsonify({"ok": False, "error": "Auth påkrevd"}), 401
    with _chat_lock:
        items = [_chat_session_summary(s) for s in _chat_sessions.values()]
    items.sort(key=lambda x: x.get("updated_at") or "", reverse=True)
    return jsonify({"ok": True, "sessions": items})


@app.route("/api/chat/sessions/<sid>", methods=["GET", "DELETE"])
def api_chat_session_one(sid):
    """GET: full samtale (kunde eller admin). DELETE: admin sletter."""
    with _chat_lock:
        sess = _chat_sessions.get(sid)
    if not sess:
        return jsonify({"ok": False, "error": "Samtale ikke funnet"}), 404

    if request.method == "DELETE":
        user, _ = _user_from_request()
        if not user:
            return jsonify({"ok": False, "error": "Auth påkrevd"}), 401
        with _chat_lock:
            snapshot = _chat_sessions.pop(sid, None)
            _save_chat_sessions()
        _archive_chat_event("session_deleted", {
            "session_id": sid,
            "deleted_by": (user or {}).get("email"),
            "snapshot": snapshot,
        })
        return jsonify({"ok": True})

    # GET — om admin er innlogget, marker som lest
    user, _ = _user_from_request()
    out = dict(sess)
    if user:
        with _chat_lock:
            sess["unread_for_admin"] = 0
            sess["last_admin_read"] = datetime.now().isoformat()
            _save_chat_sessions()
        out = dict(sess)
    return jsonify({"ok": True, "session": out})


@app.route("/api/chat/sessions/<sid>/messages", methods=["POST"])
def api_chat_messages(sid):
    """Legg til melding. role=customer|ai|admin. Auto-escalering via flagget
    'escalate' i body, eller hvis AI markerer suggest_human=true. Når admin
    sender melding i en escalert tråd og forrige kunde-melding mangler svar,
    auto-lagrer vi Q&A til lærings-base."""
    data = request.get_json(silent=True) or {}
    role = (data.get("role") or "customer").strip().lower()
    text = (data.get("text") or "").strip()
    if not text:
        return jsonify({"ok": False, "error": "Tom melding"}), 400
    if role not in ("customer", "ai", "admin"):
        return jsonify({"ok": False, "error": "Ugyldig role"}), 400
    if role == "admin":
        user, _ = _user_from_request()
        if not user:
            return jsonify({"ok": False, "error": "Auth påkrevd"}), 401

    with _chat_lock:
        sess = _chat_sessions.get(sid)
        if not sess:
            return jsonify({"ok": False, "error": "Samtale ikke funnet"}), 404

        now = datetime.now().isoformat()
        msg = {
            "id": _chat_new_id(),
            "role": role,
            "text": text[:4000],
            "at": now,
        }
        if role == "ai":
            msg["meta"] = {
                "confidence": data.get("confidence"),
                "suggest_human": bool(data.get("suggest_human")),
            }
        sess.setdefault("messages", []).append(msg)
        sess["updated_at"] = now

        archive_extra = None
        if role == "customer":
            sess["unread_for_admin"] = int(sess.get("unread_for_admin", 0)) + 1
        elif role == "admin":
            sess["unread_for_admin"] = 0
            # Auto-lær: forrige kunde-melding + dette admin-svaret
            prev_customer = None
            for m in reversed(sess["messages"][:-1]):
                if m.get("role") == "customer":
                    prev_customer = m
                    break
            if prev_customer and sess.get("escalated"):
                qna = {
                    "q": prev_customer.get("text", ""),
                    "a": text,
                    "learned_at": now,
                    "session_id": sid,
                }
                _chat_knowledge.append(qna)
                _save_chat_knowledge()
                archive_extra = qna

        _save_chat_sessions()
        out_sess = dict(sess)

    # Append-only arkiv: hver melding lagres for alltid (selv om sesjonen
    # senere slettes fra aktiv state).
    _archive_chat_event("message", {
        "session_id": sid,
        "customer": out_sess.get("customer", {}),
        "escalated": bool(out_sess.get("escalated")),
        "message": msg,
    })
    if archive_extra:
        _archive_chat_event("qna_auto", {
            "session_id": sid,
            "q": archive_extra["q"],
            "a": archive_extra["a"],
            "learned_at": archive_extra["learned_at"],
        })

    # Hvis kunden sender melding i en aktiv escalert tråd, varsle admin på mail
    if role == "customer" and out_sess.get("escalated"):
        try:
            _send_human_handoff_mail(out_sess, text)
        except Exception as e:
            print(f"[CHAT] handoff-mail feilet: {e}")
    # Når admin svarer, varsle kunden via e-post/SMS (hvis ikke aktivt tilstede)
    if role == "admin":
        try:
            _notify_customer_on_admin_reply(out_sess, text)
        except Exception as e:
            print(f"[CHAT] kunde-varsling feilet: {e}")

    return jsonify({"ok": True, "message": msg, "session": out_sess})


@app.route("/api/chat/sessions/<sid>/escalate", methods=["POST"])
def api_chat_escalate(sid):
    """Markér samtale for menneske og send mail til erik+stian."""
    data = request.get_json(silent=True) or {}
    customer_update = data.get("customer") or {}
    with _chat_lock:
        sess = _chat_sessions.get(sid)
        if not sess:
            return jsonify({"ok": False, "error": "Samtale ikke funnet"}), 404
        # Tillat oppdatering av kontaktinfo ved escalering
        if customer_update:
            cust = sess.setdefault("customer", {})
            for k in ("name", "email", "phone"):
                v = (customer_update.get(k) or "").strip()
                if v:
                    cust[k] = v[:120]
        sess["escalated"] = True
        sess["status"] = "needs_human"
        sess["updated_at"] = datetime.now().isoformat()
        sess["unread_for_admin"] = int(sess.get("unread_for_admin", 0)) + 1
        _save_chat_sessions()
        out = dict(sess)

    # Finn siste kunde-spørsmål til mail-body
    last_q = ""
    for m in reversed(out.get("messages") or []):
        if m.get("role") == "customer":
            last_q = m.get("text", "")
            break
    _archive_chat_event("escalated", {
        "session_id": sid,
        "customer": out.get("customer", {}),
        "last_question": last_q,
    })
    try:
        _send_human_handoff_mail(out, last_q)
    except Exception as e:
        print(f"[CHAT] escalate-mail feilet: {e}")
    return jsonify({"ok": True, "session": out})


@app.route("/api/chat/sessions/<sid>/notify", methods=["POST"])
def api_chat_notify_preference(sid):
    """Kunden kan oppdatere kontakt-info og varslings-preferanse uten å eskalere.
    Body: {customer:{name?, email?, phone?}, notify:{email?, sms?, opted_out?}}"""
    data = request.get_json(silent=True) or {}
    cust_in = data.get("customer") or {}
    notify_in = data.get("notify") or {}
    with _chat_lock:
        sess = _chat_sessions.get(sid)
        if not sess:
            return jsonify({"ok": False, "error": "Samtale ikke funnet"}), 404
        cust = sess.setdefault("customer", {})
        for k in ("name", "email", "phone"):
            v = (cust_in.get(k) or "").strip()
            if v:
                cust[k] = v[:120]
        if isinstance(notify_in, dict):
            pref = cust.setdefault("notify", {})
            for k in ("email", "sms", "opted_out"):
                if k in notify_in:
                    pref[k] = bool(notify_in[k])
        sess["updated_at"] = datetime.now().isoformat()
        _save_chat_sessions()
    return jsonify({"ok": True, "customer": cust, "notify": cust.get("notify") or {}})


@app.route("/api/chat/sessions/<sid>/poll", methods=["GET"])
def api_chat_poll(sid):
    """Lett polling-endpoint for kunde-widget — returnerer kun nye admin/AI-meldinger
    og status. Bruker query-param `since` (ISO-tid). Ingen auth."""
    since = request.args.get("since") or ""
    with _chat_lock:
        sess = _chat_sessions.get(sid)
        if not sess:
            return jsonify({"ok": False, "error": "Samtale ikke funnet"}), 404
        new_msgs = []
        for m in sess.get("messages", []):
            if not since or (m.get("at") or "") > since:
                if m.get("role") in ("admin", "ai"):
                    new_msgs.append(m)
        # Marker at kunden er aktiv akkurat nå (slipper push-varsel ved live-svar)
        sess["last_customer_read"] = datetime.now().isoformat()
        _save_chat_sessions()
        return jsonify({
            "ok": True,
            "messages": new_msgs,
            "status": sess.get("status"),
            "escalated": bool(sess.get("escalated")),
            "updated_at": sess.get("updated_at"),
        })


@app.route("/api/chat/knowledge", methods=["GET", "POST"])
def api_chat_knowledge():
    """GET (public): lært Q&A-base — brukes av AI-proxy som kontekst.
       POST (admin): legg til en manuell Q&A-entry."""
    if request.method == "POST":
        user, _ = _user_from_request()
        if not user:
            return jsonify({"ok": False, "error": "Auth påkrevd"}), 401
        data = request.get_json(silent=True) or {}
        q = (data.get("q") or "").strip()
        a = (data.get("a") or "").strip()
        if not q or not a:
            return jsonify({"ok": False, "error": "Mangler q eller a"}), 400
        entry = {
            "id": "k_" + secrets.token_urlsafe(8),
            "q": q[:1000],
            "a": a[:4000],
            "learned_at": datetime.now().isoformat(),
            "session_id": None,
            "source": "manual",
        }
        with _chat_lock:
            _chat_knowledge.append(entry)
            _save_chat_knowledge()
        _archive_chat_event("qna_manual", {
            "entry": entry,
            "saved_by": (user or {}).get("email"),
        })
        return jsonify({"ok": True, "item": entry})

    limit = min(int(request.args.get("limit", "60") or 60), 500)
    with_meta = request.args.get("with_meta") == "1"
    with _chat_lock:
        items = list(_chat_knowledge)[-limit:]
    if with_meta:
        # Sørg for at alle items har id (legacy-entries har det ikke)
        out = []
        changed = False
        with _chat_lock:
            for it in _chat_knowledge:
                if not it.get("id"):
                    it["id"] = "k_" + secrets.token_urlsafe(8)
                    changed = True
                out.append(it)
            if changed:
                _save_chat_knowledge()
        # Returner i nyeste-først rekkefølge for admin-UI
        resp = jsonify({"ok": True, "items": list(reversed(out)), "ts": datetime.now().isoformat()})
    else:
        resp = jsonify({"ok": True, "items": items, "ts": datetime.now().isoformat()})
    # Aldri cache — chat-ai.js skal alltid se siste trening umiddelbart
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@app.route("/api/chat/knowledge/<kid>", methods=["DELETE"])
def api_chat_knowledge_delete(kid):
    user, _ = _user_from_request()
    if not user:
        return jsonify({"ok": False, "error": "Auth påkrevd"}), 401
    with _chat_lock:
        before_items = [dict(k) for k in _chat_knowledge if k.get("id") == kid]
        before = len(_chat_knowledge)
        _chat_knowledge[:] = [k for k in _chat_knowledge if k.get("id") != kid]
        removed = before - len(_chat_knowledge)
        if removed:
            _save_chat_knowledge()
    if removed:
        _archive_chat_event("qna_deleted", {
            "kid": kid,
            "deleted_by": (user or {}).get("email"),
            "snapshot": before_items,
        })
    return jsonify({"ok": True, "removed": removed})


@app.route("/api/chat/archive", methods=["GET"])
def api_chat_archive():
    """Admin-only: les den for-alltid-arkiverte chat-loggen.
    Query-params:
      kind=message|qna_manual|qna_auto|qna_deleted|escalated|session_created|session_deleted
      session_id=<sid>
      limit=<n>          (begrens til de siste n eventer)
      download=1         (returner som NDJSON-fil)
      stats=1            (returner kun antall events + bytes)
    """
    user, _ = _user_from_request()
    if not user:
        return jsonify({"ok": False, "error": "Auth påkrevd"}), 401

    if request.args.get("stats") == "1":
        return jsonify({"ok": True, "stats": _chat_archive_stats()})

    limit = request.args.get("limit")
    try:
        limit = int(limit) if limit else None
    except Exception:
        limit = None
    kind = request.args.get("kind") or None
    session_id = request.args.get("session_id") or None
    items = _read_chat_archive(limit=limit, kind_filter=kind, session_id=session_id)

    if request.args.get("download") == "1":
        body = "\n".join(json.dumps(it, ensure_ascii=False) for it in items) + "\n"
        fname = f"havoyet_chat_archive_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        return Response(
            body,
            mimetype="application/x-ndjson",
            headers={
                "Content-Disposition": f'attachment; filename="{fname}"',
                "Cache-Control": "no-store",
            },
        )

    resp = jsonify({"ok": True, "items": items, "count": len(items),
                    "stats": _chat_archive_stats()})
    resp.headers["Cache-Control"] = "no-store"
    return resp


# Last chat-state ved boot
try:
    _load_chat_state()
    _arc_stats = _chat_archive_stats()
    print(f"[BOOT-WSGI] chat-state lastet: {len(_chat_sessions)} samtaler, "
          f"{len(_chat_knowledge)} Q&A, {_arc_stats['events']} arkiv-events "
          f"({_arc_stats['bytes']} bytes)")
except Exception as _e:
    print(f"[BOOT-WSGI] _load_chat_state feilet: {_e}")


# ── NYHETSBREV-ARKIV ───────────────────────────────────────────────────────────
# Cross-device referansefiler som sendes med hver Claude-prompt i admin →
# Nyhetsbrev. Lagres på serveren slik at alle admin-brukere ser samme arkiv
# uansett hvor de logger seg inn. Filinnhold er tekst (utf-8).
NEWSLETTER_ARCHIVE_FILE = os.path.join(STATE_DIR, "havoyet_newsletter_archive.json")
NEWSLETTER_ARCHIVE_FILE_MAX  = 512 * 1024          # 512 KB per fil
NEWSLETTER_ARCHIVE_TOTAL_MAX = 16 * 1024 * 1024    # 16 MB totalt på serveren
_newsletter_archive = []   # [{id, name, bytes, ts, content}]
_archive_lock = threading.Lock()


def _load_newsletter_archive():
    global _newsletter_archive
    try:
        if os.path.exists(NEWSLETTER_ARCHIVE_FILE):
            with open(NEWSLETTER_ARCHIVE_FILE, "r", encoding="utf-8") as f:
                _newsletter_archive = json.load(f) or []
    except Exception as e:
        print(f"[ARCHIVE] Kunne ikke laste: {e}")
        _newsletter_archive = []


def _save_newsletter_archive():
    try:
        tmp = NEWSLETTER_ARCHIVE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_newsletter_archive, f, ensure_ascii=False)
        os.replace(tmp, NEWSLETTER_ARCHIVE_FILE)
    except Exception as e:
        print(f"[ARCHIVE] save feilet: {e}")


def _archive_total_bytes():
    return sum(int(f.get("bytes") or 0) for f in _newsletter_archive)


@app.route("/api/newsletter-archive", methods=["GET", "POST", "DELETE"])
def api_newsletter_archive():
    if request.method == "GET":
        with _archive_lock:
            return jsonify({"ok": True, "files": list(_newsletter_archive),
                            "total_bytes": _archive_total_bytes()})
    if request.method == "DELETE":
        with _archive_lock:
            _newsletter_archive.clear()
            _save_newsletter_archive()
        return jsonify({"ok": True})
    # POST = legg til én fil
    data = request.get_json(force=True, silent=True) or {}
    name = (data.get("name") or "").strip()
    content = data.get("content")
    if not name or content is None:
        return jsonify({"error": "Mangler 'name' eller 'content'"}), 400
    if not isinstance(content, str):
        return jsonify({"error": "'content' må være tekst"}), 400
    raw_bytes = len(content.encode("utf-8"))
    if raw_bytes > NEWSLETTER_ARCHIVE_FILE_MAX:
        return jsonify({"error": f"Filen er for stor ({raw_bytes} bytes, maks {NEWSLETTER_ARCHIVE_FILE_MAX})"}), 413
    with _archive_lock:
        if _archive_total_bytes() + raw_bytes > NEWSLETTER_ARCHIVE_TOTAL_MAX:
            return jsonify({"error": "Arkivet er fullt — slett noen filer først"}), 413
        entry = {
            "id": "a_" + _uuid.uuid4().hex[:12],
            "name": name[:512],
            "bytes": raw_bytes,
            "ts": int(time.time() * 1000),
            "content": content,
        }
        _newsletter_archive.insert(0, entry)
        _save_newsletter_archive()
    # Returner uten content for å spare bandwidth — klienten har det allerede
    meta = {k: v for k, v in entry.items() if k != "content"}
    return jsonify({"ok": True, "file": meta})


@app.route("/api/newsletter-archive/<file_id>", methods=["DELETE"])
def api_newsletter_archive_delete(file_id):
    with _archive_lock:
        before = len(_newsletter_archive)
        _newsletter_archive[:] = [f for f in _newsletter_archive if f.get("id") != file_id]
        removed = before - len(_newsletter_archive)
        if removed:
            _save_newsletter_archive()
    return jsonify({"ok": True, "removed": removed})


# ── NYHETSBREV-ABONNENTER (Flask = sannhetskilde, Resend = sender) ──────────
# Erstatter MailerLite. Subscribers lagres lokalt; Resend brukes kun til sending.

import re as _re_mod

_EMAIL_RE = _re_mod.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")

# Admin-token for å gate skrive-endepunkter (DELETE, bulk-import). Hvis env ikke
# er satt: tillatt i dev, men logges. Sett ADMIN_API_TOKEN i Render-env i prod.
ADMIN_API_TOKEN = os.environ.get("ADMIN_API_TOKEN", "").strip()


def _normalize_email(raw):
    if not raw or not isinstance(raw, str):
        return None
    e = raw.strip().lower()
    return e if _EMAIL_RE.match(e) else None


def _is_admin_request():
    # 1) Innlogget bruker med rolle "admin" (admin.html via auth-bearer)
    try:
        user, _ = _user_from_request()
        if user and (user.get("role") or "").lower() == "admin":
            return True
    except Exception:
        pass
    # 2) Eksplisitt X-Admin-Token-header (scripts/cron uten user-session)
    if ADMIN_API_TOKEN:
        auth = (request.headers.get("X-Admin-Token") or "").strip()
        if auth and _hmac_mod.compare_digest(auth, ADMIN_API_TOKEN):
            return True
        return False
    # 3) Hvis verken auth eller ADMIN_API_TOKEN er satt — åpen (dev-modus)
    return True


def _find_subscriber(email):
    email = _normalize_email(email)
    if not email:
        return None
    for s in _subscribers:
        if s.get("email") == email:
            return s
    return None


def _is_active_subscriber(email):
    s = _find_subscriber(email)
    return bool(s and s.get("status") == "active")


@app.route("/api/subscribers", methods=["GET"])
def api_subscribers_list():
    """Lister abonnenter. Filter: ?status=active|unsubscribed|bounced (default: alle)."""
    status_filter = (request.args.get("status") or "").strip().lower()
    rows = _subscribers
    if status_filter in ("active", "unsubscribed", "bounced"):
        rows = [s for s in rows if s.get("status") == status_filter]
    counts = {
        "total": len(_subscribers),
        "active": sum(1 for s in _subscribers if s.get("status") == "active"),
        "unsubscribed": sum(1 for s in _subscribers if s.get("status") == "unsubscribed"),
        "bounced": sum(1 for s in _subscribers if s.get("status") == "bounced"),
    }
    return jsonify({"ok": True, "subscribers": rows, "counts": counts})


@app.route("/api/subscribers/subscribe", methods=["POST"])
def api_subscribers_subscribe():
    """Public sign-up endpoint. Idempotent — re-subscribe reaktiverer."""
    data = request.get_json(force=True, silent=True) or {}
    email = _normalize_email(data.get("email"))
    if not email:
        return jsonify({"error": "Ugyldig e-postadresse"}), 400
    navn = (data.get("navn") or data.get("name") or "").strip()[:120]
    kilde = (data.get("kilde") or data.get("source") or "website").strip()[:32]
    tags = data.get("tags") or []
    if not isinstance(tags, list):
        tags = []
    now = datetime.now().isoformat()
    existing = _find_subscriber(email)
    if existing:
        existing["status"] = "active"
        existing["updated_at"] = now
        existing["unsubscribed_at"] = None
        if navn and not existing.get("navn"):
            existing["navn"] = navn
        if tags:
            existing["tags"] = sorted(set((existing.get("tags") or []) + tags))
        _save_sync_state()
        return jsonify({"ok": True, "subscriber": existing, "reactivated": True})
    new = {
        "id": "s_" + _uuid.uuid4().hex[:12],
        "email": email,
        "navn": navn,
        "status": "active",
        "kilde": kilde,
        "tags": sorted(set(tags)) if tags else [],
        "created_at": now,
        "updated_at": now,
        "unsubscribed_at": None,
    }
    _subscribers.append(new)
    _save_sync_state()
    return jsonify({"ok": True, "subscriber": new, "reactivated": False})


@app.route("/api/subscribers/unsubscribe", methods=["POST"])
def api_subscribers_unsubscribe():
    """Public unsubscribe (via e-post). Senere kan vi legge til signert token-lenke."""
    data = request.get_json(force=True, silent=True) or {}
    email = _normalize_email(data.get("email"))
    if not email:
        return jsonify({"error": "Ugyldig e-postadresse"}), 400
    s = _find_subscriber(email)
    if not s:
        return jsonify({"ok": True, "found": False})
    s["status"] = "unsubscribed"
    now = datetime.now().isoformat()
    s["unsubscribed_at"] = now
    s["updated_at"] = now
    _save_sync_state()
    return jsonify({"ok": True, "found": True})


@app.route("/api/subscribers/check", methods=["GET"])
def api_subscribers_check():
    """Sjekk om en e-post er aktiv abonnent. Brukes av /api/auth/me."""
    email = _normalize_email(request.args.get("email"))
    if not email:
        return jsonify({"ok": True, "is_subscriber": False})
    return jsonify({"ok": True, "is_subscriber": _is_active_subscriber(email)})


@app.route("/api/subscribers/<path:email>", methods=["DELETE", "PATCH"])
def api_subscribers_modify(email):
    if not _is_admin_request():
        return jsonify({"error": "Mangler admin-token"}), 401
    email = _normalize_email(email)
    if not email:
        return jsonify({"error": "Ugyldig e-postadresse"}), 400
    s = _find_subscriber(email)
    if not s:
        return jsonify({"error": "Ikke funnet"}), 404
    if request.method == "DELETE":
        _subscribers.remove(s)
        _save_sync_state()
        return jsonify({"ok": True, "deleted": True})
    # PATCH
    data = request.get_json(force=True, silent=True) or {}
    now = datetime.now().isoformat()
    for field in ("navn", "status", "kilde"):
        if field in data:
            s[field] = data[field]
    if "tags" in data and isinstance(data["tags"], list):
        s["tags"] = sorted(set(data["tags"]))
    s["updated_at"] = now
    _save_sync_state()
    return jsonify({"ok": True, "subscriber": s})


@app.route("/api/subscribers/bulk-import", methods=["POST"])
def api_subscribers_bulk_import():
    """Importer flere abonnenter på en gang. Brukes til MailerLite-migrering.
    Body: { subscribers: [{ email, navn?, kilde?, status?, mailerlite_id? }, ...] }
    Idempotent: e-poster som allerede finnes oppdateres (status/navn merges)."""
    if not _is_admin_request():
        return jsonify({"error": "Mangler admin-token"}), 401
    data = request.get_json(force=True, silent=True) or {}
    rows = data.get("subscribers") or []
    if not isinstance(rows, list):
        return jsonify({"error": "subscribers må være en liste"}), 400
    now = datetime.now().isoformat()
    added = 0
    updated = 0
    skipped = 0
    for row in rows:
        if not isinstance(row, dict):
            skipped += 1
            continue
        email = _normalize_email(row.get("email"))
        if not email:
            skipped += 1
            continue
        navn = (row.get("navn") or row.get("name") or "").strip()[:120]
        kilde = (row.get("kilde") or row.get("source") or "mailerlite-migration").strip()[:32]
        status = (row.get("status") or "active").strip().lower()
        if status not in ("active", "unsubscribed", "bounced"):
            status = "active"
        ml_id = (row.get("mailerlite_id") or row.get("id") or "").strip()
        existing = _find_subscriber(email)
        if existing:
            existing["updated_at"] = now
            if navn and not existing.get("navn"):
                existing["navn"] = navn
            if ml_id and not existing.get("mailerlite_id"):
                existing["mailerlite_id"] = ml_id
            # ikke overskriv status hvis allerede aktiv lokalt
            if existing.get("status") != "active" and status == "active":
                existing["status"] = "active"
                existing["unsubscribed_at"] = None
            updated += 1
        else:
            _subscribers.append({
                "id": "s_" + _uuid.uuid4().hex[:12],
                "email": email,
                "navn": navn,
                "status": status,
                "kilde": kilde,
                "tags": [],
                "created_at": now,
                "updated_at": now,
                "unsubscribed_at": None if status == "active" else now,
                "mailerlite_id": ml_id or None,
            })
            added += 1
    _save_sync_state()
    return jsonify({
        "ok": True,
        "added": added,
        "updated": updated,
        "skipped": skipped,
        "total_after": len(_subscribers),
    })


# ── RABATTER (produkt-rabatter, ofte knyttet til nyhetsbrev-sendinger) ──────
# Brukes til at innloggede nyhetsbrev-abonnenter ser rabattert pris på spesifikke
# produkter i en gitt tidsperiode. Aktiveres automatisk når et nyhetsbrev sendes,
# eller manuelt via admin "Rabatter"-fanen.

_DATE_RE = _re_mod.compile(r"^\d{4}-\d{2}-\d{2}$")


def _today_str():
    return datetime.now().strftime("%Y-%m-%d")


def _valid_date(s):
    return bool(s and isinstance(s, str) and _DATE_RE.match(s))


def _is_discount_currently_active(d):
    if not d.get("aktiv", True):
        return False
    today = _today_str()
    start = d.get("start") or ""
    slutt = d.get("slutt") or ""
    if start and today < start:
        return False
    if slutt and today > slutt:
        return False
    return True


def _active_discounts_for(user_email=None):
    """Returnerer rabatter som faktisk gjelder akkurat nå. Filtrerer på
    kun_nyhetsbrev hvis user_email enten er None eller ikke aktiv abonnent."""
    is_sub = bool(user_email and _is_active_subscriber(user_email))
    out = []
    for d in _discounts:
        if not _is_discount_currently_active(d):
            continue
        if d.get("kun_nyhetsbrev") and not is_sub:
            continue
        out.append(d)
    return out


@app.route("/api/discounts/active", methods=["GET"])
def api_discounts_active():
    """Returnerer aktive rabatter som faktisk gjelder for den som kaller.
    Sender ?email=... for å få med 'kun_nyhetsbrev'-rabatter for en gitt bruker.
    Brukes av nettsiden ved produktlisting."""
    email = _normalize_email(request.args.get("email"))
    rows = _active_discounts_for(email)
    return jsonify({
        "ok": True,
        "discounts": rows,
        "is_subscriber": bool(email and _is_active_subscriber(email)),
    })


@app.route("/api/discounts", methods=["GET", "POST"])
def api_discounts_root():
    if request.method == "GET":
        # Admin/intern: liste ALLE rabatter (også utløpte/inaktive)
        return jsonify({"ok": True, "discounts": list(_discounts)})
    if not _is_admin_request():
        return jsonify({"error": "Mangler admin-token"}), 401
    data = request.get_json(force=True, silent=True) or {}
    handle = (data.get("handle") or "").strip()
    if not handle:
        return jsonify({"error": "Mangler produkt-handle"}), 400
    try:
        prosent = float(data.get("prosent") or 0)
    except (TypeError, ValueError):
        return jsonify({"error": "Ugyldig prosent"}), 400
    if prosent <= 0 or prosent >= 100:
        return jsonify({"error": "Prosent må være mellom 1 og 99"}), 400
    start = (data.get("start") or _today_str()).strip()
    slutt = (data.get("slutt") or "").strip()
    if not _valid_date(start):
        return jsonify({"error": "Ugyldig start-dato (forventer YYYY-MM-DD)"}), 400
    if slutt and not _valid_date(slutt):
        return jsonify({"error": "Ugyldig slutt-dato (forventer YYYY-MM-DD)"}), 400
    now = datetime.now().isoformat()
    new = {
        "id": "d_" + _uuid.uuid4().hex[:12],
        "handle": handle,
        "prosent": prosent,
        "start": start,
        "slutt": slutt or None,
        "beskrivelse": (data.get("beskrivelse") or "").strip()[:200],
        "kun_nyhetsbrev": bool(data.get("kun_nyhetsbrev", True)),
        "aktiv": bool(data.get("aktiv", True)),
        "kilde_newsletter_id": (data.get("kilde_newsletter_id") or None),
        "created_at": now,
        "updated_at": now,
    }
    _discounts.append(new)
    _save_sync_state()
    return jsonify({"ok": True, "discount": new})


@app.route("/api/discounts/<discount_id>", methods=["PATCH", "DELETE"])
def api_discounts_modify(discount_id):
    if not _is_admin_request():
        return jsonify({"error": "Mangler admin-token"}), 401
    d = next((x for x in _discounts if x.get("id") == discount_id), None)
    if not d:
        return jsonify({"error": "Ikke funnet"}), 404
    if request.method == "DELETE":
        _discounts.remove(d)
        _save_sync_state()
        return jsonify({"ok": True, "deleted": True})
    data = request.get_json(force=True, silent=True) or {}
    now = datetime.now().isoformat()
    if "prosent" in data:
        try:
            p = float(data["prosent"])
            if 0 < p < 100:
                d["prosent"] = p
        except (TypeError, ValueError):
            pass
    for f in ("handle", "beskrivelse"):
        if f in data and isinstance(data[f], str):
            d[f] = data[f].strip()
    for f in ("start", "slutt"):
        if f in data:
            v = (data[f] or "").strip()
            if not v or _valid_date(v):
                d[f] = v or None
    for f in ("kun_nyhetsbrev", "aktiv"):
        if f in data:
            d[f] = bool(data[f])
    d["updated_at"] = now
    _save_sync_state()
    return jsonify({"ok": True, "discount": d})


@app.route("/api/discounts/bulk", methods=["POST"])
def api_discounts_bulk():
    """Aktiverer flere rabatter samtidig — brukes når nyhetsbrev sendes.
    Body: { newsletter_id, rabatter: [{handle, prosent, start, slutt, beskrivelse}, ...] }
    Alle får 'kun_nyhetsbrev': true automatisk."""
    if not _is_admin_request():
        return jsonify({"error": "Mangler admin-token"}), 401
    data = request.get_json(force=True, silent=True) or {}
    newsletter_id = (data.get("newsletter_id") or "").strip() or None
    rows = data.get("rabatter") or []
    if not isinstance(rows, list):
        return jsonify({"error": "rabatter må være liste"}), 400
    now = datetime.now().isoformat()
    today = _today_str()
    created = []
    skipped = []
    for row in rows:
        if not isinstance(row, dict):
            skipped.append({"reason": "not-object"})
            continue
        handle = (row.get("handle") or "").strip()
        try:
            prosent = float(row.get("prosent") or 0)
        except (TypeError, ValueError):
            prosent = 0
        if not handle or prosent <= 0 or prosent >= 100:
            skipped.append({"handle": handle, "reason": "invalid-input"})
            continue
        start = (row.get("start") or today).strip()
        slutt = (row.get("slutt") or "").strip()
        if not _valid_date(start):
            start = today
        if slutt and not _valid_date(slutt):
            slutt = ""
        new = {
            "id": "d_" + _uuid.uuid4().hex[:12],
            "handle": handle,
            "prosent": prosent,
            "start": start,
            "slutt": slutt or None,
            "beskrivelse": (row.get("beskrivelse") or "Nyhetsbrev-rabatt").strip()[:200],
            "kun_nyhetsbrev": True,
            "aktiv": True,
            "kilde_newsletter_id": newsletter_id,
            "created_at": now,
            "updated_at": now,
        }
        _discounts.append(new)
        created.append(new)
    _save_sync_state()
    return jsonify({"ok": True, "created": len(created), "skipped": len(skipped), "discounts": created})


# ── BACKUP / GJENOPPRETT ────────────────────────────────────────────────────
# Sikkerhetsnett mens persistent disk ikke er montert på Render: admin kan
# laste ned hele state-en som JSON og laste den opp igjen om containeren ble
# nullstilt (f.eks. etter deploy). Lagrer alle samme felter som
# _save_sync_state() skriver til disk.

def _full_state_dict():
    """Full snapshot av alt som lagres i sync-state."""
    return {
        "schema_version":      1,
        "exported_at":         datetime.now().isoformat(),
        "manual_orders":          _manual_orders,
        "hidden_orders":          _hidden_orders,
        "overrides":              _overrides,
        "packing_state":          _packing_state,
        "order_notes":            _order_notes,
        "product_overrides":      _product_overrides,
        "reviews":                _reviews,
        "customer_favorites":     _customer_favorites,
        "admin_notifiers":        _admin_notifiers,
        "customers":              _customers,
        "vipps_imported_payments": _vipps_imported_payments,
        "card_payments_imported":  _card_payments_imported,
        "auth_users":             _auth_users,
        "auth_sessions":          _auth_sessions,
        "subscribers":            _subscribers,
        "discounts":              _discounts,
    }

@app.route("/api/admin/backup", methods=["GET"])
def api_admin_backup():
    """Returnerer hele sync-state som JSON (admin laster ned dette som backup)."""
    payload = _full_state_dict()
    payload["counts"] = {
        "manual_orders":     len(_manual_orders),
        "customers":         len(_customers),
        "product_overrides": len(_product_overrides),
        "reviews":           len(_reviews),
        "auth_users":        len(_auth_users),
    }
    resp = jsonify(payload)
    resp.headers["Cache-Control"] = "no-store"
    return resp

@app.route("/api/admin/restore", methods=["POST"])
def api_admin_restore():
    """Gjenoppretter sync-state fra et backup-JSON. Body = output fra /api/admin/backup.
    Skriver bare felter som faktisk finnes i body (delvis-restore støttes)."""
    global _manual_orders, _hidden_orders, _overrides, _packing_state, _order_notes
    global _product_overrides, _reviews, _customer_favorites, _admin_notifiers
    global _customers, _vipps_imported_payments, _card_payments_imported
    global _auth_users, _auth_sessions

    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        return jsonify({"ok": False, "error": "Forventer JSON-objekt"}), 400

    restored = {}
    def _maybe(field, current_default):
        if field in data and data[field] is not None:
            restored[field] = (
                len(data[field]) if hasattr(data[field], "__len__") else 1
            )
            return data[field]
        return current_default

    _manual_orders         = _maybe("manual_orders",          _manual_orders)
    _hidden_orders         = _maybe("hidden_orders",          _hidden_orders)
    _overrides             = _maybe("overrides",              _overrides)
    _packing_state         = _maybe("packing_state",          _packing_state)
    _order_notes           = _maybe("order_notes",            _order_notes)
    _product_overrides     = _maybe("product_overrides",      _product_overrides)
    _reviews               = _maybe("reviews",                _reviews)
    _customer_favorites    = _maybe("customer_favorites",     _customer_favorites)
    _admin_notifiers       = _maybe("admin_notifiers",        _admin_notifiers)
    _customers             = _maybe("customers",              _customers)
    _vipps_imported_payments = _maybe("vipps_imported_payments", _vipps_imported_payments)
    _card_payments_imported  = _maybe("card_payments_imported",  _card_payments_imported)
    _auth_users            = _maybe("auth_users",             _auth_users)
    _auth_sessions         = _maybe("auth_sessions",          _auth_sessions)

    _save_sync_state()
    return jsonify({"ok": True, "restored": restored})


# Last arkiv ved boot
try:
    _load_newsletter_archive()
    print(f"[BOOT-WSGI] nyhetsbrev-arkiv lastet: {len(_newsletter_archive)} filer ({_archive_total_bytes()} bytes)")
except Exception as _e:
    print(f"[BOOT-WSGI] _load_newsletter_archive feilet: {_e}")


# ── WSGI-bootstrap ────────────────────────────────────────────────────────────
# Render kjører `gunicorn app:app`, så __main__-blokken under kjøres ALDRI i
# produksjon. Last sync-state og seed-brukere ved import.
try:
    _load_sync_state()
    print(f"[BOOT-WSGI] sync-state lastet, {len(_auth_users)} auth-brukere")
except Exception as _e:
    print(f"[BOOT-WSGI] _load_sync_state feilet: {_e}")

try:
    _load_analytics()
except Exception as _e:
    print(f"[BOOT-WSGI] _load_analytics feilet: {_e}")

try:
    _load_replays()
except Exception as _e:
    print(f"[BOOT-WSGI] _load_replays feilet: {_e}")

try:
    _load_subscriptions()
except Exception as _e:
    print(f"[BOOT-WSGI] _load_subscriptions feilet: {_e}")

# ── ABAX ETA-integrasjon (kunder ser "X minutter til levering") ──────────
try:
    from tracking_routes import register_tracking
    register_tracking(
        app,
        manual_orders_ref=lambda: _manual_orders,
        save_state=_save_sync_state,
        state_dir=STATE_DIR,
        admin_check=_user_from_request,
    )
    print("[BOOT] Tracking-routes registrert (ABAX)")
except Exception as _e:
    print(f"[BOOT] Tracking-routes IKKE registrert: {_e}")


# ── ETIKETTSKRIVER (Brother QL-1110NWB) ───────────────────────────────────────
# To moduser:
#   1) Lokal direkte-print: hvis denne hosten har CUPS + PRINTER_NAME, sendes
#      jobben rett til lp. Brukes på Pi-en.
#   2) Kø-modus: hvis CUPS ikke er tilgjengelig (Render), legges jobben i en
#      kø som Pi-en henter via /api/print/queue. Pi-side worker er
#      print_worker.py (kjører som systemd-tjeneste på Pi).
import base64 as _b64
import shutil as _shutil
import subprocess as _sp
import tempfile as _tempfile
import uuid as _uuid
import threading as _threading

LABEL_TEMPLATES_FILE = os.path.join(STATE_DIR, "label_templates.json")
PRINT_QUEUE_FILE     = os.path.join(STATE_DIR, "print_queue.json")
PRINT_QUEUE_DIR      = os.path.join(STATE_DIR, "print_queue")
PRINT_HEARTBEAT_FILE = os.path.join(STATE_DIR, "print_worker_heartbeat.json")
PRINTER_NAME         = os.environ.get("PRINTER_NAME", "brother-ql1110")
# Bearer-token for Pi-worker. Hvis ikke satt → åpen (greit for utvikling, men
# bør settes på Render i produksjon).
PRINT_WORKER_TOKEN   = os.environ.get("PRINT_WORKER_TOKEN", "")
_print_queue_lock    = _threading.Lock()

try:
    os.makedirs(PRINT_QUEUE_DIR, exist_ok=True)
except Exception:
    pass

def _label_templates_load():
    if not os.path.exists(LABEL_TEMPLATES_FILE):
        return []
    try:
        with open(LABEL_TEMPLATES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[PRINT] Kunne ikke lese label-maler: {e}")
        return []

def _label_templates_save(templates):
    try:
        with open(LABEL_TEMPLATES_FILE, "w", encoding="utf-8") as f:
            json.dump(templates[:200], f, ensure_ascii=False)  # cap til 200
    except Exception as e:
        print(f"[PRINT] Kunne ikke lagre label-maler: {e}")

def _printer_status():
    """Returner (ready: bool, info: dict). Sjekker CUPS + at PRINTER_NAME finnes."""
    lp = _shutil.which("lp")
    lpstat = _shutil.which("lpstat")
    if not lp or not lpstat:
        return False, {
            "ready": False, "cups_available": False,
            "message": "CUPS er ikke installert på denne hosten",
        }
    try:
        out = _sp.run([lpstat, "-p", PRINTER_NAME], capture_output=True, text=True, timeout=4)
        if out.returncode != 0:
            return False, {
                "ready": False, "cups_available": True, "printer": PRINTER_NAME,
                "message": f"Skriver «{PRINTER_NAME}» finnes ikke i CUPS",
            }
        ok = "disabled" not in out.stdout.lower()
        return ok, {
            "ready": ok, "cups_available": True, "printer": PRINTER_NAME,
            "message": "Klar" if ok else "Skriver er deaktivert i CUPS",
        }
    except Exception as e:
        return False, {
            "ready": False, "cups_available": True, "printer": PRINTER_NAME,
            "message": f"lpstat-feil: {e}",
        }


# ── Kø-håndtering (brukes når CUPS ikke er tilgjengelig på denne hosten) ─────
def _print_queue_load():
    if not os.path.exists(PRINT_QUEUE_FILE):
        return []
    try:
        with open(PRINT_QUEUE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []

def _print_queue_save(jobs):
    try:
        with open(PRINT_QUEUE_FILE, "w", encoding="utf-8") as f:
            json.dump(jobs[-500:], f, ensure_ascii=False)  # cap til 500 jobs
    except Exception as e:
        print(f"[PRINT] Kunne ikke lagre print-kø: {e}")

def _print_queue_enqueue(raw_png, product, order_id):
    job_id = _uuid.uuid4().hex
    png_path = os.path.join(PRINT_QUEUE_DIR, f"{job_id}.png")
    with open(png_path, "wb") as f:
        f.write(raw_png)
    job = {
        "id":         job_id,
        "status":     "pending",          # pending | printing | done | failed
        "product":    product or "—",
        "order_id":   order_id or "",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "attempts":   0,
        "error":      None,
    }
    with _print_queue_lock:
        jobs = _print_queue_load()
        jobs.append(job)
        _print_queue_save(jobs)
    return job

def _print_queue_update(job_id, **fields):
    with _print_queue_lock:
        jobs = _print_queue_load()
        for j in jobs:
            if j["id"] == job_id:
                j.update(fields)
                j["updated_at"] = datetime.now().isoformat()
                break
        _print_queue_save(jobs)

def _print_worker_authed():
    """Sjekk at request har korrekt Bearer-token (hvis token er satt)."""
    if not PRINT_WORKER_TOKEN:
        return True  # auth deaktivert
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return False
    return auth.split(" ", 1)[1].strip() == PRINT_WORKER_TOKEN

def _print_lp_print(raw_png, job_id):
    """Skriv PNG til midlertidig fil og send via lp. Returner (ok, output, err)."""
    fpath = os.path.join(_tempfile.gettempdir(), f"havoyet_label_{job_id}.png")
    try:
        with open(fpath, "wb") as f:
            f.write(raw_png)
        out = _sp.run(["lp", "-d", PRINTER_NAME, fpath],
                      capture_output=True, text=True, timeout=15)
        if out.returncode != 0:
            return False, out.stdout.strip(), (out.stderr.strip() or out.stdout.strip() or "lp feilet")
        return True, out.stdout.strip(), None
    finally:
        try: os.remove(fpath)
        except Exception: pass

def _print_worker_heartbeat_read():
    if not os.path.exists(PRINT_HEARTBEAT_FILE):
        return None
    try:
        with open(PRINT_HEARTBEAT_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return None


@app.route("/api/print/status")
def api_print_status():
    """Statussjekk. To moduser:
      - Lokal direkte-print (CUPS funnet) → som før
      - Kø-modus → rapporter kø-størrelse + worker-heartbeat
    """
    ready, info = _printer_status()
    info["mode"] = "direct" if info.get("cups_available") else "queue"
    if info["mode"] == "queue":
        jobs = _print_queue_load()
        pending = [j for j in jobs if j.get("status") == "pending"]
        info["pending"] = len(pending)
        hb = _print_worker_heartbeat_read()
        if hb:
            info["worker"] = hb
            try:
                last = datetime.fromisoformat(hb.get("ts", ""))
                age = (datetime.now() - last).total_seconds()
                info["worker_age_s"] = round(age, 1)
                info["ready"] = age < 60  # worker ansett som klar hvis hb yngre enn 60s
                info["message"] = "Worker tilkoblet" if info["ready"] else f"Worker stille i {int(age)}s"
            except Exception:
                info["ready"] = False
                info["message"] = "Worker-heartbeat ugyldig"
        else:
            info["ready"] = False
            info["message"] = "Ingen worker tilkoblet"
    return jsonify(info)


@app.route("/api/print/label", methods=["POST"])
def api_print_label():
    """Tar imot base64-PNG fra ptouch.html. Hvis denne hosten har CUPS, skrives
    rett til lp. Hvis ikke (Render), legges jobben i kø for Pi-worker.
    """
    data = request.get_json(force=True, silent=True) or {}
    png = data.get("png") or ""
    if "," in png:
        png = png.split(",", 1)[1]
    if not png:
        return jsonify({"ok": False, "error": "Mangler png-felt"}), 400
    try:
        raw = _b64.b64decode(png)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Ugyldig base64: {e}"}), 400
    if len(raw) > 4 * 1024 * 1024:
        return jsonify({"ok": False, "error": "Bilde for stort (>4 MB)"}), 413

    product = data.get("product") or "—"
    ordr    = data.get("order_id") or ""
    ready, _info = _printer_status()

    if ready:
        # Direkte-print (Pi)
        job_id = _uuid.uuid4().hex[:8]
        ok, stdout, err = _print_lp_print(raw, job_id)
        if not ok:
            return jsonify({"ok": False, "error": f"lp feilet: {err}"}), 500
        print(f"[PRINT-DIRECT] {product} (ordre {ordr or 'manuell'}) → {PRINTER_NAME} :: {stdout}")
        return jsonify({"ok": True, "mode": "direct", "message": "Sendt til skriver",
                        "lp_output": stdout, "job": job_id})

    # Kø-modus (Render → Pi-worker)
    job = _print_queue_enqueue(raw, product, ordr)
    print(f"[PRINT-QUEUE] {product} (ordre {ordr or 'manuell'}) → kø-id {job['id']}")
    hb = _print_worker_heartbeat_read()
    worker_active = False
    if hb:
        try:
            last = datetime.fromisoformat(hb.get("ts", ""))
            worker_active = (datetime.now() - last).total_seconds() < 60
        except Exception:
            pass
    return jsonify({
        "ok": True, "mode": "queue", "job": job["id"],
        "message": ("Lagt i kø — skrives ut snart" if worker_active
                    else "Lagt i kø, men ingen worker tilkoblet — sjekk at print_worker.py kjører på Pi"),
        "worker_active": worker_active,
    })


# ── Worker-endepunkter (Pi henter jobber herfra) ─────────────────────────────
@app.route("/api/print/queue")
def api_print_queue():
    """Pi-worker henter ventende jobber. Krever Bearer-token hvis satt."""
    if not _print_worker_authed():
        return jsonify({"error": "Unauthorized"}), 401
    limit = int(request.args.get("limit", 5))
    jobs = _print_queue_load()
    pending = [j for j in jobs if j.get("status") == "pending"][:limit]
    return jsonify({
        "jobs":      [{k: v for k, v in j.items() if k != "_png"} for j in pending],
        "total":     len(jobs),
        "pending":   sum(1 for j in jobs if j.get("status") == "pending"),
    })


@app.route("/api/print/queue/<job_id>/png")
def api_print_queue_png(job_id):
    """Pi-worker henter PNG-binæren for en spesifikk jobb."""
    if not _print_worker_authed():
        return jsonify({"error": "Unauthorized"}), 401
    # Sikre mot path-traversal: kun hex-id tillatt
    if not all(c in "0123456789abcdef" for c in job_id):
        return jsonify({"error": "Ugyldig job_id"}), 400
    png_path = os.path.join(PRINT_QUEUE_DIR, f"{job_id}.png")
    if not os.path.exists(png_path):
        return jsonify({"error": "Ikke funnet"}), 404
    return send_from_directory(PRINT_QUEUE_DIR, f"{job_id}.png", mimetype="image/png")


@app.route("/api/print/queue/<job_id>/ack", methods=["POST"])
def api_print_queue_ack(job_id):
    """Pi-worker kvitterer at en jobb er ferdig (eller feilet)."""
    if not _print_worker_authed():
        return jsonify({"error": "Unauthorized"}), 401
    payload = request.get_json(force=True, silent=True) or {}
    success = bool(payload.get("success"))
    error   = payload.get("error") or None
    new_status = "done" if success else "failed"
    _print_queue_update(job_id, status=new_status, error=error,
                        attempts=int(payload.get("attempts", 1)))
    # Slett PNG når done for å spare disk
    if success:
        try: os.remove(os.path.join(PRINT_QUEUE_DIR, f"{job_id}.png"))
        except Exception: pass
    return jsonify({"ok": True, "status": new_status})


@app.route("/api/print/worker/heartbeat", methods=["POST"])
def api_print_worker_heartbeat():
    if not _print_worker_authed():
        return jsonify({"error": "Unauthorized"}), 401
    payload = request.get_json(force=True, silent=True) or {}
    hb = {
        "ts":       datetime.now().isoformat(),
        "host":     payload.get("host", "unknown"),
        "printer":  payload.get("printer", PRINTER_NAME),
        "version":  payload.get("version", "?"),
    }
    try:
        with open(PRINT_HEARTBEAT_FILE, "w") as f:
            json.dump(hb, f)
    except Exception:
        pass
    return jsonify({"ok": True, "ts": hb["ts"]})


@app.route("/api/print/queue/clear", methods=["POST"])
def api_print_queue_clear():
    """Admin: slett ferdige/feilede jobber. (Pending beholdes.)"""
    with _print_queue_lock:
        jobs = _print_queue_load()
        kept = [j for j in jobs if j.get("status") == "pending"]
        removed = len(jobs) - len(kept)
        _print_queue_save(kept)
    return jsonify({"ok": True, "removed": removed, "kept": len(kept)})


@app.route("/api/print/templates", methods=["GET", "POST"])
def api_print_templates():
    if request.method == "GET":
        return jsonify(_label_templates_load())
    payload = request.get_json(force=True, silent=True) or {}
    if not payload.get("id") or not payload.get("product"):
        return jsonify({"ok": False, "error": "Mangler id eller product"}), 400
    items = _label_templates_load()
    items = [t for t in items if t.get("id") != payload["id"]]
    items.insert(0, payload)
    _label_templates_save(items)
    return jsonify({"ok": True, "count": len(items)})


@app.route("/api/print/templates/<tmpl_id>", methods=["DELETE"])
def api_print_template_delete(tmpl_id):
    items = _label_templates_load()
    new_items = [t for t in items if t.get("id") != tmpl_id]
    if len(new_items) == len(items):
        return jsonify({"ok": False, "error": "Ikke funnet"}), 404
    _label_templates_save(new_items)
    return jsonify({"ok": True, "count": len(new_items)})


if __name__ == "__main__":
    # Last sync-state (pakkingstilstand, manuelle ordre, etc.)
    _load_sync_state()
    _load_analytics()
    _load_replays()
    _load_subscriptions()

    # Last prisliste fra disk
    if os.path.exists(PRISLISTE_FILE):
        try:
            with open(PRISLISTE_FILE, "r", encoding="utf-8") as f:
                _prisliste.update(json.load(f))
            print(f"Lastet prisliste: {len(_prisliste.get('items', []))} varelinjer fra disk")
        except Exception:
            pass

    print(f"Havøyet backend starter — http://0.0.0.0:{PORT}")
    print(f"Kilde: ny.havoyet.no/kasse → /api/orders/new → _manual_orders")

    is_cloud = os.environ.get("RENDER") or os.environ.get("RAILWAY_ENVIRONMENT")
    app.run(host="0.0.0.0", port=PORT, debug=not is_cloud, use_reloader=not is_cloud)
