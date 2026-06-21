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
try:
    from pricing_validation import validate_order_payment as _validate_order_payment
except Exception:
    _validate_order_payment = None  # best-effort: betaling slippes gjennom hvis modul mangler
try:
    from zoneinfo import ZoneInfo
    _OSLO_TZ = ZoneInfo("Europe/Oslo")
except Exception:
    _OSLO_TZ = timezone(timedelta(hours=1))  # vinter-fallback, ikke perfekt men trygt


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
VIPPS_PAYMENTS_FILE     = os.path.join(STATE_DIR, "havoyet_vipps_payments.json")  # persistent disk, ikke /tmp (overlevde ikke Render-restart)

# ── STRIPE (kort-betaling, separat fra Vipps) ────────────────────────────────
STRIPE_SECRET_KEY      = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET  = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
# Publishable key er trygt å eksponere til frontend. Render-env-varen kan ha
# blandet casing; aksepter flere varianter.
STRIPE_PUBLISHABLE_KEY = (os.environ.get("STRIPE_PUBLISHABLE_KEY")
                         or os.environ.get("STRIPE_Publishable_KEY")
                         or os.environ.get("STRIPE_PUBLIC_KEY")
                         or "")
STRIPE_PAYMENTS_FILE   = os.path.join(STATE_DIR, "havoyet_stripe_payments.json")  # persistent disk, ikke /tmp (overlevde ikke Render-restart)
try:
    import stripe as _stripe
    if STRIPE_SECRET_KEY:
        _stripe.api_key = STRIPE_SECRET_KEY
    # Robusthet: kort timeout + retries så en treg Stripe-respons ikke henger en
    # gunicorn-worker i default 80s (kan ellers kvele hele API-et).
    try:
        _stripe.max_network_retries = 2
        _stripe.default_http_client = _stripe.http_client.RequestsClient(timeout=20)
    except Exception:
        pass
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
            "unitLabel":    v.get("unitLabel") or "",
            "tilbehorValgt": v.get("tilbehorValgt") or v.get("tilbehor_valgt") or [],
            "boxSelection":  v.get("boxSelection") or [],
            # Eksakt valgt kasse-innhold fra storefront (total = perPerson × personer)
            "innholdValgt":  v.get("innholdValgt") or [],
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

_DRIVER_HOST_MARKERS = ("rute.", "sjåfør", "xn--sjfr-zra", "sjafor.")


def _is_driver_host() -> bool:
    # Vercel proxies bestilling/rute.havoyet.no → Render og bytter Host-headeren
    # til onrender.com-targetet. Vi må derfor sjekke X-Forwarded-Host først (den
    # bevares av Vercel) og falle tilbake til request.host for direkte tilkobling.
    host = (
        request.headers.get("X-Forwarded-Host")
        or request.host
        or ""
    ).lower()
    return any(m in host for m in _DRIVER_HOST_MARKERS)


@app.route("/")
def serve_index():
    # sjåfør.havoyet.no (Unicode eller Punycode) → sjåfør-appen
    if _is_driver_host():
        return send_from_directory(_BASE_DIR, "sjafor.html")
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
    "tracking-preview": "tracking-preview.html",
    "rute":             "rute.html",
    "sjafor":           "sjafor.html",
    "sjåfør":           "sjafor.html",
    "varer":            "varer.html",
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

# Ordre-statuser som betyr "venter på betaling" — pre-lagret før kunde har betalt.
# Admin skal IKKE få "new_order"-varsel mens en ordre fortsatt er i en av disse,
# bare når status flippes til en fullført status ("NEW" for kontant/faktura, eller
# "PAID" etter at Vipps/Stripe har bekreftet betaling).
_PENDING_ORDER_STATUSES = {"AWAITING_PAYMENT", "PENDING", "CART"}

def _is_pending_order_status(status):
    """True hvis ordrestatusen er en pre-betalings-status og ikke skal utløse admin-varsel."""
    return str(status or "").strip().upper() in _PENDING_ORDER_STATUSES


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


def _effective_kg(name, qty, unit, unit_price=None):
    """kg-ekvivalent for en ordrelinje. g→kg, kg→kg, sjøkreps stk→0,16 kg/stk.
    Returnerer None for vanlige stk-varer (kan ikke konverteres til kg).
    NB: sjøkreps selges nå per stk (104 kr/stk) — stk→kg-konverteringen gjelder
    kun kr/kg-prisede linjer (gamle ordre à 650 kr/kg). Når unit_price er kjent
    og < 300 antas per stk-pris, og vi returnerer None (qty × pris er riktig)."""
    try:
        q = float(qty)
    except (TypeError, ValueError):
        return None
    u = (unit or "").strip().lower()
    if u in ("g", "gram", "grams"):
        return q / 1000.0
    if u == "kg":
        return q
    n = (name or "").lower()
    # Stk-solgte skalldyr med kjent snittvekt: sjøkreps 160 g, krabbeklør 170 g
    # (~6 stk/kg). Konverteringen gjelder kun kr/kg-prisede linjer — per stk-
    # prisede (pris < 300) skal regnes qty × pris direkte.
    kg_per_stk = None
    if "sjøkreps" in n or "sjokreps" in n:
        kg_per_stk = 0.16
    elif "krabbeklør" in n or "krabbeklor" in n:
        kg_per_stk = 0.17
    if kg_per_stk is not None:
        try:
            if unit_price is not None and 0 < float(unit_price) < 300:
                return None
        except (TypeError, ValueError):
            pass
        return q * kg_per_stk
    return None


# Standard kost-andel for varer uten registrert innkjøpspris — Eriks faste
# antagelse (45 % kost / 55 % margin), lik frontendens _orderCostBreakdown.
_COST_AVG_RATIO = 0.45

# MVA-sats for omsetningsrapporten. Havøyet selger sjømat/næringsmidler =
# 15 % (redusert sats). Brukt til netto/mva-splitt i regnskaps-rapporten.
_MVA_RATE = 0.15

_MND_NAVN = ["januar", "februar", "mars", "april", "mai", "juni",
             "juli", "august", "september", "oktober", "november", "desember"]


def _norsk_dato(s):
    """Formater en ISO-dato (2026-08-10) som «10. august 2026» for visning.
    Tåler etterfølgende klokkeslett eller annet format — returnerer da
    originalstrengen uendret (ingen krasj på «—» e.l.)."""
    m = _re.match(r"^\s*(\d{4})-(\d{1,2})-(\d{1,2})", str(s or ""))
    if not m:
        return str(s or "")
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if not (1 <= mo <= 12):
        return str(s or "")
    return f"{d}. {_MND_NAVN[mo - 1]} {y}"


def _nice_period_label(pf, pt):
    """Pen periode-etikett. Hvis fra=1. og til=siste dag i SAMME måned,
    vis «Juni 2026» i stedet for «01.06.2026 – 30.06.2026»."""
    import calendar as _cal
    try:
        if (pf.day == 1 and pf.year == pt.year and pf.month == pt.month
                and pt.day == _cal.monthrange(pf.year, pf.month)[1]):
            return f"{_MND_NAVN[pf.month - 1].capitalize()} {pf.year}"
    except Exception:
        pass
    return f"{pf.strftime('%d.%m.%Y')} – {pt.strftime('%d.%m.%Y')}"


def _order_cost_kr(order, cost_map):
    """Beregn innkjøpskostnad for én ordre — robust mot korrupte ×1000-verdier.

    Ordredataen har flere feilkilder fra bug-æraen, og de KAN ikke stoles på som
    absoluttverdier:
      • korrupt `pris` ×1000 på erstatnings-linjer (#HBU13UB «Kveite steak»
        pris=260000 i stedet for 260; én ordre blåste linjesum til 581 090 kr).
      • korrupt `lineCost` ×1000 («Reker ferske» lineCost=86 310 i stedet for 86).
      • gram/kg-miks — `cost` (kr/kg) × `qty` (gram) ga ~1000× for høy fiskekost.

    Vi unngår ALLE ved aldri å gange opp en lagret absoluttverdi:
      1) Forankre i ordrens RENE totalsum (`sum`/`total` — samme autoritative felt
         som omsetningen bruker; ukorrupt).
      2) Beregn en verdivektet kost-RATIO fra produktmiksen (cost_map-ratio =
         innkjøpspris/utsalgspris, 45 % fallback), vektet med rene strukturerte
         linjeverdier (qty/unit/price via _effective_kg — IKKE den korrupte `pris`).
      3) COGS = totalsum × ratio. Ratioen er begrenset til [0,1], så ingen korrupt
         verdi kan blåse opp tallet (HBU13UB blir 1149 × ~0,5 ≈ 575 kr, ikke 261k)."""
    if not isinstance(order, dict):
        return 0.0
    try:
        order_total = float(order.get("sum") or order.get("total") or 0)
    except (TypeError, ValueError):
        order_total = 0.0
    items = order.get("varer") or order.get("prods") or order.get("items") or []
    if not isinstance(items, list):
        items = []
    num = 0.0   # Σ linjeverdi × ratio
    den = 0.0   # Σ linjeverdi
    for it in items:
        if not isinstance(it, dict):
            continue
        name = (it.get("name") or it.get("navn") or it.get("title") or "").strip().lower()
        try:
            qty = float(it.get("qty") or it.get("quantity") or 1)
        except (TypeError, ValueError):
            qty = 1.0
        try:
            unit_price = float(it.get("price") or 0)
        except (TypeError, ValueError):
            unit_price = 0.0
        # Strukturert linjeverdi (kg×kr/kg for vekt, qty×pris for stk) — ren,
        # uavhengig av den korrupte `pris`. Brukes KUN til ratio-vekting.
        kg = _effective_kg(name, qty, it.get("unit"), unit_price)
        line_val = (kg * unit_price) if kg is not None else (qty * unit_price)
        if line_val <= 0:
            continue
        info = cost_map.get(name) if name else None
        ratio = info["ratio"] if info else _COST_AVG_RATIO
        num += line_val * ratio
        den += line_val
    ratio = (num / den) if den > 0 else _COST_AVG_RATIO
    ratio = min(0.95, max(0.05, ratio))   # sikkerhetsnett mot korrupt data
    base = order_total if order_total > 0 else den
    return base * ratio


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
                st = str(o.get("status") or "").strip().upper()
                # status PAID/PAID_OUT settes av Vipps/Stripe-callbacks og
                # nettside-sync — teller som betalt selv om paymentStatus
                # mangler (samme regel som _all_orders_normalized).
                if str(ps).lower() == "paid" or st in ("PAID", "PAID_OUT"):
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
            # Pre-betalings-ordre (AWAITING_PAYMENT/PENDING/CART) er ikke betalt og
            # skal ikke vises i den aktive lista — heller ikke når paymentStatus-
            # feltet mangler (tomt felt tolkes ellers som "betalt" for legacy-import
            # lenger nede). Uten dette lekket forlatte Vipps-checkouts (kunde gikk inn
            # i Vipps, men betalte aldri) inn som aktive ordre i admin.
            #
            # MEN: `ordrenr in paid` er fasiten på FAKTISK betaling — den leser
            # Vipps/Stripe-betalingsloggen direkte (CAPTURED/AUTHORIZED/PAID …).
            # Hvis betalingen er bekreftet der, men status-feltet henger igjen på
            # AWAITING_PAYMENT (race: Vipps-callback kom før ordren ble lagra via
            # /api/orders/new, så status-flippen fant ingen ordre å oppdatere),
            # SKAL ordren vises. Vi skjuler derfor kun pending-ordre som heller
            # ikke finnes i betalingsloggen — slik leses ekte betalte ordre alltid.
            if _is_pending_order_status(status) and ordrenr not in paid:
                continue
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
# Kategori-konfigurasjon styrt fra admin: hidden=skjul fra filter,
# custom=admin-opprettede ekstra-kategorier (utover de hardkodede i admin.html)
_category_config = {"hidden": [], "custom": []}
_reviews = []  # [{id, slug, name, rating, text, date}]
# Faktura-konfig styrt fra admin: kontonr, orgnr, forfallsdager.
# Synket cross-device så ulike admin-enheter ikke ender med ulik info
# på PDF-fakturaer som genereres for kundene.
_INVOICE_CONFIG_DEFAULTS = {
    "bankAccount": "1520.16.87214",
    "orgNr":       "934 859 197 MVA",  # Havøyet AS
    "paymentDays": 14,
}
_invoice_config = dict(_INVOICE_CONFIG_DEFAULTS)
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
    "route_eta": {
        "enabled": True, "channel": "email",
        "subject": "Estimert leveringstid for bestilling #{ordrenr} — Havøyet",
        "body": ("Hei {navn},\n\nVi planlegger å være på døren din i dag "
                 "ca. kl. {eta_clock} med bestilling #{ordrenr}.\n\n"
                 "Du vil høre fra oss kort tid før vi er på døren, og vi "
                 "tar kontakt umiddelbart hvis det blir vesentlige avvik "
                 "fra estimert tid.\n\n"
                 "Følg leveringen live: {tracking_url}\n\n"
                 "Spørsmål? Svar på denne e-posten.\n\n— Havøyet"),
    },
    "welcome_email": {
        "enabled": True, "channel": "email",
        "subject": "Velkommen til Havøyet",
        "body": ("Hei {navn},\n\nTakk for at du meldte deg på nyhetsbrevet "
                 "vårt. Du vil få meldinger om ukens fisk, sesong-tilbud og "
                 "nyheter fra Havøyet — ikke noe spam, vi lover.\n\n"
                 "Som abonnent får du også eksklusive rabatter når du er "
                 "innlogget på havoyet.no.\n\nVennlig hilsen,\nErik · Havøyet"),
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
# ── FORLATTE KASSER (snapshots fra /kasse-flyt før ordren er fullført) ────────
# Hver entry: {id (session-id fra frontend), ts_created, ts_updated, status,
#              kunde {navn, epost, tlf, adresse, postnr, sted, ...}, varer[],
#              total, fee, sum, rabattBelop, source}
# status: 'open' | 'contacted' | 'converted'
_abandoned_carts = []
# ── ORDRE-TOMBSTONES (slettede ordre-id-er) ──────────────────────────────────
# iPad-en (pakke.html, index.html) merger sin localStorage med server-lista og
# POSTer union-en tilbake. Uten tombstones ville en slettet test-ordre dukke
# opp igjen så snart iPad-en synket. Vi lagrer derfor slettede id/ordrenr som
# {kode: ts_deleted}; POST /api/manual-orders filtrerer dem bort fra incoming
# lister. En ny ordre med samme nr (via /api/orders/new) fjerner tombstone-en.
_order_tombstones = {}
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
            "category_config":     _category_config,
            "reviews":             _reviews,
            "customer_favorites":  _customer_favorites,
            "admin_notifiers":     _admin_notifiers,
            "customer_notify_config": _customer_notify_config,
            "invoice_config":      _invoice_config,
            "customers":           _customers,
            "vipps_imported_payments": _vipps_imported_payments,
            "card_payments_imported":  _card_payments_imported,
            "auth_users":          _auth_users,
            "auth_sessions":       _auth_sessions,
            "subscribers":         _subscribers,
            "discounts":           _discounts,
            "abandoned_carts":     _abandoned_carts,
            "order_tombstones":    _order_tombstones,
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
    global _manual_orders, _hidden_orders, _overrides, _packing_state, _order_notes, _product_overrides, _category_config, _reviews, _customer_favorites, _admin_notifiers, _customer_notify_config, _invoice_config, _customers, _vipps_imported_payments, _card_payments_imported, _auth_users, _auth_sessions, _subscribers, _discounts, _abandoned_carts, _order_tombstones
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
        _category_config   = d.get("category_config") or {"hidden": [], "custom": []}
        if not isinstance(_category_config, dict):
            _category_config = {"hidden": [], "custom": []}
        _category_config.setdefault("hidden", [])
        _category_config.setdefault("custom", [])
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
        saved_inv = d.get("invoice_config") or {}
        merged_inv = dict(_INVOICE_CONFIG_DEFAULTS)
        if isinstance(saved_inv, dict):
            for k in ("bankAccount", "orgNr", "paymentDays"):
                if k in saved_inv and saved_inv[k] not in (None, ""):
                    merged_inv[k] = saved_inv[k]
        try:
            merged_inv["paymentDays"] = int(merged_inv["paymentDays"])
        except (TypeError, ValueError):
            merged_inv["paymentDays"] = _INVOICE_CONFIG_DEFAULTS["paymentDays"]
        _invoice_config = merged_inv
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
        _abandoned_carts   = d.get("abandoned_carts", []) or []
        _order_tombstones  = d.get("order_tombstones", {}) or {}
        print(f"Lastet sync-state fra disk: {len(_packing_state)} pakket, {len(_manual_orders)} manuelle ordre, {len(_product_overrides)} produkt-overrides, {len(_reviews)} anmeldelser, {len(_admin_notifiers)} admin-mottakere, {len(_customers)} kunder, {len(_auth_users)} auth-brukere, {len(_auth_sessions)} aktive sesjoner, {len(_subscribers)} nyhetsbrev-abonnenter")
    except Exception as e:
        print(f"[ADVARSEL] Kunne ikke laste sync-state: {e}")
        _seed_auth_users()


def _order_keys(o):
    """Begge mulige nøkler en ordre identifiseres med (kan være ulike i edge-cases)."""
    return {str(o.get("id") or "").strip(), str(o.get("ordrenr") or "").strip()} - {""}


def _order_updated_at(o):
    """Epoch-tidsstempel for siste reelle redigering av ordren (0 hvis ukjent).
    Settes av admin-PATCH (server-side) og pakke.html persistManualOrder
    (klient-side). Brukes i POST-mergen så nyeste versjon vinner."""
    try:
        return float(o.get("updatedAt") or 0)
    except (TypeError, ValueError):
        return 0.0

@app.route("/api/manual-orders", methods=["GET", "POST"])
def api_manual_orders():
    global _manual_orders
    if request.method == "POST":
        old_ids = {str(o.get("ordrenr") or o.get("id")) for o in _manual_orders}
        new_list = request.get_json(force=True) or []
        # Filter ut ordre vi har slettet før — iPad-en (pakke.html/index.html)
        # merger lokal localStorage med server-lista og POSTer union-en tilbake.
        # Uten dette filteret ville slettede ordre dukke opp igjen på neste sync.
        if _order_tombstones and isinstance(new_list, list):
            new_list = [o for o in new_list if not (_order_keys(o) & set(_order_tombstones.keys()))]
        # MERGE i stedet for full-replace: en utdatert klient (annen enhet/iPad,
        # eller etter en server-restart uten persistent disk) kjenner kanskje ikke
        # til alle ordre. Tidligere overskrev dens POST hele lista → ordre forsvant
        # (særlig manuelle Nesttun-hente-ordre). Nå beholder vi eksisterende ordre
        # som ikke finnes i innkommende liste og ikke er slettet (tombstone).
        # Ved id-konflikt vinner versjonen med nyest updatedAt — en stale iPad-
        # kopi skal IKKE rulle tilbake varer admin nettopp la til via PATCH.
        # (Mangler begge stempel → innkommende vinner, som før.)
        if isinstance(new_list, list):
            incoming_keys = set()
            for o in new_list:
                incoming_keys |= _order_keys(o)
            tomb = set(_order_tombstones.keys()) if _order_tombstones else set()
            existing_by_key = {}
            for o in _manual_orders:
                for k in _order_keys(o):
                    existing_by_key[k] = o
            merged = []
            for o in new_list:
                srv = next((existing_by_key[k] for k in _order_keys(o) if k in existing_by_key), None)
                if srv is not None and _order_updated_at(srv) > _order_updated_at(o):
                    merged.append(srv)  # server-versjonen er nyere redigert — behold den
                else:
                    merged.append(o)
            for o in _manual_orders:
                keys = _order_keys(o)
                if keys & incoming_keys:
                    continue            # konflikten er allerede avgjort over
                if keys & tomb:
                    continue            # ordren er slettet
                merged.append(o)        # behold ordre klienten ikke kjente til
            new_list = merged
        # Finn ordre som er nye (ikke fantes på serveren fra før) → varsle om dem
        added = [o for o in new_list
                 if str(o.get("ordrenr") or o.get("id")) not in old_ids]
        _manual_orders = new_list
        _save_sync_state()
        for o in added:
            # Hopp over varsel for ordre som fortsatt venter på betaling —
            # disse fyrer av "new_order" først når status flippes til PAID/NEW.
            if _is_pending_order_status(o.get("status")):
                continue
            nr = o.get("ordrenr") or o.get("id") or "?"
            _notify_admins(
                "new_order",
                f"[Havøyet] Ny bestilling #{nr}",
                _format_order_lines(o),
                html_body=_format_order_email_html(o, "Det er kommet inn en ny bestilling.", "new_order"),
            )
        return jsonify({"ok": True, "count": len(_manual_orders)})
    # Skjul pre-betalings-ordre (AWAITING_PAYMENT/PENDING/CART) fra den aktive
    # ordre-/pakkelista — de er IKKE betalt og skal ikke pakkes. De beholdes i
    # _manual_orders slik at en sen Vipps/Stripe-webhook fortsatt kan flippe dem
    # til PAID (da dukker de opp igjen). ?include_pending=1 gir full liste.
    # Betalingsloggen (_paid_ordrenrs) er fasiten: er betalingen bekreftet der,
    # men status-feltet henger på AWAITING_PAYMENT (race der callback kom før
    # ordren ble lagra), SKAL ordren vises — samme regel som _all_orders_normalized.
    if request.args.get("include_pending") == "1":
        return jsonify(_manual_orders)
    paid = _paid_ordrenrs()
    def _hide_unpaid_pending(o):
        if not _is_pending_order_status(o.get("status")):
            return False
        nr = str(o.get("ordrenr") or o.get("id") or "").strip()
        return nr not in paid
    visible = [o for o in _manual_orders if not _hide_unpaid_pending(o)]
    return jsonify(visible)


@app.route("/api/manual-orders/<order_id>", methods=["DELETE"])
def api_delete_manual_order(order_id):
    """Slett ordre + opprett tombstone så stale iPad-cache ikke kan re-skape den."""
    global _manual_orders, _order_tombstones
    target = str(order_id).strip()
    if not target:
        return jsonify({"ok": False, "error": "Tomt ordrenummer"}), 400
    before = len(_manual_orders)
    # Match både id og ordrenr (tidligere matchet bare id — feilet på ordrer
    # opprettet via /api/orders/new som setter ordrenr men ikke id).
    removed_keys = set()
    keep = []
    for o in _manual_orders:
        keys = _order_keys(o)
        if target in keys:
            removed_keys |= keys
        else:
            keep.append(o)
    _manual_orders = keep
    # Tombstone alle nøkler ordren ble identifisert med, så iPad-syncen
    # ikke kan resurrekte den. Inkluder også target uavhengig av om ordren
    # faktisk fantes (idempotent — gjentatt sletting funker).
    now = int(time.time())
    _order_tombstones[target] = now
    for k in removed_keys:
        _order_tombstones[k] = now
    _save_sync_state()
    return jsonify({"ok": True, "removed": before - len(_manual_orders), "tombstoned": list({target} | removed_keys)})


@app.route("/api/manual-orders/tombstones", methods=["GET", "DELETE"])
def api_order_tombstones():
    """GET: liste tombstones. DELETE: tøm alle (admin "Glem slettede ordre")."""
    global _order_tombstones
    if request.method == "DELETE":
        n = len(_order_tombstones)
        _order_tombstones = {}
        _save_sync_state()
        return jsonify({"ok": True, "cleared": n})
    return jsonify({"ok": True, "tombstones": _order_tombstones, "count": len(_order_tombstones)})


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


def _packing_diff_summary(old_state: dict, new_state: dict) -> list[str]:
    """Bygg en kort liste over produkt-endringer mellom to packing-state snapshots.
    Returnerer menneske-lesbare linjer som "[#H4WH9UW] Krabbeskjell: Tilgjengelig → Ikke tilgjengelig".
    Tom liste = ingen endringer verdt å varsle om."""
    AVAIL_LABELS = {"ok": "Tilgjengelig", "unsure": "Avventer fiskebåt", "no": "Ikke tilgjengelig", "maybe": "Avventer fiskebåt"}
    def _label(v):
        return AVAIL_LABELS.get(str(v or "").lower(), "(ikke vurdert)")
    def _repl_name(r):
        if not r:
            return ""
        if isinstance(r, dict):
            return r.get("name") or r.get("slug") or r.get("id") or ""
        return str(r)

    def _item_name(order_id: str, idx) -> str:
        for o in (_manual_orders or []):
            oid = str(o.get("ordrenr") or o.get("id") or "")
            if oid != str(order_id):
                continue
            varer = o.get("varer") or o.get("items") or []
            try:
                v = varer[int(idx)]
                return v.get("name") or v.get("navn") or f"linje {idx}"
            except (IndexError, ValueError, TypeError):
                return f"linje {idx}"
        return f"linje {idx}"

    lines: list[str] = []
    for order_id, new_lines in (new_state or {}).items():
        old_lines = (old_state or {}).get(order_id) or {}
        if not isinstance(new_lines, dict):
            continue
        for idx, new_meta in new_lines.items():
            if not isinstance(new_meta, dict):
                continue
            old_meta = old_lines.get(idx) or {}
            name = _item_name(order_id, idx)
            old_avail = old_meta.get("avail")
            new_avail = new_meta.get("avail")
            if (old_avail or "") != (new_avail or ""):
                lines.append(f"#{order_id} · {name}: {_label(old_avail)} → {_label(new_avail)}")
            old_conf = _repl_name(old_meta.get("confirmedReplacement"))
            new_conf = _repl_name(new_meta.get("confirmedReplacement"))
            if old_conf != new_conf and new_conf:
                lines.append(f"#{order_id} · {name}: erstattet med «{new_conf}»")
            # Endringer i komponent-status for kasse-linjer
            old_components = old_meta.get("components") or []
            new_components = new_meta.get("components") or []
            if new_components and (old_components or new_components):
                old_by_name = {c.get("name"): c for c in old_components if isinstance(c, dict)}
                for c in new_components:
                    if not isinstance(c, dict):
                        continue
                    cname = c.get("name") or "komponent"
                    oc = old_by_name.get(cname) or {}
                    if (oc.get("avail") or "") != (c.get("avail") or ""):
                        lines.append(f"#{order_id} · {name} / {cname}: {_label(oc.get('avail'))} → {_label(c.get('avail'))}")
                    old_cr = _repl_name(oc.get("confirmedReplacement"))
                    new_cr = _repl_name(c.get("confirmedReplacement"))
                    if old_cr != new_cr and new_cr:
                        lines.append(f"#{order_id} · {name} / {cname}: erstattet med «{new_cr}»")
    return lines


@app.route("/api/morning-availability-check")
def api_morning_availability_check():
    """Morgen-sjekk på leveringsdagen: hvis ikke ALLE varer i dagens leveringer er
    markert «Tilgjengelig», send én samle-e-post til alle admin med liste over de
    uavklarte varene. Trigges av daglig cron (06:30) via Nettsidens newsletter-cron
    med admin-Bearer. Sender bare når noe faktisk er uavklart (trygt å kalle ofte)."""
    deny = _require_admin_user()
    if deny: return deny
    import html as _h
    def _esc(s): return _h.escape(str(s or ""))
    AVAIL_LABELS = {"ok": "Tilgjengelig", "unsure": "Avventer fiskebåt", "no": "Ikke tilgjengelig", "maybe": "Avventer fiskebåt"}
    today = datetime.now(_OSLO_TZ).strftime("%Y-%m-%d")
    DONE = {"DONE", "PAID_OUT", "CANCELLED", "CANCELED", "REFUNDED"}
    try:
        orders = _all_orders_normalized(only_paid=True)
    except Exception:
        orders = []
    unresolved = []
    checked = 0
    for o in orders:
        if str(o.get("delivery") or "")[:10] != today:
            continue
        if str(o.get("status") or "").upper() in DONE:
            continue
        checked += 1
        oid = str(o.get("id"))
        pstate = (_packing_state or {}).get(oid) or {}
        if not isinstance(pstate, dict):
            pstate = {}
        miss = []
        for i, it in enumerate(o.get("items") or []):
            meta = pstate.get(str(i))
            if meta is None:
                meta = pstate.get(i)
            meta = meta if isinstance(meta, dict) else {}
            name = (it.get("name") or it.get("navn") or f"linje {i}") if isinstance(it, dict) else f"linje {i}"
            comps = meta.get("components")
            if isinstance(comps, list) and comps:
                for c in comps:
                    if not isinstance(c, dict):
                        continue
                    av = str(c.get("avail") or "").lower()
                    if av != "ok":
                        miss.append((f"{name} / {c.get('name') or 'komponent'}", AVAIL_LABELS.get(av, "Ikke vurdert")))
            else:
                av = str(meta.get("avail") or "").lower()
                if av != "ok":
                    miss.append((name, AVAIL_LABELS.get(av, "Ikke vurdert")))
        if miss:
            unresolved.append({"order": oid, "customer": o.get("customer") or "Ukjent", "items": miss})

    emailed = 0
    if unresolved:
        n_items = sum(len(u["items"]) for u in unresolved)
        subject = f"⚠️ {n_items} uavklarte vare(r) for dagens levering – {today}"
        text_lines = [f"Ikke alt er markert «Tilgjengelig» for dagens leveringer ({today}).", ""]
        html_blocks = []
        for u in unresolved:
            text_lines.append(f"#{u['order']} · {u['customer']}:")
            html_blocks.append(
                f"<p style='margin:16px 0 4px;font-weight:700;color:#0f172a'>#{_esc(u['order'])} · {_esc(u['customer'])}</p>"
                "<ul style='margin:0 0 6px 18px;padding:0;color:#334155;line-height:1.6'>")
            for nm, lbl in u["items"]:
                text_lines.append(f"   • {nm} — {lbl}")
                html_blocks.append(f"<li>{_esc(nm)} — <strong>{_esc(lbl)}</strong></li>")
            html_blocks.append("</ul>")
            text_lines.append("")
        body = "\n".join(text_lines)
        html_body = (
            "<div style=\"font-family:'Helvetica Neue',Arial,sans-serif;max-width:640px;margin:0 auto;padding:8px\">"
            "<h2 style='color:#b45309;margin:0 0 6px'>Uavklarte varer – dagens levering</h2>"
            f"<p style='color:#475569;margin:0 0 8px'>Ikke alt er markert «Tilgjengelig» for {today}. "
            "Gå gjennom pakke-visningen, finn alternativer eller kontakt kundene.</p>"
            + "".join(html_blocks) +
            "<p style='margin-top:18px'><a href='https://bestilling.havoyet.no/pakke.html' "
            "style='background:#0d9488;color:#fff;padding:11px 20px;border-radius:8px;text-decoration:none;font-weight:600'>Åpne pakke-visningen</a></p>"
            "</div>")
        seen = set()
        recipients = []
        for n in (_admin_notifiers or []):
            e = (n.get("email") or "").strip()
            if e and e.lower() not in seen:
                seen.add(e.lower()); recipients.append(e)
        if "erik@havoyet.no" not in seen:
            recipients.append("erik@havoyet.no")
        for em in recipients:
            try:
                ok, _d = _send_via_resend("nyhetsbrev@havoyet.no", "Havøyet", subject, body, to_email=em, html_body=html_body)
                if ok:
                    emailed += 1
            except Exception as _e:
                print(f"[MORNING] send feilet til {em}: {_e}")

    return jsonify({"date": today, "orders_checked": checked, "unresolved_orders": len(unresolved), "emailed": emailed})


@app.route("/api/packing-state", methods=["GET", "POST"])
def api_packing_state():
    global _packing_state
    if request.method == "POST":
        old_state = dict(_packing_state or {})
        _packing_state = request.get_json(force=True) or {}
        _save_sync_state()
        # Diff vs forrige snapshot — send "product_changed"-varsel hvis det er
        # meningsfulle endringer (avail eller bekreftet erstatning).
        try:
            changes = _packing_diff_summary(old_state, _packing_state)
            if changes:
                # Begrens til de første 12 linjene i body — resten vises som "...og N flere".
                preview = changes[:12]
                remaining = len(changes) - len(preview)
                body_lines = list(preview)
                if remaining > 0:
                    body_lines.append(f"...og {remaining} flere endringer.")
                import html as _html_mod
                _notify_admins(
                    "product_changed",
                    "[Havøyet] Pakke-status oppdatert",
                    "\n".join(body_lines),
                    html_body=(
                        "<p>Følgende produkt-endringer er registrert på bestillingsiden eller i admin:</p>"
                        + "<ul>" + "".join(f"<li>{_html_mod.escape(l)}</li>" for l in body_lines) + "</ul>"
                    ),
                )
        except Exception as e:
            print(f"[packing-state] notify feilet: {e}")
        return jsonify({"ok": True})
    return jsonify(_packing_state)


@app.route("/api/prisliste")
def api_prisliste():
    return jsonify(_prisliste)


@app.route("/api/prisliste/sync", methods=["POST"])
def api_prisliste_sync():
    fetch_domstein_prisliste()
    return jsonify(_prisliste)


# ── FORLATTE KASSER (snapshots fra /kasse-flyt før ordren er fullført) ────────
def _normalize_email(e):
    return (e or "").strip().lower()

def _normalize_phone(t):
    return "".join(ch for ch in (t or "") if ch.isdigit())

def _mark_carts_converted_for_order(order):
    """Når en ordre lagres i _manual_orders, finn snapshots med samme e-post/tlf
    og marker dem som 'converted'. Kalles fra /api/orders/new."""
    global _abandoned_carts
    if not _abandoned_carts:
        return
    kunde = (order or {}).get("kunde") or {}
    email = _normalize_email(kunde.get("epost"))
    phone = _normalize_phone(kunde.get("tlf"))
    ordrenr = str(order.get("ordrenr") or order.get("id") or "")
    changed = False
    for c in _abandoned_carts:
        if c.get("status") == "converted":
            continue
        c_email = _normalize_email((c.get("kunde") or {}).get("epost"))
        c_phone = _normalize_phone((c.get("kunde") or {}).get("tlf"))
        if (email and c_email and email == c_email) or (phone and c_phone and phone == c_phone):
            c["status"] = "converted"
            c["converted_at"] = int(time.time())
            c["converted_ordrenr"] = ordrenr
            changed = True
    if changed:
        _save_sync_state()

@app.route("/api/checkout/snapshot", methods=["POST"])
def api_checkout_snapshot():
    """Upsert et kasse-snapshot. Frontend sender dette debounced når kunden har
    fylt inn e-post eller telefon i /kasse-flyt og har varer i kurven.
    Krever sessionId + minst én av e-post/telefon."""
    global _abandoned_carts
    data = request.get_json(force=True, silent=True) or {}
    sid = (data.get("sessionId") or "").strip()
    kunde = data.get("kunde") or {}
    varer = data.get("varer") or []
    if not sid:
        return jsonify({"ok": False, "error": "Mangler sessionId"}), 400
    email = (kunde.get("epost") or "").strip()
    tlf   = (kunde.get("tlf") or "").strip()
    if not email and not tlf:
        return jsonify({"ok": False, "error": "Trenger e-post eller telefon"}), 400
    if not isinstance(varer, list) or not varer:
        return jsonify({"ok": False, "error": "Tom handlekurv"}), 400

    now = int(time.time())
    # Finn eksisterende snapshot med samme session-id
    existing = next((c for c in _abandoned_carts if c.get("id") == sid), None)
    # Hvis e-posten allerede har en fullført ordre, hopp over (unngå støy)
    if existing and existing.get("status") == "converted":
        return jsonify({"ok": True, "skipped": "converted"})

    entry = existing or {
        "id":         sid,
        "ts_created": now,
        "status":     "open",
        "source":     "kasse-flyt",
    }
    entry["ts_updated"] = now
    entry["kunde"]      = kunde
    entry["varer"]      = varer
    entry["total"]      = float(data.get("total") or 0)
    entry["fee"]        = float(data.get("fee") or 0)
    entry["sum"]        = float(data.get("sum") or 0)
    entry["rabattBelop"]= float(data.get("rabattBelop") or 0)
    if not existing:
        _abandoned_carts.append(entry)
    _save_sync_state()
    return jsonify({"ok": True, "id": sid})

@app.route("/api/abandoned-carts", methods=["GET"])
def api_abandoned_carts():
    """Liste forlatte kasser. Default returnerer alt unntatt 'converted'.
    Bruk ?status=all for alle, ?status=open for kun aktive."""
    deny = _require_admin_user()
    if deny: return deny
    status_filter = (request.args.get("status") or "").strip().lower()
    items = list(_abandoned_carts or [])
    if status_filter == "all":
        out = items
    elif status_filter in ("open", "contacted", "converted"):
        out = [c for c in items if c.get("status") == status_filter]
    else:
        out = [c for c in items if c.get("status") != "converted"]
    # Nyeste først
    out = sorted(out, key=lambda c: int(c.get("ts_updated") or 0), reverse=True)
    return jsonify({"ok": True, "carts": out, "count": len(out)})

@app.route("/api/abandoned-carts/<cart_id>", methods=["PATCH", "DELETE"])
def api_abandoned_cart_item(cart_id):
    """PATCH: oppdater status (open|contacted). DELETE: fjern snapshot."""
    deny = _require_admin_user()
    if deny: return deny
    global _abandoned_carts
    idx = next((i for i, c in enumerate(_abandoned_carts) if c.get("id") == cart_id), None)
    if idx is None:
        return jsonify({"ok": False, "error": "Fant ikke kassen"}), 404
    if request.method == "DELETE":
        removed = _abandoned_carts.pop(idx)
        _save_sync_state()
        return jsonify({"ok": True, "removed": removed.get("id")})
    patch = request.get_json(force=True, silent=True) or {}
    new_status = (patch.get("status") or "").strip().lower()
    if new_status not in ("open", "contacted"):
        return jsonify({"ok": False, "error": "Ugyldig status (open|contacted)"}), 400
    _abandoned_carts[idx]["status"] = new_status
    _abandoned_carts[idx]["status_changed_at"] = int(time.time())
    _save_sync_state()
    return jsonify({"ok": True, "cart": _abandoned_carts[idx]})


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


@app.route("/api/admin/invoice-config", methods=["GET", "PUT"])
def api_invoice_config():
    """Faktura-konfig (bankkonto, orgnr, forfallsdager) styrt fra admin.
    Synket cross-device så alle admin-enheter genererer PDF-fakturaer med
    samme info. Defaults fra _INVOICE_CONFIG_DEFAULTS gjelder hvis ingen
    har lagret noe enda."""
    global _invoice_config
    if request.method == "GET":
        return jsonify(_invoice_config or dict(_INVOICE_CONFIG_DEFAULTS))
    payload = request.get_json(force=True) or {}
    if not isinstance(payload, dict):
        return jsonify({"error": "Forventer JSON-objekt"}), 400
    cur = dict(_invoice_config or _INVOICE_CONFIG_DEFAULTS)
    if "bankAccount" in payload and payload["bankAccount"] not in (None, ""):
        cur["bankAccount"] = str(payload["bankAccount"]).strip()
    if "orgNr" in payload and payload["orgNr"] not in (None, ""):
        cur["orgNr"] = str(payload["orgNr"]).strip()
    if "paymentDays" in payload and payload["paymentDays"] not in (None, ""):
        try:
            cur["paymentDays"] = int(payload["paymentDays"])
        except (TypeError, ValueError):
            pass
    _invoice_config = cur
    _save_sync_state()
    return jsonify({"ok": True, "config": _invoice_config})


@app.route("/api/categories/config", methods=["GET", "PUT"])
def api_categories_config():
    """Admin-styrt kategori-konfig: skjulte + ekstra-egendefinerte kategorier.
    Brukt av admin-produktfilteret og produkt-redigeringsdroppen."""
    global _category_config
    if request.method == "GET":
        return jsonify(_category_config or {"hidden": [], "custom": []})
    payload = request.get_json(force=True) or {}
    if not isinstance(payload, dict):
        return jsonify({"error": "Forventer JSON-objekt"}), 400
    def _clean_list(v):
        if not isinstance(v, list):
            return []
        out, seen = [], set()
        for x in v:
            s = str(x or "").strip()
            if s and s not in seen:
                seen.add(s)
                out.append(s)
        return out
    _category_config = {
        "hidden": _clean_list(payload.get("hidden")),
        "custom": _clean_list(payload.get("custom")),
    }
    _save_sync_state()
    return jsonify({"ok": True, "config": _category_config})


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
    baseline_slugs = set()
    for p in products:
        slug = p.get("slug")
        baseline_slugs.add(slug)
        ov = overrides.get(slug)
        if isinstance(ov, dict):
            p.update(ov)
    # Produkter som KUN finnes i overrides (opprettet i admin, ikke i data2.jsx-baseline).
    # Uten dette blir _created-produkter (f.eks. fiskesuppe) usynlige for alle som
    # leser denne lista — nyhetsbrev, varer.html osv. Krever et ekte navn for å
    # hoppe over rene pris-stubber (slug-overrides uten produktdata).
    for slug, ov in overrides.items():
        if not isinstance(ov, dict) or slug in baseline_slugs:
            continue
        if not (ov.get("name") or "").strip():
            continue
        np = dict(ov)
        np.setdefault("slug", slug)
        products.append(np)
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


# ── PRODUKT-POPULARITET (bestselgere fra ekte ordre) ────────────────────────
# Driver «mest populært øverst» på nettsidens /butikk. Aggregerer betalte
# Havøyet-ordre til {slug: antall_ordre_med_produktet} (ordre-frekvens, ikke
# kvantum — mindre skjevt mot store enkeltkjøp). Cachet 30 min. Returnerer KUN
# slug→tall (ingen kundedata). NFV-ordre + test-produkter ekskluderes.
_POPULARITY_CACHE = {"data": None, "ts": 0}
_POPULARITY_TTL = 1800  # 30 min


def _norm_prod_name(s):
    """Normaliser et (ordrelinje-)navn til base-navn for slug-oppslag.
    Kutter variant-suffiks («Krabbeskjell - 200 g» → «krabbeskjell») og fjerner
    etterfølgende vekt-/mengde-tokens («Blåkveitefilet røkt 250g» → «…røkt»)."""
    import re as _re
    n = (s or "").strip().lower()
    for sep in (" - ", " – ", " / ", " (", " ,", ","):
        k = n.find(sep)
        if k > 0:
            n = n[:k]
    n = _re.sub(r"\b\d+[\.,]?\d*\s*(?:g|gram|kg|stk|pk|pakke|l|cl|ml)\b\.?$", "", n).strip()
    n = _re.sub(r"\s{2,}", " ", n).strip()
    return n


def _build_name_slug_index():
    """(exact, prefixes): norm(navn)→slug fra baseline + overrides (inkl.
    override-only produkter som fiskesuppe). `prefixes` er sortert lengste navn
    først for trygt prefiks-oppslag («blåskjell levende økologisk»→blaaskjell)."""
    try:
        products = _get_products_baseline()
    except Exception:
        products = []
    overrides = _product_overrides or {}
    merged = []
    seen = set()
    for p in products:
        slug = p.get("slug")
        if not slug:
            continue
        seen.add(slug)
        ov = overrides.get(slug)
        nm = ((ov.get("name") if isinstance(ov, dict) and ov.get("name") else p.get("name")) or "").strip()
        if nm:
            merged.append((slug, nm))
    for slug, ov in overrides.items():
        if slug in seen or not isinstance(ov, dict):
            continue
        nm = (ov.get("name") or "").strip()
        if nm:
            merged.append((slug, nm))
    exact = {}
    pref = {}
    for slug, nm in merged:
        k = _norm_prod_name(nm)
        if not k:
            continue
        exact.setdefault(k, slug)
        if k not in pref:
            pref[k] = slug
    prefixes = sorted(pref.items(), key=lambda kv: -len(kv[0]))
    return exact, prefixes


def _line_slug(it, exact, prefixes, valid):
    slug = (it.get("slug") or "").strip()
    if slug in valid:
        return slug
    nm = (it.get("productName") or it.get("name") or "").strip()
    if not nm or "test" in nm.lower():
        return ""
    key = _norm_prod_name(nm)
    if key in exact:
        return exact[key]
    for pk, ps in prefixes:
        if pk and key.startswith(pk):
            return ps
    return ""


def _compute_popularity():
    exact, prefixes = _build_name_slug_index()
    valid = set(exact.values()) | set(s for _, s in prefixes)
    try:
        orders = _all_orders_normalized(only_paid=True)
    except Exception:
        orders = []
    score = {}
    n_orders = 0
    for o in orders:
        if not isinstance(o, dict):
            continue
        store = (o.get("store") or "").lower()
        if "nesttun" in store or "nfv" in store:
            continue
        items = o.get("items") or o.get("varer") or []
        if not isinstance(items, list) or not items:
            continue
        n_orders += 1
        seen = set()
        for it in items:
            if not isinstance(it, dict):
                continue
            slug = _line_slug(it, exact, prefixes, valid)
            if slug and slug not in seen:
                score[slug] = score.get(slug, 0) + 1
                seen.add(slug)
    return {"popularity": score, "orders": n_orders, "updated": _now_iso_utc()}


@app.route("/api/products/popularity", methods=["GET"])
def api_products_popularity():
    """Bestselger-rangering {slug: antall_ordre}. ?refresh=1 tvinger ny beregning."""
    force = request.args.get("refresh") in ("1", "true", "yes")
    now = time.time()
    if not force and _POPULARITY_CACHE["data"] and (now - _POPULARITY_CACHE["ts"] < _POPULARITY_TTL):
        data = _POPULARITY_CACHE["data"]
    else:
        try:
            data = _compute_popularity()
            _POPULARITY_CACHE["data"] = data
            _POPULARITY_CACHE["ts"] = now
        except Exception as e:
            print(f"[popularity] feil: {e}")
            data = _POPULARITY_CACHE["data"] or {"popularity": {}, "orders": 0, "updated": _now_iso_utc()}
    resp = jsonify({"ok": True, **data})
    resp.headers["Cache-Control"] = "public, max-age=300"
    return resp


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


@app.route("/api/reviews/<review_id>", methods=["PATCH"])
def api_review_patch(review_id):
    """Admin-redigering av anmeldelse. Tillater å nulle ut produkt-lenken
    (slug="") for generelle service-anmeldelser, eller rette tekst/navn."""
    global _reviews
    data = request.get_json(force=True) or {}
    updated = None
    for r in _reviews:
        if r.get("id") == review_id:
            if "slug" in data:
                r["slug"] = (data.get("slug") or "").strip()
            if "name" in data:
                r["name"] = (data.get("name") or "Anonym").strip()[:80] or "Anonym"
            if "text" in data:
                r["text"] = (data.get("text") or "").strip()[:2000]
            if "rating" in data:
                try:
                    rating = int(data.get("rating", 5))
                except (TypeError, ValueError):
                    rating = 5
                r["rating"] = max(1, min(5, rating))
            updated = r
            break
    if not updated:
        return jsonify({"ok": False, "error": "Ikke funnet"}), 404
    _save_sync_state()
    return jsonify({"ok": True, "review": updated})


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


def _send_via_resend(from_email, from_name, subject, body, to_email=None, reply_to=None,
                      html_body=None):
    """Send via Resend API (enklest — bare API-nøkkel trengs).
    Legger automatisk ved signatur (text + html) på alle utgående e-poster.

    Hvis `html_body` er gitt, brukes den som HTML-versjon istedenfor å auto-
    konvertere `body`. Plain-text-versjonen er fortsatt avledet fra `body`."""
    try:
        text_body = _strip_image_placeholders(body or "") + _SIGNATURE_TEXT
        html_body = (html_body if html_body is not None else _body_to_html(body)) + _SIGNATURE_HTML
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


def _send_via_smtp(from_email, from_name, subject, body, to_email=None, reply_to=None,
                    html_body=None):
    """Send via SMTP (Gmail / annen SMTP-server).
    Sender multipart/alternative med både text- og HTML-versjon med signatur.

    Hvis `html_body` er gitt, brukes den istedenfor auto-konvertert HTML."""
    recipient = to_email or CONTACT_TO
    text_body = _strip_image_placeholders(body or "") + _SIGNATURE_TEXT
    html_body = (html_body if html_body is not None else _body_to_html(body)) + _SIGNATURE_HTML
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


def _send_contact_mail(from_email, from_name, subject, body, html_body=None):
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
        ok, detail = _send_via_resend(from_email, from_name, subject, body, html_body=html_body)
        print(f"[CONTACT] Resend: {detail}")
        if ok:
            return True, detail

    # Fallback: SMTP
    if SMTP_USER and SMTP_PASS:
        ok, detail = _send_via_smtp(from_email, from_name, subject, body, html_body=html_body)
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
ADMIN_EVENTS = ("new_order", "order_updated", "order_delivered", "new_message", "tracking_health", "product_changed")
ADMIN_NOTIFY_LOG = os.path.join(os.path.dirname(_BASE_DIR), "admin_notifications.jsonl")

# ── SMS PÅ PAUSE (Eriks beslutning 2026-06-05) ──────────────────────────────
# Vi har ikke noe fungerende SMS-system/leverandør ennå, så ALL SMS-utsending
# (admin-varsler, kunde-statusmeldinger, testvarsler) hoppes over. E-post er
# primær varslingskanal (+ push/Telegram der det er konfigurert).
# Gjenåpne SMS ved å sette env SMS_PAUSED=0 på Render når leverandør er klar.
SMS_PAUSED = os.environ.get("SMS_PAUSED", "1").strip() not in ("0", "false", "False")

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


def _send_admin_mail(to_email, subject, body, html_body=None, reply_to=None):
    """Send én e-post til en admin-mottaker. Bruker Resend → SMTP → log.
    `html_body` (valgfritt) lar deg sende rik HTML istedenfor auto-konvertert.
    `reply_to` (valgfritt) setter Reply-To (f.eks. kundens adresse på
    «Ny melding»-varsler, så admin kan svare kunden direkte)."""
    if RESEND_API_KEY:
        ok, detail = _send_via_resend("", "Admin-varsel", subject, body,
                                      to_email=to_email, reply_to=reply_to,
                                      html_body=html_body)
        if ok:
            return True, detail
    if SMTP_USER and SMTP_PASS:
        ok, detail = _send_via_smtp("", "Admin-varsel", subject, body,
                                    to_email=to_email, reply_to=reply_to,
                                    html_body=html_body)
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
    if SMS_PAUSED:
        return False, "sms-paused (ingen aktiv SMS-leverandør — sett SMS_PAUSED=0 for å gjenåpne)"
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
    # Hard-disable kunde-mail på order_updated/order_delivered (besluttet 2026-05-22).
    # Kunden følger status via /konto-dashbord. SMS-grenen er urørt.
    allow_email = False

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


def _notify_admins(event, subject, body, html_body=None, reply_to=None):
    """Send varsel til alle admin-mottakere som har valgt `event`. Hver mottaker
    kan styre per varseltype hvilke kanaler som fyrer (event_channels).
    Bakoverkompat: hvis event_channels ikke er satt, brukes alle konfigurerte
    kanaler (e-post + SMS + push + telegram).

    `html_body` (valgfritt) gir rik HTML for e-post-kanalen. Plain-text-`body`
    brukes fortsatt for SMS, push og som fallback-tekst i e-postene.
    `reply_to` (valgfritt) setter Reply-To på e-postene (kundens adresse på
    «Ny melding» så admin kan svare direkte).

    Returnerer antall leverte varsler (alle kanaler) så kallere kan falle
    tilbake til andre kanaler hvis ingen mottakere fikk varselet."""
    if event not in ADMIN_EVENTS:
        return 0

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
            ok, detail = _send_admin_mail(email, subject, body, html_body=html_body, reply_to=reply_to)
            if ok: mail_sent += 1
            else:
                mail_failed += 1
                print(f"[ADMIN-NOTIFY] mail {email}: {detail}")
        if phone and "sms" in allowed and SMS_PAUSED:
            pass  # SMS er på pause — hopp over uten å telle som feil
        elif phone and "sms" in allowed:
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
    return mail_sent + sms_sent + push_sent + tg_sent


def _format_message_email_html(source, navn, epost, melding, *, tlf="", emne="", session_id="", history=None):
    """Pen HTML-versjon av kundemeldings-varselet — strukturert med banner,
    avsender-info-tabell, full meldingstekst i sitatblokk, og evt. chat-historikk.
    Brukes både for kontaktskjema-mailer og chat-handoff-mailer.
    `source` = "Kontaktskjema" | "Chat" | annet."""
    import html as _html
    def esc(s): return _html.escape(str(s or ""))

    # Banner-farge per kilde
    if source.lower().startswith("chat"):
        banner_color, banner_label = "#7C3AED", "Chat — kunde ber om hjelp"
    else:
        banner_color, banner_label = "#0d9488", "Ny melding"

    # Avsender-info som tabell
    info_rows = []
    info_rows.append(("Navn", esc(navn) or "—"))
    info_rows.append(("E-post", f'<a href="mailto:{esc(epost)}" style="color:#0d9488;text-decoration:none">{esc(epost)}</a>' if epost else "—"))
    if tlf:
        info_rows.append(("Telefon", f'<a href="tel:{esc(tlf)}" style="color:#0d9488;text-decoration:none">{esc(tlf)}</a>'))
    info_rows.append(("Mottatt", datetime.now(_OSLO_TZ).strftime("%Y-%m-%d kl. %H:%M")))
    info_rows.append(("Kilde", esc(source)))
    if emne:
        info_rows.append(("Emne", esc(emne)))
    info_html = "".join(
        f'<tr><td style="padding:6px 10px 6px 0;color:#777;font-size:13px;width:100px;vertical-align:top">{lbl}</td>'
        f'<td style="padding:6px 0;color:#1a1a1a;font-size:14px">{val}</td></tr>'
        for lbl, val in info_rows
    )

    # Melding som sitatblokk
    melding_html = (
        f'<div style="margin-top:18px">'
        f'<div style="font-size:11px;color:#666;text-transform:uppercase;letter-spacing:.6px;font-weight:700;margin-bottom:8px">Melding</div>'
        f'<div style="background:#F4F1EA;border-left:4px solid #0d9488;padding:14px 18px;border-radius:0 6px 6px 0;color:#1a1a1a;font-size:15px;line-height:1.6;white-space:pre-wrap">{esc(melding)}</div>'
        f'</div>'
    )

    # Chat-historikk (valgfri)
    history_html = ""
    if isinstance(history, list) and history:
        rows = []
        for m in history[-12:]:
            role = m.get("role") or ""
            text = m.get("text") or ""
            who_label = {"customer": navn or "Kunde", "ai": "Bot", "admin": "Admin"}.get(role, role.title() or "—")
            color = "#0d9488" if role == "customer" else ("#7C3AED" if role == "ai" else "#1a1a1a")
            rows.append(
                f'<div style="margin-bottom:10px"><div style="font-size:11px;color:{color};font-weight:700;text-transform:uppercase;letter-spacing:.4px;margin-bottom:2px">{esc(who_label)}</div>'
                f'<div style="color:#1a1a1a;font-size:13.5px;line-height:1.5;white-space:pre-wrap">{esc(text)}</div></div>'
            )
        history_html = (
            f'<div style="margin-top:18px">'
            f'<div style="font-size:11px;color:#666;text-transform:uppercase;letter-spacing:.6px;font-weight:700;margin-bottom:8px">Samtalehistorikk</div>'
            f'<div style="background:#FAFAF8;padding:14px;border-radius:6px;border:1px solid #ECE8DC">{"".join(rows)}</div>'
            f'</div>'
        )

    # Footer / call-to-action
    cta_parts = []
    if epost:
        cta_parts.append(f'<a href="mailto:{esc(epost)}" style="display:inline-block;background:#0d9488;color:#fff;text-decoration:none;padding:10px 22px;border-radius:999px;font-weight:700;font-size:14px">Svar {esc(navn) or "kunden"}</a>')
    if session_id:
        cta_parts.append(f'<a href="https://admin.havoyet.no/#chat" style="display:inline-block;margin-left:10px;padding:10px 22px;border-radius:999px;font-size:14px;color:#0d9488;text-decoration:none;border:1px solid #0d9488;font-weight:600">Åpne chat i admin</a>')
    cta_html = f'<div style="margin-top:22px;text-align:center">{"".join(cta_parts)}</div>' if cta_parts else ""

    reply_hint = ""
    if epost:
        reply_hint = (
            f'<div style="margin-top:16px;padding:10px 14px;background:#FFF8E1;border-left:3px solid #C8A45C;'
            f'border-radius:0 4px 4px 0;font-size:12.5px;color:#5C4A1E">'
            f'<strong>Tips:</strong> Svar direkte på denne e-posten — Reply-To peker til {esc(epost)}, '
            f'så kunden får svaret rett i innboksen.'
            f'</div>'
        )

    return (
        f'<div style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Helvetica,Arial,sans-serif;'
        f'background:#F4F1EA;padding:24px 12px;color:#1a1a1a">'
        f'<div style="max-width:560px;margin:0 auto;background:#fff;border-radius:10px;overflow:hidden;'
        f'box-shadow:0 1px 3px rgba(0,0,0,.08)">'
        # Banner
        f'<div style="background:{banner_color};color:#fff;padding:18px 22px">'
        f'<div style="font-size:11px;text-transform:uppercase;letter-spacing:1px;opacity:.85">{banner_label}</div>'
        f'<div style="font-size:22px;font-weight:700;margin-top:4px">{esc(navn) or "Kunde uten navn"}</div>'
        f'</div>'
        # Body
        f'<div style="padding:22px 22px 26px">'
        f'<table style="width:100%;border-collapse:collapse">{info_html}</table>'
        f'{melding_html}'
        f'{history_html}'
        f'{cta_html}'
        f'{reply_hint}'
        f'</div>'
        # Footer
        f'<div style="background:#FAFAF8;padding:12px 22px;border-top:1px solid #ECE8DC;'
        f'font-size:11px;color:#888;text-align:center">Havøyet — Bare fersk sjømat · '
        f'<a href="https://havoyet.no" style="color:#888;text-decoration:none">havoyet.no</a></div>'
        f'</div></div>'
    )


# ── E-POST MENGDE-HELPERS ──────────────────────────────────────────────────
# Delt av admin-varsel (_format_order_email_html) og kundebekreftelsen
# (_send_customer_order_confirmation) så begge viser identiske mengder.
import re as _re
def _grams_from_text(text):
    if not text:
        return None
    m = _re.search(r"(\d+(?:[.,]\d+)?)\s*(kg|g)\b", str(text).lower().replace(",", "."))
    if not m:
        return None
    try:
        n = float(m.group(1))
    except ValueError:
        return None
    return int(round(n * 1000)) if m.group(2) == "kg" else int(round(n))

def _amount_from_parens(text):
    """Tall oppgitt i parentes UTEN enhet, f.eks. "Reker (400)" → 400,
    "Sjøkreps (2)" → 2, "Torsk (2 – uten skinn)" → 2. Tallet må følges av
    ")" eller "–" (notat), så vekt-parenteser som "(1,5 kg)" / "(ca. 500 g)"
    IKKE matcher — de håndteres av _grams_from_text."""
    if not text:
        return None
    m = _re.search(r"\(\s*(\d+(?:[.,]\d+)?)\s*(?:[–\-]|\))", str(text))
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "."))
    except ValueError:
        return None

def _qty_text(item):
    """Vis faktisk bestilt mengde til høyre i stedet for "×1". Tallet i
    parentes er mengden: "Reker (400)" med enhet g → "400 g", "Sjøkreps (2)"
    med enhet stk → "2 stk". Eksplisitt vekt i navnet ("(ca. 500 g)") støttes
    også via _grams_from_text."""
    qty = item.get("qty") or item.get("quantity") or item.get("antall") or 1
    try:
        qty = float(qty)
    except (TypeError, ValueError):
        qty = 1
    unit = (item.get("unit") or "").lower()
    # Telle-enheter: produkter som SELGES per stykk/beger/pakke skal vises i
    # ANTALL i e-posten — aldri i vekt — selv om de er kr/kg-priset (kind="fish")
    # eller har gram i navnet/variantLabel. Eksempler: «Fiskekaker 2 stk» (ikke
    # «500 g»), «Fiskesuppe 1 beger» (ikke «75 g», som ble parset fra
    # variantLabel «Torsk + Kveite (75 g hver)»). Holder e-post i synk med admin.
    COUNT_UNITS = ("stk", "beger", "pk", "pakke", "boks", "glass", "stykk")
    _ul = str(item.get("unitLabel") or "").lower()              # linjens eget felt (som admin bruker)
    if _ul not in COUNT_UNITS:
        _slug = (item.get("slug") or "").strip()               # reserve: slå opp i overrides
        _prod = _overrides.get(_slug) if _slug else None
        if _prod:
            _ul = str(_prod.get("unitLabel") or "").lower()
    is_count = (unit in COUNT_UNITS) or (_ul in COUNT_UNITS) \
        or (str(item.get("kind") or "").lower() == "unit")
    if is_count:
        # Telle-vare: tallet i parentes er antallet («Sjøkreps (2)» → 2), ellers qty.
        _p = _amount_from_parens(item.get("variantLabel") or item.get("variant") or item.get("name") or "")
        count = _p if _p else qty
        count_int = int(count) if count == int(count) else count
        disp = _ul if _ul in COUNT_UNITS else (unit if unit in COUNT_UNITS else "stk")
        return f"{count_int} {disp}"
    # Kanonisk totalvekt fra checkout (`grams` = vekt × antall, satt av
    # havoyet.no) — autoritativ når den finnes. Allerede total, skal ikke
    # skaleres med qty.
    try:
        canon = float(item.get("grams") or 0)
    except (TypeError, ValueError):
        canon = 0
    if canon > 0 and unit != "stk":
        if canon >= 1000:
            kg_s = f"{canon/1000:.2f}".rstrip("0").rstrip(".").replace(".", ",")
            return f"{kg_s} kg"
        return f"{int(round(canon))} g"
    variant = item.get("variantLabel") or item.get("variant") or item.get("name") or ""
    grams = _grams_from_text(variant)
    parens = _amount_from_parens(variant)
    if unit != "stk":
        # Eksplisitt vekt-enhet (admin/manuell ordre): qty ER mengden i den
        # enheten — «10 kg» betyr 10 kg totalt. Vi skal IKKE gange qty med vekt
        # parset fra variantLabel (det dobbelteller: qty=10 × «10 kg» = 100 kg).
        # Kanonisk `grams` fra nettside-checkout er allerede returnert over.
        if unit in ("kg", "g"):
            grams_total = qty * 1000 if unit == "kg" else qty
            if grams_total >= 1000:
                kg_s = f"{grams_total/1000:.2f}".rstrip("0").rstrip(".").replace(".", ",")
                return f"{kg_s} kg"
            return f"{int(round(grams_total))} g"
        # Vekt-vare uten eksplisitt enhet: bruk eksplisitt gram fra navnet, ellers tallet i parentes.
        per = grams if grams else parens
        if per:
            total = qty * per
            if total >= 1000:
                kg_s = f"{total/1000:.2f}".rstrip("0").rstrip(".").replace(".", ",")
                return f"{kg_s} kg"
            return f"{int(round(total))} g"
    else:
        # Stk-vare: tallet i parentes er antallet ("Sjøkreps (2)" → 2 stk).
        count = parens if parens else qty
        count_int = int(count) if count == int(count) else count
        return f"{count_int} stk"
    # Fallback: manglende mengde-info — vis qty + enhet
    qty_int = int(qty) if qty == int(qty) else qty
    return f"{qty_int} {unit or 'stk'}".strip()

def _strip_amount_parens(text):
    """Fjern mengde-parentes fra slutten av navnet ("Reker (400)" → "Reker",
    "Torsk (2 – uten skinn)" → "Torsk") slik at mengden kun vises i egen
    kolonne til høyre. Beholder vekt-parenteser med enhet ("(1,5 kg)",
    "(ca. 500 g)") — de er en del av produktnavnet."""
    cleaned = _re.sub(r"\s*\(\s*\d+(?:[.,]\d+)?\s*(?:[–\-][^)]*)?\s*\)\s*$", "", str(text or "")).strip()
    return cleaned or str(text or "")

def _fmt_mengde(q, unit):
    """Formater komponent-mengde: g ≥1000 vises som kg («1,2 kg»),
    ellers «400 g» / «2 stk» / «1 pk»."""
    u = str(unit or "").lower()
    if u in ("g", "kg"):
        grams = q * 1000 if u == "kg" else q
        if grams >= 1000:
            kg_s = f"{grams/1000:.2f}".rstrip("0").rstrip(".").replace(".", ",")
            return f"{kg_s} kg"
        return f"{int(round(grams))} g"
    lbl = {"stk": "stk", "pakke": "pk"}.get(u, u or "stk")
    q_int = int(q) if q == int(q) else q
    return f"{q_int} {lbl}"

def _innhold_linjer(v):
    """Komponent-linjer for kasser: custom builder (boxSelection) viser
    kundens valgte mengde per art, faste kasser (innholdValgt) viser
    eksakt valgt innhold. Tilbehør listes til slutt. Mengder ganges med
    antall kasser (line-qty) — samme regel som admin-visningen."""
    try:
        line_qty = float(v.get("qty") or v.get("quantity") or 1)
    except (TypeError, ValueError):
        line_qty = 1
    linjer = []
    for s in (v.get("boxSelection") or []):
        if not isinstance(s, dict):
            continue
        navn_s = s.get("navn") or s.get("name") or ""
        if not navn_s:
            continue
        if s.get("variant"):
            navn_s += f" ({s['variant']})"
        try:
            q = float(s.get("qty") or 0)
        except (TypeError, ValueError):
            q = 0
        # Legacy-valg uten qty/unit: vis kun navnet
        linjer.append(f"{navn_s} — {_fmt_mengde(q * line_qty, s.get('unit'))}" if q > 0 else navn_s)
    for it in (v.get("innholdValgt") or []):
        if not isinstance(it, dict) or not it.get("label"):
            continue
        try:
            tot = float(it.get("total") or 0)
        except (TypeError, ValueError):
            tot = 0
        linjer.append(f"{it['label']} — {_fmt_mengde(tot * line_qty, it.get('unit') or 'g')}" if tot > 0 else it["label"])
    # Fiskesuppe: hvilken fisk (1–2 arter) som er valgt, med gram per art
    # (150 g totalt, delt likt ved 2). Slik ser kjøkkenet/kunden hva som er i
    # suppen — antallet supper står i mengde-kolonnen («1 beger»).
    for f in (v.get("fiskesuppeValg") or []):
        if not isinstance(f, dict):
            continue
        navn_f = f.get("navn") or f.get("name") or f.get("slug") or ""
        if not navn_f:
            continue
        try:
            g = float(f.get("gram") or 0)
        except (TypeError, ValueError):
            g = 0
        linjer.append(f"{navn_f} — {_fmt_mengde(g * line_qty, 'g')}" if g > 0 else navn_f)
    tilbehor = [str(t) for t in (v.get("tilbehorValgt") or []) if t]
    if tilbehor:
        linjer.append("Tilbehør: " + ", ".join(tilbehor))
    return linjer


def _format_order_email_html(order, change_summary="", event=""):
    """Pen HTML-versjon av admin-ordre-varselet — strukturert med tabeller,
    farger og tydelige seksjoner som ikke kollapser i Gmail/Outlook.
    Returnerer en `<div>` som passer å sende rett som e-post-body."""
    import html as _html

    nr = order.get("ordrenr") or order.get("name") or order.get("id") or "?"
    raw_kunde = order.get("kunde") if isinstance(order.get("kunde"), dict) else {}
    navn = raw_kunde.get("navn") or raw_kunde.get("name") or order.get("customer") or "Ukjent"
    tlf  = raw_kunde.get("tlf") or raw_kunde.get("phone") or order.get("phone") or ""
    adr  = raw_kunde.get("adresse") or raw_kunde.get("address") or ""
    postnr = raw_kunde.get("postnr") or ""
    poststed = raw_kunde.get("poststed") or ""
    full_adr = ", ".join(p for p in [adr, f"{postnr} {poststed}".strip()] if p) or "—"
    dag  = _norsk_dato(raw_kunde.get("leveringsdag") or order.get("delivery") or "—")
    tid  = raw_kunde.get("leveringstid") or order.get("slot") or ""
    levering = f"{dag} {tid}".strip()
    merk = raw_kunde.get("kommentar") or order.get("note") or ""
    epost = raw_kunde.get("epost") or raw_kunde.get("email") or ""
    total = order.get("sum") if order.get("sum") is not None else order.get("total")
    status = order.get("status") or "—"
    varer = order.get("varer") or order.get("items") or []

    # Banner-farge basert på event-type
    banner_color, banner_label = "#1A3A5C", "Ordre-oppdatering"
    if event == "new_order":
        banner_color, banner_label = "#2F7A4F", "Ny bestilling"
    elif event == "order_delivered":
        banner_color, banner_label = "#0d9488", "Bestilling levert"
    elif event == "order_updated":
        banner_color, banner_label = "#1A3A5C", "Ordre oppdatert"

    def esc(s): return _html.escape(str(s or ""))

    rows = []
    for v in varer:
        name = esc(_strip_amount_parens(v.get("name") or v.get("navn") or "?"))
        price = v.get("price") if v.get("price") is not None else v.get("pris")
        qty_str = esc(_qty_text(v))
        # Innholds-linjer (kasse-komponenter): uten dem viser e-posten bare
        # «Din skalldyrkasse» uten hva kunden faktisk valgte.
        innhold = _innhold_linjer(v)
        line_border = "border-bottom:none" if innhold else "border-bottom:1px solid #eee"
        rows.append(
            f'<tr>'
            f'<td style="padding:8px 10px;{line_border};color:#1a1a1a">{name}</td>'
            f'<td style="padding:8px 10px;{line_border};text-align:center;color:#0f766e;font-weight:600;font-variant-numeric:tabular-nums;white-space:nowrap">{qty_str}</td>'
            f'<td style="padding:8px 10px;{line_border};text-align:right;color:#1a1a1a;font-variant-numeric:tabular-nums">{esc(price)+" kr" if price is not None else ""}</td>'
            f'</tr>'
        )
        if innhold:
            innhold_html = "".join(
                f'<div style="padding:1px 0">+ {esc(l)}</div>' for l in innhold
            )
            rows.append(
                f'<tr><td colspan="3" style="padding:0 10px 8px 20px;'
                f'border-bottom:1px solid #eee;color:#555;font-size:13px;'
                f'line-height:1.5">{innhold_html}</td></tr>'
            )
    varer_html = "".join(rows) or (
        '<tr><td colspan="3" style="padding:14px;text-align:center;color:#999;font-style:italic">Ingen varer registrert</td></tr>'
    )

    sum_html = (f"{esc(total)} kr" if total is not None and total != "" else "—")

    change_html = ""
    if change_summary:
        change_html = (
            f'<div style="background:#FFF8E1;border-left:3px solid #C8A45C;'
            f'padding:10px 14px;margin-bottom:18px;border-radius:4px;'
            f'color:#5C4A1E;font-size:14px">{esc(change_summary)}</div>'
        )

    merk_html = ""
    if merk:
        merk_html = (
            f'<div style="margin-top:16px;padding:12px 14px;background:#F4F1EA;'
            f'border-radius:6px"><div style="font-size:11px;color:#666;'
            f'text-transform:uppercase;letter-spacing:.5px;font-weight:600;'
            f'margin-bottom:4px">Merknad</div>'
            f'<div style="color:#1a1a1a;font-size:14px;line-height:1.5;'
            f'white-space:pre-wrap">{esc(merk)}</div></div>'
        )

    return (
        f'<div style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\','
        f'Helvetica,Arial,sans-serif;background:#F4F1EA;padding:24px 12px;'
        f'color:#1a1a1a">'
        f'<div style="max-width:560px;margin:0 auto;background:#fff;'
        f'border-radius:10px;overflow:hidden;'
        f'box-shadow:0 1px 3px rgba(0,0,0,.08)">'

        # Banner
        f'<div style="background:{banner_color};color:#fff;padding:18px 22px">'
        f'<div style="font-size:11px;text-transform:uppercase;letter-spacing:1px;'
        f'opacity:.8">{banner_label}</div>'
        f'<div style="font-size:22px;font-weight:700;margin-top:4px">'
        f'Bestilling #{esc(nr)}</div>'
        f'</div>'

        # Body
        f'<div style="padding:22px">'
        f'{change_html}'

        # Info-tabell
        f'<table style="width:100%;border-collapse:collapse;font-size:14px;'
        f'margin-bottom:18px">'
        f'<tr><td style="padding:6px 0;color:#666;width:90px">Kunde</td>'
        f'<td style="padding:6px 0;color:#1a1a1a;font-weight:600">{esc(navn)}</td></tr>'
        + (f'<tr><td style="padding:6px 0;color:#666">Telefon</td>'
           f'<td style="padding:6px 0"><a href="tel:{esc(tlf)}" style="color:#1A3A5C;text-decoration:none">{esc(tlf)}</a></td></tr>' if tlf else "")
        + (f'<tr><td style="padding:6px 0;color:#666">E-post</td>'
           f'<td style="padding:6px 0"><a href="mailto:{esc(epost)}" style="color:#1A3A5C;text-decoration:none">{esc(epost)}</a></td></tr>' if epost else "")
        + f'<tr><td style="padding:6px 0;color:#666;vertical-align:top">Adresse</td>'
          f'<td style="padding:6px 0;color:#1a1a1a">{esc(full_adr)}</td></tr>'
        + f'<tr><td style="padding:6px 0;color:#666">Levering</td>'
          f'<td style="padding:6px 0;color:#1a1a1a;font-weight:600">{esc(levering)}</td></tr>'
        + f'<tr><td style="padding:6px 0;color:#666">Status</td>'
          f'<td style="padding:6px 0;color:#1a1a1a">'
          f'<span style="background:#F4F1EA;padding:2px 10px;border-radius:999px;'
          f'font-size:12px;font-weight:600">{esc(status)}</span></td></tr>'
        + f'</table>'

        # Varer
        f'<div style="font-size:11px;color:#666;text-transform:uppercase;'
        f'letter-spacing:.5px;font-weight:600;margin-bottom:6px">Varer</div>'
        f'<table style="width:100%;border-collapse:collapse;font-size:14px;'
        f'border-top:1px solid #eee">'
        f'{varer_html}'
        f'<tr><td style="padding:10px;font-weight:700;color:#1a1a1a">Sum</td>'
        f'<td></td>'
        f'<td style="padding:10px;text-align:right;font-weight:700;color:#1a1a1a;'
        f'font-size:16px;font-variant-numeric:tabular-nums">{esc(sum_html)}</td></tr>'
        f'</table>'

        f'{merk_html}'

        # Footer
        f'<div style="margin-top:22px;padding-top:14px;border-top:1px solid #eee;'
        f'font-size:12px;color:#999;text-align:center">'
        f'Sendt automatisk fra admin-systemet'
        f'</div>'

        f'</div>'   # body
        f'</div>'   # card
        f'</div>'   # wrapper
    )


def _send_customer_order_confirmation(order):
    """Send ordrebekreftelse til kunden. Kalles når ordren er fullført (PAID/NEW)."""
    if not RESEND_API_KEY:
        return False, "no-resend"
    kunde = order.get("kunde") or {}
    epost = (kunde.get("epost") or kunde.get("email") or "").strip()
    if not epost:
        return False, "no-customer-email"
    nr = order.get("ordrenr") or "?"
    navn = (kunde.get("navn") or "").strip()
    fornavn = navn.split()[0] if navn else "du"

    import html as _html
    def esc(s): return _html.escape(str(s or ""))

    varer = order.get("varer") or []
    rows = []
    for v in varer:
        vname = esc(_strip_amount_parens(v.get("name") or v.get("navn") or "?"))
        price = v.get("price") if v.get("price") is not None else v.get("pris")
        # Vekt/antall per produkt (samme regler som admin-varselet: grams →
        # vekt i navn → parentes-tall → qty + enhet)
        qty_str = esc(_qty_text(v))
        vlabel = v.get("variantLabel") or ""
        # Kasse-innhold: kunden skal se hvert produkt med valgt vekt/antall
        innhold = _innhold_linjer(v)
        line_border = "border-bottom:none" if innhold else "border-bottom:1px solid #eee"
        rows.append(
            f'<tr>'
            f'<td style="padding:8px 10px;{line_border};color:#1a1a1a">{vname}'
            f'{"<br/><span style=font-size:12px;color:#888>" + esc(vlabel) + "</span>" if vlabel else ""}</td>'
            f'<td style="padding:8px 10px;{line_border};text-align:center;color:#0f766e;font-weight:600;font-variant-numeric:tabular-nums;white-space:nowrap">{qty_str}</td>'
            f'<td style="padding:8px 10px;{line_border};text-align:right;font-variant-numeric:tabular-nums">'
            f'{esc(price)+" kr" if price is not None else ""}</td>'
            f'</tr>'
        )
        if innhold:
            innhold_html = "".join(
                f'<div style="padding:1px 0">+ {esc(l)}</div>' for l in innhold
            )
            rows.append(
                f'<tr><td colspan="3" style="padding:0 10px 8px 20px;'
                f'border-bottom:1px solid #eee;color:#555;font-size:13px;'
                f'line-height:1.5">{innhold_html}</td></tr>'
            )
    varer_html = "".join(rows)

    total = order.get("total") or 0
    fee = order.get("fee") or 0
    rabatt = order.get("rabattBelop") or 0
    summ = order.get("sum") or total
    dag = _norsk_dato(kunde.get("leveringsdag") or "—")
    tid = kunde.get("leveringstid") or ""
    adr = kunde.get("adresse") or ""
    postnr = kunde.get("postnr") or ""
    sted = kunde.get("sted") or ""
    betaling = kunde.get("betaling") or ""
    bet_label = "Vipps" if betaling == "vipps" else "Kort" if betaling == "kort" else betaling

    html_body = (
        f'<div style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\','
        f'Helvetica,Arial,sans-serif;background:#F4F1EA;padding:24px 12px;color:#1a1a1a">'
        f'<div style="max-width:560px;margin:0 auto;background:#fff;'
        f'border-radius:10px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.08)">'

        f'<div style="background:#0d9488;color:#fff;padding:22px;text-align:center">'
        f'<div style="font-size:36px;margin-bottom:8px">✓</div>'
        f'<div style="font-size:20px;font-weight:700">Takk for bestillinga, {esc(fornavn)}!</div>'
        f'<div style="font-size:13px;opacity:.85;margin-top:4px">Ordrenummer {esc(nr)}</div>'
        f'</div>'

        f'<div style="padding:22px">'

        f'<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;'
        f'padding:14px;margin-bottom:18px;text-align:center">'
        f'<div style="font-size:13px;color:#166534;font-weight:600">Levering</div>'
        f'<div style="font-size:16px;color:#1a1a1a;font-weight:700;margin-top:2px">'
        f'{esc(dag)} kl. {esc(tid)}</div>'
        f'<div style="font-size:13px;color:#666;margin-top:2px">'
        f'{esc(adr)}, {esc(postnr)} {esc(sted)}</div>'
        f'</div>'

        f'<div style="font-size:11px;color:#666;text-transform:uppercase;'
        f'letter-spacing:.5px;font-weight:600;margin-bottom:6px">Din bestilling</div>'
        f'<table style="width:100%;border-collapse:collapse;font-size:14px;'
        f'border-top:1px solid #eee">'
        f'{varer_html}'
        f'</table>'

        f'<table style="width:100%;border-collapse:collapse;font-size:14px;margin-top:8px">'
        f'<tr><td style="padding:4px 10px;color:#666">Subtotal</td>'
        f'<td style="padding:4px 10px;text-align:right">{esc(total)} kr</td></tr>'
        + (f'<tr><td style="padding:4px 10px;color:#22c55e">Rabatt</td>'
           f'<td style="padding:4px 10px;text-align:right;color:#22c55e">−{esc(rabatt)} kr</td></tr>' if rabatt else "")
        + f'<tr><td style="padding:4px 10px;color:#666">Levering</td>'
          f'<td style="padding:4px 10px;text-align:right">{"Gratis" if not fee else str(fee)+" kr"}</td></tr>'
        f'<tr><td style="padding:10px;font-weight:700;font-size:16px;border-top:2px solid #0d9488">Totalt</td>'
        f'<td style="padding:10px;text-align:right;font-weight:700;font-size:16px;'
        f'border-top:2px solid #0d9488;color:#0d9488">{esc(summ)} kr</td></tr>'
        f'</table>'

        f'<div style="margin-top:16px;font-size:13px;color:#666">Betaling: {esc(bet_label)}</div>'

        f'<div style="margin-top:24px;text-align:center">'
        f'<a href="{PUBLIC_SITE_URL}/konto" style="display:inline-block;background:#0d9488;'
        f'color:#fff;padding:12px 28px;border-radius:8px;text-decoration:none;'
        f'font-weight:600;font-size:14px">Se bestillinga di</a>'
        f'</div>'

        f'<div style="margin-top:24px;padding-top:14px;border-top:1px solid #eee;'
        f'font-size:12px;color:#999;text-align:center">'
        f'Spørsmål? Svar på denne e-posten eller ring +47 416 39 788'
        f'</div>'

        f'</div></div></div>'
    )

    subject = f"Ordrebekreftelse #{nr} — Havøyet"
    text_body = (
        f"Hei {fornavn},\n\n"
        f"Takk for bestillinga! Her er ordrebekreftelsen din.\n\n"
        f"Ordrenummer: {nr}\n"
        f"Levering: {dag} kl. {tid}\n"
        f"Adresse: {adr}, {postnr} {sted}\n"
        f"Totalt: {summ} kr\n\n"
        f"Se bestillinga di: {PUBLIC_SITE_URL}/konto\n"
    )

    ok, detail = _send_via_resend(
        CONTACT_TO, "Havøyet", subject, text_body,
        to_email=epost, reply_to=CONTACT_TO, html_body=html_body,
    )
    if ok:
        print(f"[CUSTOMER-CONFIRM] Sendt ordrebekreftelse til {epost} for #{nr}")
    else:
        print(f"[CUSTOMER-CONFIRM] Feilet for {epost}: {detail}")
    return ok, detail


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
        f"Levering: {_norsk_dato(dag)} {tid}\n"
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
    deny = _require_admin_user()
    if deny: return deny
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
    deny = _require_admin_user()
    if deny: return deny
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
    deny = _require_admin_user()
    if deny: return deny
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
    deny = _require_admin_user()
    if deny: return deny
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
    deny = _require_admin_user()
    if deny: return deny
    data = request.get_json(force=True) or {}
    target_id = data.get("id")
    targets = _admin_notifiers
    if target_id:
        targets = [n for n in _admin_notifiers if n.get("id") == target_id]
    ts = datetime.now(_OSLO_TZ).strftime("%Y-%m-%d %H:%M")
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
    deny = _require_admin_user()
    if deny: return deny
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
    if _rate_limited(f"contact:{_client_ip()}", 6, 600):
        return jsonify({"error": "For mange meldinger på kort tid. Vent litt før du sender igjen."}), 429
    navn    = (data.get("navn") or "").strip()
    epost   = (data.get("epost") or "").strip()
    melding = (data.get("melding") or "").strip()
    emne    = (data.get("emne") or f"[Kontakt] Ny henvendelse fra {navn or 'Havøyet-nettside'}").strip()

    if not navn or not epost or not melding:
        return jsonify({"ok": False, "error": "Navn, e-post og melding er påkrevet"}), 400

    # Plain-text fallback (for e-postklienter som ikke renderer HTML)
    body = (
        f"Ny melding fra Havøyet-nettsiden\n"
        f"{'-'*54}\n"
        f"Navn:     {navn}\n"
        f"E-post:   {epost}\n"
        f"Mottatt:  {datetime.now(_OSLO_TZ).strftime('%Y-%m-%d %H:%M')}\n\n"
        f"Melding:\n{melding}\n\n"
        f"Svar på denne e-posten for å nå {navn} direkte — Reply-To peker til {epost}.\n"
    )
    html_body = _format_message_email_html(
        source="Kontaktskjema",
        navn=navn, epost=epost, melding=melding, emne=emne,
    )
    # Send via varselssystemet så ALLE admin-mottakere med «Ny melding» avkrysset
    # får den — e-post med Reply-To til kunden (svar går direkte til kunden),
    # pluss SMS/push/telegram etter mottakerens kanal-avkrysninger. Fortsatt kun
    # ÉN e-post per mottaker (den gamle dubletten oppsto fordi både kontakt-mail
    # og varsel gikk til samme adresse). Logger til disk som før (backup).
    try:
        with open(CONTACT_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "at": datetime.now().isoformat(),
                "from": epost, "name": navn,
                "subject": emne, "body": body, "to": "admin-mottakere (new_message)",
            }, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[CONTACT] Kunne ikke logge: {e}")
    sent = _notify_admins("new_message", emne, body, html_body=html_body, reply_to=epost)
    if sent:
        return jsonify({"ok": True, "detail": f"sent-to-{sent}-admin-channels"})
    # Fallback: ingen mottakere registrert (eller alle sendinger feilet) —
    # bruk den gamle faste kontakt-mailen til CONTACT_TO så meldingen aldri mistes.
    ok, detail = _send_contact_mail(epost, navn, emne, body, html_body=html_body)
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
        old_status = _manual_orders[existing_idx].get("status")
        merged = dict(_manual_orders[existing_idx])
        for k, v in data.items():
            # Tillat status å oppdateres fra AWAITING_PAYMENT → PAID/NEW etc.
            # Ikke skriv over med tom/None.
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            merged[k] = v
        _manual_orders[existing_idx] = merged
        _save_sync_state()
        _mark_carts_converted_for_order(merged)
        # Fyr av "new_order"-varsel hvis ordren nettopp gikk fra pending → fullført
        # (typisk AWAITING_PAYMENT → PAID etter at Vipps/Stripe bekreftet betaling).
        # Dette er det første tidspunktet admin skal få bestillingsvarsel.
        if _is_pending_order_status(old_status) and not _is_pending_order_status(merged.get("status")):
            try:
                navn_n = ((merged.get("kunde") or {}).get("navn") or "?").strip()
                _notify_admins(
                    "new_order",
                    f"[Havøyet] Ny bestilling #{target_id} — {navn_n} ({merged.get('sum', 0)} kr)",
                    _format_order_lines(merged),
                    html_body=_format_order_email_html(merged, "Bestillingen er fullført og betalt.", "new_order"),
                )
            except Exception as e:
                print(f"[ADMIN-NOTIFY] new_order varsel (pending→fullført) feilet: {e}")
            # Kundebekreftelse — sendes når ordren flippar frå pending til fullført
            try:
                _send_customer_order_confirmation(merged)
            except Exception as e:
                print(f"[CUSTOMER-CONFIRM] feilet (pending→fullført): {e}")
        return jsonify({"ok": True, "ordrenr": target_id, "updated": True})

    # Ny ordre — legg til i state. Fjern eventuelle tombstones for samme nr
    # så ordren faktisk vises (relevant hvis admin slettet ordren før kunden
    # gjorde retry, eller hvis vi gjenbruker et tidligere brukt ordrenummer).
    for k in _order_keys(data):
        _order_tombstones.pop(k, None)
    _manual_orders.append(data)
    _save_sync_state()
    _mark_carts_converted_for_order(data)

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
                navn_s = s.get("navn") or s.get("name") or ""
                if s.get("variant"):
                    navn_s += f" ({s['variant']})"
                # Valgt mengde per art fra builderen (qty + unit): g-arter i
                # gram, stk/pakke i antall — vis «Reker 400 g», «Østers 4 stk».
                q = s.get("qty")
                if q:
                    u = {"g": "g", "kg": "kg", "stk": "stk", "pakke": "pk"}.get(s.get("unit") or "", "")
                    navn_s += f" — {q} {u}".rstrip()
                lines.append(f"      + {navn_s}")
    lines.append("-" * 54)
    lines.append(f"{'Subtotal':<44} {data.get('total', 0):>6} kr")
    lines.append(f"{'Levering':<44} {data.get('fee', 0):>6} kr")
    lines.append(f"{'TOTAL':<44} {data.get('sum', 0):>6} kr")
    lines.append("=" * 54)
    lines.append("")
    lines.append(f"Svar på denne e-posten for å svare {navn} direkte.")
    lines.append("Ordren er også synlig i admin-panelet.")

    # NB: Tidligere gikk det OGSÅ en direkte «[Bestilling X]»-mail til admin her
    # via _send_contact_mail — fjernet 2026-06-06 fordi admin da fikk to varsler
    # for samme ordre. _notify_admins under er nå eneste admin-varsel (og bruker
    # samme HTML-mal med kasse-innhold). Kunden får egen ordrebekreftelse.

    # Send admin-varsel (e-post / SMS / ntfy / Telegram) til registrerte mottakere.
    # Hopp over hvis ordren fortsatt er i en pre-betalings-status (AWAITING_PAYMENT) —
    # da fyrer varselet først når merge-pathen oppdager pending→PAID-overgangen, eller
    # Stripe-webhooken bekrefter betalingen. Slik unngår vi falske varsler om bestillinger
    # som aldri blir fullført.
    if not _is_pending_order_status(data.get("status")):
        try:
            _notify_admins(
                "new_order",
                f"[Havøyet] Ny bestilling #{data['ordrenr']} — {navn} ({data.get('sum', 0)} kr)",
                "\n".join(lines),
                html_body=_format_order_email_html(
                    data,
                    "Det er kommet inn en ny bestilling.",
                    "new_order",
                ),
            )
        except Exception as e:
            print(f"[ADMIN-NOTIFY] new_order varsel feilet: {e}")
        # Kundebekreftelse — sendast med ein gong for nye fullførte ordrar
        try:
            _send_customer_order_confirmation(data)
        except Exception as e:
            print(f"[CUSTOMER-CONFIRM] feilet (ny ordre): {e}")

    return jsonify({"ok": True, "mail": "admin-notify", "ordrenr": data["ordrenr"], "order": data})


@app.route("/api/orders/<ordrenr>/resend-confirmation", methods=["POST"])
def api_resend_customer_confirmation(ordrenr):
    """Send (eller re-send) ordrebekreftelse til kunden for ein eksisterande ordre."""
    for o in _manual_orders:
        if str(o.get("ordrenr") or o.get("id") or "").strip() == str(ordrenr):
            ok, detail = _send_customer_order_confirmation(o)
            return jsonify({"ok": ok, "detail": detail})
    return jsonify({"ok": False, "error": "Ordre ikkje funne"}), 404


# ── KUNDE-KONTO: ordrehistorikk + favoritter (identifiseres via e-post) ──────
def _overlay_packing_avail(o):
    """Returner en kopi av ordren der hver varelinjes `avail` (og veide vekt)
    speiler packing-state fra pakke-/admin-flyten. Uten dette viste kundesiden
    ordrelinjas opprinnelige `avail` (ofte 'unsure'/Bekreftes) selv om admin
    hadde markert varen Tilgjengelig i pakke-staten → admin og nettside usynk."""
    keys = [str(o.get("id") or ""), str(o.get("ordrenr") or "")]
    pstate = None
    for k in keys:
        cand = _packing_state.get(k)
        if k and isinstance(cand, (dict, list)):
            pstate = cand
            break
    if not pstate:
        return o
    varer = o.get("varer") or []
    if not isinstance(varer, list):
        return o
    new_varer = []
    for i, v in enumerate(varer):
        vv = dict(v) if isinstance(v, dict) else v
        # packing-state per ordre kan være en LISTE ([{...}, ...]) eller et
        # objekt keyet på indeks ("0","1"). Håndter begge.
        if isinstance(pstate, list):
            meta = pstate[i] if i < len(pstate) else None
        else:
            meta = pstate.get(str(i)) or pstate.get(i)
        if isinstance(vv, dict) and isinstance(meta, dict):
            if meta.get("avail"):
                vv["avail"] = meta["avail"]
                vv["avail_confirmed"] = True   # admin/pakke har eksplisitt satt status → autoritativ

            if meta.get("weight") not in (None, ""):
                vv["packed_weight"] = meta["weight"]
                if meta.get("weightUnit"):
                    vv["packed_weight_unit"] = meta["weightUnit"]
            if meta.get("confirmedReplacement"):
                vv["confirmedReplacement"] = meta["confirmedReplacement"]
        new_varer.append(vv)
    oc = dict(o)
    oc["varer"] = new_varer
    return oc


def _orders_for_email(email):
    """Samler alle ordre som matcher en e-postadresse."""
    email = (email or "").strip().lower()
    if not email:
        return []
    orders = []
    # Manuelle ordre lagret via checkout-skjema. Speil packing-state-avail inn
    # på varelinjene så kundesiden viser samme tilgjengelighet som admin/pakke.
    for o in _manual_orders:
        kunde_epost = ((o.get("kunde") or {}).get("epost") or "").lower()
        if kunde_epost == email:
            orders.append(_overlay_packing_avail(o))
    # Sorter nyeste først
    def _key(o):
        return o.get("dato") or o.get("created_at") or ""
    orders.sort(key=_key, reverse=True)
    return orders


def _require_account_access(requested_email):
    """Returnerer (email, None) hvis forespørselen har lov til å lese/endre
    kontodata for `requested_email`, ellers (None, (json_response, status)).

    Regel: kunden må være innlogget (gyldig Bearer-token) OG token-eposten må
    matche den forespurte e-posten. Admin (rolle=admin / X-Admin-Token) får lese
    alle. Uten dette returnerte endepunktet tidligere HVEM SOM HELST sin
    ordrehistorikk fra bare ?email= i URL-en (IDOR / persondata-lekkasje)."""
    user, _ = _user_from_request()
    requested = (requested_email or "").strip().lower()
    if _is_admin_request():
        # Admin må fortsatt oppgi hvilken kunde de slår opp.
        if not requested:
            return None, (jsonify({"error": "E-post mangler"}), 400)
        return requested, None
    if not user:
        return None, (jsonify({"error": "Innlogging kreves"}), 401)
    owner = (user.get("email") or "").strip().lower()
    # Tom ?email = "min egen konto" for en innlogget bruker.
    if not requested:
        requested = owner
    if requested != owner:
        return None, (jsonify({"error": "Ingen tilgang til denne kontoen"}), 403)
    return requested, None


@app.route("/api/customer/account")
def api_customer_account():
    """?email=... → returnerer ordrehistorikk + favoritter for kunden.
    Krever innlogging + eierskap (eller admin) — se _require_account_access."""
    email, deny = _require_account_access(request.args.get("email"))
    if deny:
        return deny
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
    email, deny = _require_account_access(data.get("email"))
    if deny:
        return deny
    slug   = (data.get("slug") or "").strip()
    action = (data.get("action") or "toggle").strip()
    if not slug:
        return jsonify({"error": "slug er påkrevet"}), 400
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
            o["updatedAt"] = int(time.time())
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
                    change_summary + "\n\n" + _format_order_lines(o),
                    html_body=_format_order_email_html(o, change_summary, "order_delivered"),
                )
                _notify_customer_order_update(o, "order_delivered", change_summary)
            elif old_status != new_status:
                _notify_admins(
                    "order_updated",
                    f"[Havøyet] Bestilling #{nr} oppdatert",
                    change_summary + "\n\n" + _format_order_lines(o),
                    html_body=_format_order_email_html(o, change_summary, "order_updated"),
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
            # Stemple redigeringen — POST-mergen bruker updatedAt til å hindre
            # at en stale iPad-liste overskriver denne endringen.
            o["updatedAt"] = int(time.time())
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
                        change_summary + "\n\n" + _format_order_lines(o),
                        html_body=_format_order_email_html(o, change_summary, "order_delivered"),
                    )
                    _notify_customer_order_update(o, "order_delivered", change_summary)
                else:
                    _notify_admins(
                        "order_updated",
                        f"[Havøyet] Bestilling #{nr} oppdatert",
                        change_summary + "\n\n" + _format_order_lines(o),
                        html_body=_format_order_email_html(o, change_summary, "order_updated"),
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

def _vipps_capture(reference, amount_ore):
    """Fang (capture) ein autorisert Vipps-betaling slik at pengane faktisk blir trekte."""
    url = f"{VIPPS_API_BASE}/epayment/v1/payments/{reference}/capture"
    body = {"modificationAmount": {"currency": "NOK", "value": amount_ore}}
    try:
        r = requests.post(url, headers=_vipps_headers(idempotency_key=f"cap-{reference}"),
                          json=body, timeout=15)
        if r.status_code < 300:
            print(f"[VIPPS] Captured {reference} ({amount_ore/100:.2f} kr)")
            return True
        print(f"[VIPPS] Capture feilet for {reference}: {r.status_code} {r.text[:200]}")
        return False
    except Exception as e:
        print(f"[VIPPS] Capture exception for {reference}: {e}")
        return False


def _vipps_refund(reference, amount_ore, idem_suffix=""):
    """Refunder ein capturert Vipps-betaling — full eller delvis (amount i øre).
    Vipps tillater flere delvise refusjoner så lenge summen ikke overstiger captured.
    Returnerer (ok, body_dict, status_code)."""
    url = f"{VIPPS_API_BASE}/epayment/v1/payments/{reference}/refund"
    body = {"modificationAmount": {"currency": "NOK", "value": int(amount_ore)}}
    # Unik idempotency-nøkkel per delvis refusjon — la admin kunne refundere flere
    # ganger uten at Vipps avviser som duplikat.
    idem = f"ref-{reference}-{idem_suffix or int(time.time()*1000)}"
    try:
        r = requests.post(url, headers=_vipps_headers(idempotency_key=idem),
                          json=body, timeout=20)
        try:
            data = r.json() if r.content else {}
        except Exception:
            data = {"raw": r.text}
        if r.status_code < 300:
            print(f"[VIPPS] Refunded {reference} ({amount_ore/100:.2f} kr)")
            return True, data, r.status_code
        print(f"[VIPPS] Refund feilet for {reference}: {r.status_code} {str(data)[:200]}")
        return False, data, r.status_code
    except Exception as e:
        print(f"[VIPPS] Refund exception for {reference}: {e}")
        return False, {"error": str(e)}, 502


def _find_vipps_reference_for_order(ordrenr):
    """Slår opp Vipps payment-referanse for en gitt ordrenr i lokal payments-fil.
    Returnerer (reference, rec_dict) eller (None, None)."""
    payments = _vipps_load_payments() or {}
    target = str(ordrenr).strip()
    for ref, rec in payments.items():
        if str(rec.get("ordrenr") or "").strip() == target:
            return ref, rec
    return None, None

@app.route("/api/vipps/capture/<reference>", methods=["POST"])
def api_vipps_capture_endpoint(reference):
    """Manuelt capture-endepunkt for admin."""
    if not _vipps_configured():
        return jsonify({"error": "Vipps er ikke konfigurert"}), 503
    payments = _vipps_load_payments()
    rec = payments.get(reference, {})
    amount = int(rec.get("amount", 0))
    if amount <= 0:
        data = request.get_json(silent=True) or {}
        amount = int(data.get("amount", 0))
    if amount <= 0:
        return jsonify({"error": "Ukjent beløp — oppgi amount i øre"}), 400
    ok = _vipps_capture(reference, amount)
    if ok:
        if reference in payments:
            payments[reference]["state"] = "CAPTURED"
            payments[reference]["captured_at"] = time.time()
            _vipps_save_payments(payments)
        return jsonify({"ok": True, "state": "CAPTURED"})
    return jsonify({"ok": False, "error": "Capture feilet"}), 502

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
    deny = _check_payment_amount(ordrenr, amount)
    if deny: return deny
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
    """Hent status for ein Vipps-betaling frå Vipps API.
    Oppdaterer også ordren til PAID viss Vipps bekreftar (sikkerheitsnett
    i tillegg til callback-webhooken)."""
    global _manual_orders
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

        # Auto-capture: fang betalinga med ein gong den er autorisert
        if state == "AUTHORIZED":
            amount = payments[reference].get("amount", 0)
            if amount > 0:
                captured = _vipps_capture(reference, amount)
                if captured:
                    state = "CAPTURED"
                    payments[reference]["state"] = "CAPTURED"
                    payments[reference]["captured_at"] = time.time()
                    _vipps_save_payments(payments)

        # Oppdater ordren til PAID — same logikk som callback-handleren
        if state in _VIPPS_PAID_STATES:
            ordrenr = payments[reference].get("ordrenr")
            if ordrenr:
                for o in _manual_orders:
                    if str(o.get("ordrenr") or o.get("id") or "").strip() == str(ordrenr):
                        was_pending = _is_pending_order_status(o.get("status"))
                        if was_pending or o.get("status") != "PAID":
                            o["status"] = "PAID"
                            o["paymentStatus"] = "paid"
                            o["paid_at"] = datetime.now().isoformat()
                            o["vipps_reference"] = reference
                            _save_sync_state()
                            if was_pending:
                                try:
                                    navn_n = ((o.get("kunde") or {}).get("navn") or "?").strip()
                                    _notify_admins(
                                        "new_order",
                                        f"[Havøyet] Ny bestilling #{ordrenr} — {navn_n} ({o.get('sum', 0)} kr)",
                                        _format_order_lines(o),
                                        html_body=_format_order_email_html(o, "Bestillingen er fullført og betalt med Vipps.", "new_order"),
                                    )
                                except Exception as e:
                                    print(f"[ADMIN-NOTIFY] new_order varsel (Vipps status-poll) feilet: {e}")
                                try:
                                    _send_customer_order_confirmation(o)
                                except Exception as e:
                                    print(f"[CUSTOMER-CONFIRM] feilet (Vipps status-poll): {e}")
                        break

    return jsonify({"reference": reference, "state": state, "vipps": body})

@app.route("/api/vipps/status-by-order/<ordrenr>")
def api_vipps_status_by_order(ordrenr):
    """Oppslag via ordrenr for tilfelle der frontend mista sessionStorage (mobil app-bytte).
    Finn Vipps-referansen frå betalingsloggen, sjekk status, oppdater ordren viss betalt."""
    global _manual_orders
    if not _vipps_configured():
        return jsonify({"error": "Vipps er ikke konfigurert"}), 503

    payments = _vipps_load_payments()
    reference = None
    for ref, rec in payments.items():
        if rec.get("ordrenr") == ordrenr:
            reference = ref
            break
    if not reference:
        return jsonify({"error": "Fant ingen Vipps-betaling for denne ordren", "state": "UNKNOWN"}), 404

    url = f"{VIPPS_API_BASE}/epayment/v1/payments/{reference}"
    try:
        r = requests.get(url, headers=_vipps_headers(), timeout=10)
        body = r.json() if r.content else {}
    except Exception as e:
        return jsonify({"error": f"Kunne ikke kontakte Vipps: {e}", "state": "UNKNOWN"}), 502

    state = body.get("state", "UNKNOWN")
    payments[reference]["state"] = state
    payments[reference]["last_check"] = time.time()
    _vipps_save_payments(payments)

    # Auto-capture ved AUTHORIZED
    if state == "AUTHORIZED":
        amount = payments[reference].get("amount", 0)
        if amount <= 0:
            amount = int(body.get("amount", {}).get("value", 0))
        if amount > 0:
            captured = _vipps_capture(reference, amount)
            if captured:
                state = "CAPTURED"
                payments[reference]["state"] = "CAPTURED"
                payments[reference]["captured_at"] = time.time()
                _vipps_save_payments(payments)

    # Oppdater ordren til PAID viss Vipps bekreftar
    order_data = None
    if state in _VIPPS_PAID_STATES:
        for o in _manual_orders:
            if str(o.get("ordrenr") or o.get("id") or "").strip() == str(ordrenr):
                was_pending = _is_pending_order_status(o.get("status"))
                if was_pending or o.get("status") != "PAID":
                    o["status"] = "PAID"
                    o["paymentStatus"] = "paid"
                    o["paid_at"] = datetime.now().isoformat()
                    o["vipps_reference"] = reference
                    _save_sync_state()
                    if was_pending:
                        try:
                            navn_n = ((o.get("kunde") or {}).get("navn") or "?").strip()
                            _notify_admins(
                                "new_order",
                                f"[Havøyet] Ny bestilling #{ordrenr} — {navn_n} ({o.get('sum', 0)} kr)",
                                _format_order_lines(o),
                                html_body=_format_order_email_html(o, "Bestillingen er fullført og betalt med Vipps.", "new_order"),
                            )
                        except Exception as e:
                            print(f"[ADMIN-NOTIFY] new_order varsel (Vipps status-by-order) feilet: {e}")
                        try:
                            _send_customer_order_confirmation(o)
                        except Exception as e:
                            print(f"[CUSTOMER-CONFIRM] feilet (Vipps status-by-order): {e}")
                order_data = o
                break

    return jsonify({"reference": reference, "state": state, "order": order_data})

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
            period_label = _nice_period_label(period_from, period_to)
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
    # GYLDIGHET: Shopify-import-ordre (source=shopify, orders_export) er de SAMME
    # salgene som allerede ligger i Shopify-kort-CSV og Vipps-via-nettside-CSV.
    # De telles derfor IKKE i omsetning (ellers dobbelttelling — #1103–1109 lå
    # både som ordre OG som kort/Vipps-betaling). Innkjøpskost beholdes likevel
    # (de er reelt vareforbruk, og matcher omsetningen som fanges via betalingene).
    def _is_shopify_src(o):
        return str(o.get("source") or "").lower() == "shopify"
    web_orders = [o for o in _manual_orders
                  if str(o.get("ordrenr") or o.get("id")) in paid_set
                  and not _is_shopify_src(o)]
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
    # Median-ordresum — robust mot ekstreme enkeltordrer (samme transaksjons-sett
    # som snittet teller: web + vipps + kort-charges, refusjoner utelatt).
    _period_amounts = (
        [_order_total_kr(o) for o in web_period_rows]
        + [((r.get("amount_ore") or 0) / 100.0) for r in vipps_period_rows]
        + [_card_signed_kr(r) for r in card_period_charges]
    )
    _period_amounts = sorted(a for a in _period_amounts if a is not None)
    if _period_amounts:
        _n = len(_period_amounts)
        _mid = _n // 2
        period_median_kr = (
            _period_amounts[_mid] if _n % 2 == 1
            else (_period_amounts[_mid - 1] + _period_amounts[_mid]) / 2.0
        )
    else:
        period_median_kr = 0.0

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
            "median_kr":        round(period_median_kr, 2),
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


def _economy_report_html(period_label, pf, pt, org_nr, mva_pst,
                         brutto, netto, mva, kost, ntx,
                         by_system, by_kilde, excluded_rows, sum_excl, rows):
    """Render omsetningsrapporten som en pen, utskrifts-/PDF-vennlig HTML-side
    (samme stil som en kasserer-rapport: firma-header, rapportdetaljer,
    sammendrag, omsetning per kilde med MVA-splitt, og transaksjonsliste)."""
    import html as _h

    def esc(v):
        return _h.escape("" if v is None else str(v))

    def kr(v):
        s = ("%0.2f" % float(v or 0))
        # 27020.00 -> 27 020,00 (norsk: mellomrom tusen, komma desimal)
        neg = s.startswith("-")
        s = s.lstrip("-")
        intp, dec = s.split(".")
        grp = ""
        while len(intp) > 3:
            grp = " " + intp[-3:] + grp
            intp = intp[:-3]
        intp = intp + grp
        return ("−" if neg else "") + intp + "," + dec + " kr"

    gen = datetime.now().strftime("%d.%m.%Y - %H:%M")
    eksport = "erik@havoyet.no"
    mva_rate = 1 + (float(mva_pst) / 100.0)

    # Per kilde med MVA-splitt
    kilde_sorted = sorted(by_kilde.items(), key=lambda kv: -kv[1]["kr"])
    kilde_rows = ""
    for k, g in kilde_sorted:
        b = g["kr"]; nv = b / mva_rate; mv = b - nv
        kilde_rows += (
            "<tr><td>{}</td><td class='num'>{}</td><td class='num'>{}</td>"
            "<td class='num'>{}</td><td class='num strong'>{}</td></tr>"
        ).format(esc(k), g["n"], kr(nv), kr(mv), kr(b))
    kilde_rows += (
        "<tr class='sumrow'><td>Sum</td><td class='num'>{}</td><td class='num'>{}</td>"
        "<td class='num'>{}</td><td class='num strong'>{}</td></tr>"
    ).format(ntx, kr(netto), kr(mva), kr(brutto))

    # Per system
    system_rows = ""
    for k, g in sorted(by_system.items(), key=lambda kv: -kv[1]["kr"]):
        system_rows += "<tr><td>{}</td><td class='num'>{}</td><td class='num strong'>{}</td></tr>".format(
            esc(k), g["n"], kr(g["kr"]))

    # Ekskludert-notis
    excl_html = ""
    if excluded_rows:
        excl_html = (
            "<div class='note'><strong>Ikke tatt med (unngår dobbelttelling):</strong> "
            "{} Shopify-import-ordre à totalt {} — samme salg ligger allerede i "
            "Shopify kort / Vipps via nettside.</div>"
        ).format(len(excluded_rows), kr(sum_excl))

    # Transaksjonsliste (kun de som teller i omsetning, kronologisk)
    tx_rows = ""
    for r in rows:
        if not r["counted"]:
            continue
        tx_rows += (
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td>"
            "<td>{}</td><td class='num strong'>{}</td></tr>"
        ).format(
            esc(r["date"]), esc(r["system"]), esc(r["kilde"]), esc(r["ref"]),
            esc(r["kunde"] or r["detalj"] or "—"), esc(r["type"]), kr(r["belop"]))

    csv_qs = "?from={}&to={}&format=csv".format(
        pf.strftime("%Y-%m-%d"), pt.strftime("%Y-%m-%d"))
    title = "Omsetningsrapport Havøyet AS – " + period_label

    css = """
    *{box-sizing:border-box;}
    body{margin:0;background:#e9edf1;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;color:#2b3440;font-size:13px;-webkit-print-color-adjust:exact;print-color-adjust:exact;}
    .sheet{max-width:980px;margin:24px auto;background:#fff;padding:48px 56px 64px;box-shadow:0 1px 4px rgba(0,0,0,.12);}
    h1{font-size:30px;font-weight:600;color:#3a4654;margin:0 0 2px;letter-spacing:.2px;}
    h2{font-size:18px;font-weight:700;color:#3a4654;margin:0 0 28px;}
    h3{font-size:13px;font-weight:700;color:#3a4654;margin:34px 0 12px;text-transform:none;}
    .meta{display:grid;grid-template-columns:150px 1fr;row-gap:7px;margin:6px 0 8px;font-size:13px;}
    .meta .k{font-weight:700;color:#3a4654;}
    .meta .v{color:#5b6675;}
    .cards{display:flex;gap:14px;margin:22px 0 6px;flex-wrap:wrap;}
    .card{flex:1;min-width:180px;border:1px solid #e3e8ee;border-radius:10px;padding:14px 16px;background:#fafbfc;}
    .card .lbl{font-size:11px;font-weight:700;letter-spacing:.4px;text-transform:uppercase;color:#8a95a3;margin-bottom:6px;}
    .card .val{font-size:22px;font-weight:700;color:#222b36;font-variant-numeric:tabular-nums;}
    .card.accent{background:#eafaf6;border-color:#bfe9df;}
    .card.accent .val{color:#0d8f74;}
    table{width:100%;border-collapse:collapse;margin:4px 0 8px;}
    th{text-align:left;font-size:11px;font-weight:700;letter-spacing:.3px;text-transform:uppercase;color:#8a95a3;padding:10px 10px;border-bottom:1px solid #d7dee6;}
    td{padding:11px 10px;border-bottom:1px solid #eef2f5;color:#3f4a58;font-variant-numeric:tabular-nums;}
    td.num,th.num{text-align:right;}
    td.strong{font-weight:700;color:#222b36;}
    tr.sumrow td{border-top:2px solid #d7dee6;border-bottom:none;font-weight:700;color:#222b36;background:#fafbfc;}
    .note{margin:14px 0;padding:11px 14px;background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;color:#9a3412;font-size:12px;}
    .foot{margin-top:26px;padding-top:14px;border-top:1px solid #eef2f5;color:#8a95a3;font-size:11px;line-height:1.6;}
    .bar{position:sticky;top:0;background:#2b3440;color:#fff;padding:10px 16px;display:flex;gap:10px;align-items:center;justify-content:center;font-size:13px;}
    .bar button,.bar a{font:inherit;border:none;border-radius:7px;padding:8px 16px;cursor:pointer;text-decoration:none;font-weight:600;}
    .bar button{background:#16b894;color:#fff;}
    .bar a{background:#46525f;color:#fff;}
    @media print{.bar{display:none;}body{background:#fff;}.sheet{box-shadow:none;margin:0;max-width:none;padding:0 8px;}}
    """

    html = (
        "<!doctype html><html lang='no'><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<title>" + esc(title) + "</title><style>" + css + "</style></head><body>"
        "<div class='bar'>"
        "<button onclick='window.print()'>🖨️ Skriv ut / Lagre som PDF</button>"
        "<a href='" + esc(csv_qs) + "'>⬇ Last ned CSV (Excel)</a>"
        "</div>"
        "<div class='sheet'>"
        "<h1>Havøyet AS</h1><h2>Omsetningsrapport</h2>"
        "<h3>Rapportdetaljer</h3>"
        "<div class='meta'>"
        "<div class='k'>Sted:</div><div class='v'>Havøyet AS</div>"
        "<div class='k'>Org.nr:</div><div class='v'>" + esc(org_nr) + "</div>"
        "<div class='k'>Område:</div><div class='v'>Norge</div>"
        "<div class='k'>Periode:</div><div class='v'>" + esc(period_label) + " ("
        + pf.strftime("%d.%m.%Y") + " – " + pt.strftime("%d.%m.%Y") + ")</div>"
        "<div class='k'>Eksportert av:</div><div class='v'>" + esc(eksport) + "</div>"
        "<div class='k'>Dato:</div><div class='v'>" + esc(gen) + "</div>"
        "</div>"
        "<div class='cards'>"
        "<div class='card accent'><div class='lbl'>Omsetning inkl. mva</div><div class='val'>" + kr(brutto) + "</div></div>"
        "<div class='card'><div class='lbl'>Herav MVA " + esc(mva_pst) + " %</div><div class='val'>" + kr(mva) + "</div></div>"
        "<div class='card'><div class='lbl'>Omsetning eks. mva</div><div class='val'>" + kr(netto) + "</div></div>"
        "<div class='card'><div class='lbl'>Antall transaksjoner</div><div class='val'>" + str(ntx) + "</div></div>"
        "</div>"
        + excl_html +
        "<h3>Omsetning per kilde (med MVA-spesifikasjon)</h3>"
        "<table><thead><tr><th>Kilde</th><th class='num'>Antall</th>"
        "<th class='num'>Eks. mva (netto)</th><th class='num'>MVA " + esc(mva_pst) + " %</th>"
        "<th class='num'>Inkl. mva (brutto)</th></tr></thead><tbody>" + kilde_rows + "</tbody></table>"
        "<h3>Omsetning per system</h3>"
        "<table><thead><tr><th>System</th><th class='num'>Antall</th><th class='num'>Omsetning inkl. mva</th></tr></thead>"
        "<tbody>" + system_rows + "</tbody></table>"
        "<h3>Transaksjoner (" + str(ntx) + ")</h3>"
        "<table><thead><tr><th>Dato</th><th>System</th><th>Kilde</th><th>Referanse</th>"
        "<th>Kunde</th><th>Type</th><th class='num'>Beløp inkl. mva</th></tr></thead>"
        "<tbody>" + tx_rows + "</tbody></table>"
        "<div class='foot'>"
        "MVA-spesifikasjonen antar " + esc(mva_pst) + " % sats (sjømat/næringsmidler) på hele omsetningen. "
        "Har du varer med 25 % mva må splitten justeres manuelt.<br>"
        "Innkjøpskostnader er ikke vist her (estimerte tall finnes i CSV-versjonen). "
        "Omsetningen er deduplisert: Shopify-import-ordre telles ikke når salget allerede ligger i betalings-importene. "
        "Generert av Havøyet admin · " + esc(gen) +
        "</div></div></body></html>"
    )
    resp = Response(html, mimetype="text/html; charset=utf-8")
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/api/economy/report")
def api_economy_report():
    """Last ned en reell omsetningsrapport (CSV) for valgt periode.

    Bruker NØYAKTIG samme betalt-regel (_paid_ordrenrs), oppgjort-regel
    (_settled_ordrenrs) og periode-filter som /api/economy/stats, slik at
    summen i rapporten stemmer eksakt med tallene i Omsetning-fanen.
    Inkluderer per-kilde- og per-butikk-sammendrag + én rad per transaksjon
    (nettside/admin-ordre, Vipps-import og Shopify-kort) for full sporbarhet.

    Query: ?year=YYYY  |  ?from=YYYY-MM-DD&to=YYYY-MM-DD
    Format: norsk CSV (UTF-8 BOM, «;» som skille, komma som desimaltegn) —
    åpnes direkte i Excel/Numbers og kan gis til regnskapsfører.
    """
    try:
        return _api_economy_report_impl()
    except Exception as _er_e:
        import traceback as _tb
        print(f"[economy/report] ERROR: {_er_e}\n{_tb.format_exc()}")
        return jsonify({"error": str(_er_e)}), 500


def _api_economy_report_impl():
    today = datetime.now().date()
    EARLIEST = date(2025, 1, 1)
    q_year = request.args.get("year")
    q_from = request.args.get("from")
    q_to   = request.args.get("to")
    period_from, period_to = today.replace(month=1, day=1), today
    period_label = f"Hittil i {today.year}"
    try:
        if q_from:
            period_from = datetime.strptime(q_from, "%Y-%m-%d").date()
            period_to   = datetime.strptime(q_to, "%Y-%m-%d").date() if q_to else today
            period_label = _nice_period_label(period_from, period_to)
        elif q_year:
            y = max(2025, int(q_year))
            period_from = date(y, 1, 1)
            period_to   = date(y, 12, 31) if y != today.year else today
            period_label = (f"Hittil i {y}" if y == today.year else str(y))
    except (ValueError, TypeError):
        pass
    if period_from < EARLIEST:
        period_from = EARLIEST

    def _parse_date(s):
        if not s:
            return None
        s = str(s).strip()[:10]
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None

    def _in_period(s):
        d = _parse_date(s)
        return d is not None and period_from <= d <= period_to

    paid_set    = _paid_ordrenrs()
    settled_set = _settled_ordrenrs()
    cost_map    = _build_product_cost_map()

    def _ototal(o):
        try:
            return float(o.get("sum") or o.get("total") or 0)
        except (TypeError, ValueError):
            return 0.0

    def _odate(o):
        return o.get("dato") or o.get("created_at") or ""

    def _ostore(o):
        return (o.get("store") or "").strip() or "Havøyet"

    # --- Transaksjonsrader (samme rad-sett som Omsetning-fanen aggregerer) ---
    rows = []
    # 1) Nettside/manuelle ordre — oppgjorte (betalt + gratis) i perioden.
    #    Betalt => teller i omsetning; gratis => 0 kr men koster oss innkjøp.
    for o in (_manual_orders or []):
        if not isinstance(o, dict):
            continue
        ordrenr = str(o.get("ordrenr") or o.get("id") or "").strip()
        if not ordrenr or ordrenr not in settled_set:
            continue
        if not _in_period(_odate(o)):
            continue
        is_paid = ordrenr in paid_set
        is_shop = str(o.get("source") or "").lower() == "shopify"
        kunde = o.get("kunde") or {}
        rows.append({
            "date":  str(_odate(o))[:10],
            "time":  str(o.get("tid") or "")[:5],
            "system": "Shopify-nettside" if is_shop else "Nytt system",
            # Shopify-import-ordre er samme salg som Shopify-kort/Vipps-betalingene
            # under, så de telles IKKE i omsetning (counted=False) for å unngå
            # dobbelttelling. Innkjøpskost beholdes (reelt vareforbruk).
            "kilde": "Shopify-import (ordre)" if is_shop else "Nettside/admin",
            "store": _ostore(o),
            "ref":   ordrenr,
            "kunde": (kunde.get("navn") or ""),
            "detalj": (kunde.get("tlf") or kunde.get("epost") or ""),
            "type":  ("Kjøp" if is_paid else "Gratis"),
            "belop": (_ototal(o) if is_paid else 0.0),
            "kost":  _order_cost_kr(o, cost_map),
            "counted": (not is_shop),
        })
    # 2) Vipps-import (direkte i Vipps-app + ePayment via gammel nettside)
    for r in _vipps_imported_payments.values():
        if not _in_period(r.get("date")):
            continue
        direct = (r.get("payment_channel") == "direct")
        rows.append({
            "date":  str(r.get("date") or "")[:10],
            "time":  str(r.get("time") or "")[:5],
            "system": "Direkte salg" if direct else "Shopify-nettside",
            "kilde": ("Vipps direkte" if direct else "Vipps via nettside"),
            "store": "Havøyet",
            "ref":   r.get("transaction_id") or "",
            "kunde": r.get("name") or "",
            "detalj": r.get("phone") or r.get("description") or "",
            "type":  r.get("type") or "Kjøp",
            "belop": (r.get("amount_ore") or 0) / 100.0,
            "kost":  None,
            "counted": True,
        })
    # 3) Shopify-kort (CSV-import) — refusjon trekkes fra (negativt beløp)
    for r in _card_payments_imported.values():
        if not _in_period(r.get("date")):
            continue
        signed = (r.get("amount_ore") or 0) / 100.0
        if r.get("type") == "Refusjon":
            signed = -signed
        rows.append({
            "date":  str(r.get("date") or "")[:10],
            "time":  str(r.get("time") or "")[:5],
            "system": "Shopify-nettside",
            "kilde": "Shopify kort",
            "store": "Havøyet",
            "ref":   r.get("order") or r.get("transaction_id") or "",
            "kunde": "",
            "detalj": r.get("brand") or "",
            "type":  r.get("type") or "Kjøp",
            "belop": signed,
            "kost":  None,
            "counted": True,
        })

    rows.sort(key=lambda x: (x["date"], x["time"]))

    # --- Sammendrag (kun counted=True teller i omsetning; kost teller alltid) ---
    counted_rows  = [r for r in rows if r["counted"]]
    excluded_rows = [r for r in rows if not r["counted"]]
    sum_belop = sum(r["belop"] for r in counted_rows)
    sum_kost  = sum((r["kost"] or 0.0) for r in rows)
    sum_excl  = sum(r["belop"] for r in excluded_rows)
    by_system, by_kilde, by_store = {}, {}, {}
    for r in counted_rows:
        for agg, key in ((by_system, r["system"]), (by_kilde, r["kilde"]), (by_store, r["store"])):
            g = agg.setdefault(key, {"n": 0, "kr": 0.0})
            g["n"] += 1
            g["kr"] += r["belop"]

    # --- Bygg norsk CSV (semikolon-skille, komma-desimal, UTF-8 BOM) ---
    def kr(v):
        return ("%.2f" % float(v or 0)).replace(".", ",")

    def esc(v):
        s = "" if v is None else str(v)
        if any(c in s for c in (";", '"', "\n", "\r")):
            s = '"' + s.replace('"', '""') + '"'
        return s

    # MVA-splitt (Havøyet = 15 % sjømat/næringsmidler). Brutto = mottatt beløp.
    netto = sum_belop / (1 + _MVA_RATE)
    mva   = sum_belop - netto
    org_nr = (_invoice_config or {}).get("orgNr") or _INVOICE_CONFIG_DEFAULTS["orgNr"]
    mva_pst = ("%g" % (_MVA_RATE * 100))

    # === Standard: pen HTML-rapport (skriv ut / lagre som PDF). CSV ved ?format=csv ===
    if (request.args.get("format") or "html").lower() != "csv":
        return _economy_report_html(
            period_label, period_from, period_to, org_nr, mva_pst,
            sum_belop, netto, mva, sum_kost, len(counted_rows),
            by_system, by_kilde, excluded_rows, sum_excl, rows,
        )

    L = []
    L.append("Omsetningsrapport;Havøyet AS")
    L.append("Org.nr;" + esc(org_nr))
    L.append("Generert;" + datetime.now().strftime("%d.%m.%Y %H:%M"))
    L.append("Periode;" + esc(period_label))
    L.append("Fra;" + period_from.strftime("%Y-%m-%d") + ";Til;" + period_to.strftime("%Y-%m-%d"))
    L.append("")
    L.append("SAMMENDRAG")
    L.append(f"Omsetning inkl. mva (brutto);{len(counted_rows)};{kr(sum_belop)}")
    L.append(f"Innkjøpskostnader (estimert, inkl. gratis-ordre);;{kr(sum_kost)}")
    L.append(f"Bruttofortjeneste (estimert);;{kr(sum_belop - sum_kost)}")
    L.append("")
    L.append(f"MVA-SPESIFIKASJON (sats {mva_pst} % - sjømat/næringsmidler)")
    L.append("Post;Beløp (kr)")
    L.append(f"Omsetning eks. mva (netto);{kr(netto)}")
    L.append(f"Herav MVA {mva_pst} %;{kr(mva)}")
    L.append(f"Omsetning inkl. mva (brutto);{kr(sum_belop)}")
    L.append("Merk: antatt 15 % sats for hele omsetningen. Har du varer med 25 % mva må splitten justeres manuelt.")
    L.append("")
    L.append("OMSETNING PER SYSTEM")
    L.append("System;Antall;Beløp (kr)")
    for k in sorted(by_system, key=lambda x: -by_system[x]["kr"]):
        L.append(f"{esc(k)};{by_system[k]['n']};{kr(by_system[k]['kr'])}")
    L.append(f"Sum;{len(counted_rows)};{kr(sum_belop)}")
    L.append("")
    L.append("OMSETNING PER KILDE")
    L.append("Kilde;Antall;Beløp (kr)")
    for k in sorted(by_kilde, key=lambda x: -by_kilde[x]["kr"]):
        L.append(f"{esc(k)};{by_kilde[k]['n']};{kr(by_kilde[k]['kr'])}")
    L.append("")
    L.append("OMSETNING PER BUTIKK")
    L.append("Butikk;Antall;Beløp (kr)")
    for s in sorted(by_store, key=lambda x: -by_store[x]["kr"]):
        L.append(f"{esc(s)};{by_store[s]['n']};{kr(by_store[s]['kr'])}")
    if excluded_rows:
        L.append("")
        L.append("IKKE TALT MED - unngår dobbelttelling")
        L.append("Disse Shopify-ordrene er samme salg som allerede ligger i Shopify kort / Vipps via nettside.")
        L.append("Kilde;Antall;Beløp (kr)")
        L.append(f"Shopify-import (ordre);{len(excluded_rows)};{kr(sum_excl)}")
    L.append("")
    L.append("TRANSAKSJONER")
    L.append("Dato;Tid;System;Kilde;Butikk;Referanse;Kunde;Detalj;Type;Beløp (kr);Innkjøpskost (kr);Talt med i omsetning")
    for r in rows:
        L.append(";".join([
            esc(r["date"]), esc(r["time"]), esc(r["system"]), esc(r["kilde"]), esc(r["store"]),
            esc(r["ref"]), esc(r["kunde"]), esc(r["detalj"]), esc(r["type"]),
            kr(r["belop"]), ("" if r["kost"] is None else kr(r["kost"])),
            ("Ja" if r["counted"] else "Nei - dobbelttelling"),
        ]))

    csv_text = "﻿" + "\r\n".join(L) + "\r\n"
    fname = f"havoyet-omsetning-{period_from.strftime('%Y%m%d')}_{period_to.strftime('%Y%m%d')}.csv"
    resp = Response(csv_text, mimetype="text/csv; charset=utf-8")
    resp.headers["Content-Disposition"] = f'attachment; filename="{fname}"'
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/api/vipps/callback", methods=["POST"])
def api_vipps_callback():
    """Webhook fra Vipps når betalingsstatus endrer seg.
    Oppdaterer betalingslogg OG ordren i _manual_orders (same mønster som
    Stripe payment_intent.succeeded-webhooken)."""
    global _manual_orders
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

        # Auto-capture ved AUTHORIZED
        if state == "AUTHORIZED":
            amount = payments[reference].get("amount", 0)
            if amount > 0:
                captured = _vipps_capture(reference, amount)
                if captured:
                    state = "CAPTURED"
                    payments[reference]["state"] = "CAPTURED"
                    payments[reference]["captured_at"] = time.time()
                    _vipps_save_payments(payments)

        # Oppdater ordren til PAID viss Vipps bekreftar betaling
        if state in _VIPPS_PAID_STATES:
            ordrenr = payments[reference].get("ordrenr")
            if ordrenr:
                for o in _manual_orders:
                    if str(o.get("ordrenr") or o.get("id") or "").strip() == str(ordrenr):
                        was_pending = _is_pending_order_status(o.get("status"))
                        o["status"] = "PAID"
                        o["paymentStatus"] = "paid"
                        o["paid_at"] = datetime.now().isoformat()
                        o["vipps_reference"] = reference
                        _save_sync_state()
                        _notify_admins(
                            "payment_received",
                            f"[Havøyet] Vipps-betaling mottatt #{ordrenr}",
                            f"Beløp: {payments[reference].get('amount', 0)/100:.2f} kr (Vipps)\n"
                            f"Ordre: {ordrenr}\nReference: {reference}",
                        )
                        if was_pending:
                            try:
                                navn_n = ((o.get("kunde") or {}).get("navn") or "?").strip()
                                _notify_admins(
                                    "new_order",
                                    f"[Havøyet] Ny bestilling #{ordrenr} — {navn_n} ({o.get('sum', 0)} kr)",
                                    _format_order_lines(o),
                                    html_body=_format_order_email_html(o, "Bestillingen er fullført og betalt med Vipps.", "new_order"),
                                )
                            except Exception as e:
                                print(f"[ADMIN-NOTIFY] new_order varsel (Vipps→PAID) feilet: {e}")
                            try:
                                _send_customer_order_confirmation(o)
                            except Exception as e:
                                print(f"[CUSTOMER-CONFIRM] feilet (Vipps callback): {e}")
                        break
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
    deny = _check_payment_amount(ordrenr, amount)
    if deny: return deny
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
    Subscription-id får prefiks 'test_' så det er enkelt å rydde opp.

    Når `send_mail: true` sendes en kvitterings-mail til kunden via Resend slik
    at admin kan inspisere hvordan en faktisk subscription-bekreftelse ser ut.
    Mailen får tydelig TEST-merking i subject så ingen forveksler den med
    en ekte transaksjon."""
    user, err = _subscription_admin_required()
    if err: return err
    data  = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    if not email:
        return jsonify({"ok": False, "error": "E-post kreves"}), 400
    amount   = int(data.get("amount", 1490_00))   # default: 1490 kr/mnd (i øre)
    kasse    = data.get("kasse") or {"name": "Sjømatkasse — 2 personer", "size": "2pers"}
    desc     = data.get("description") or "Sjømatkasse — månedlig abonnement (TEST)"
    navn     = data.get("navn") or "Test Testesen"
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
        "kunde":              {"epost": email, "navn": navn},
        "kasse":              kasse,
        "description":        desc,
        "created_at":         now,
        "last_charged_at":    now,
        "charges_count":      1,
        "is_test":            True,
    }
    _save_subscriptions()

    mail_status = None
    if data.get("send_mail"):
        # `hide_test_markers=True` rendrer mailen som om det var en ekte
        # bestilling — ingen [TEST]-prefix, ingen gult banner. Sub-en er
        # fortsatt synthetisk internt (id med prefiks `test_`), men
        # mailen ser identisk ut med en ekte Stripe-bekreftelse.
        show_test = not bool(data.get("hide_test_markers"))
        try:
            ok, info = _send_subscription_receipt_mail(
                to_email=email,
                navn=navn,
                amount_ore=amount,
                kasse=kasse,
                description=desc,
                next_period_ts=next_period,
                sub_id=sub_id,
                is_test=show_test,
            )
            mail_status = {"ok": ok, "info": info}
        except Exception as e:
            mail_status = {"ok": False, "info": f"exception: {e}"}

    return jsonify({
        "ok": True,
        "subscription_id": sub_id,
        "row": _subscriptions[sub_id],
        "mail": mail_status,
    })


def _send_subscription_receipt_mail(to_email, navn, amount_ore, kasse, description,
                                     next_period_ts, sub_id, is_test=False):
    """Sender en realistisk kvitterings-mail for et nyopprettet abonnement.
    Brukes både fra admin-test-create (synthetisk) og kan kobles på den ekte
    Stripe-flyten senere. Returnerer (ok, info)."""
    if not RESEND_API_KEY:
        return False, "RESEND_API_KEY ikke konfigurert"

    amount_kr = (amount_ore or 0) / 100.0
    amount_str = f"{amount_kr:,.2f}".replace(",", " ").replace(".", ",").rstrip("0").rstrip(",")
    if "," not in amount_str:
        amount_str = amount_str + ",00"

    next_dt = datetime.fromtimestamp(next_period_ts).strftime("%d.%m.%Y")
    kasse_name = (kasse or {}).get("name") or "Sjømatkasse"
    kasse_size = (kasse or {}).get("size") or ""
    meta = (kasse or {}).get("meta") or {}
    voksne = meta.get("voksne")
    barn = meta.get("barn")
    leverdag = meta.get("leverdag")
    portion_line = ""
    if voksne or barn:
        bits = []
        if voksne: bits.append(f"{voksne} voksne")
        if barn:   bits.append(f"{barn} barn")
        portion_line = " · ".join(bits)

    test_banner_html = ""
    test_banner_text = ""
    subject_prefix = ""
    if is_test:
        subject_prefix = "[TEST] "
        test_banner_html = (
            '<div style="background:#fff3cd;border:1px solid #ffe69c;color:#664d03;'
            'padding:12px 16px;border-radius:8px;margin:0 0 24px;font-size:13px;'
            'font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Helvetica,Arial,sans-serif;">'
            '<strong>Dette er en test-bekreftelse</strong> — ingen kort er trukket, '
            'ingen leveranse vil bli sendt. Generert fra admin for å vise hvordan '
            'en ekte abonnement-bekreftelse ser ut for kunden.'
            '</div>'
        )
        test_banner_text = (
            "[TEST] Dette er en test-bekreftelse. Ingen kort er trukket, "
            "ingen leveranse blir sendt.\n\n"
        )

    html_body = f"""
<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;color:#1a1a1a;max-width:560px;margin:0 auto;">
  {test_banner_html}
  <div style="text-align:center;padding:32px 0 24px;">
    <div style="width:64px;height:64px;border-radius:50%;background:#d6f0eb;color:#0a8674;display:inline-flex;align-items:center;justify-content:center;font-size:30px;border:2px solid #0a8674;line-height:1;">&#10003;</div>
    <h1 style="font-size:28px;font-weight:600;letter-spacing:-0.01em;margin:18px 0 6px;">Velkommen som abonnent!</h1>
    <p style="color:#5b6470;font-size:15px;margin:0;">Vi gleder oss til å sende fersk fisk hjem til deg.</p>
  </div>

  <div style="background:#fafafa;border:1px solid #e8e8e8;border-radius:12px;padding:20px 22px;margin:0 0 20px;">
    <div style="font-size:12px;text-transform:uppercase;letter-spacing:0.06em;color:#7a8290;margin-bottom:10px;">Ditt abonnement</div>
    <div style="font-size:18px;font-weight:600;margin-bottom:6px;">{kasse_name}</div>
    {f'<div style="font-size:13px;color:#5b6470;margin-bottom:14px;">{portion_line}</div>' if portion_line else ''}
    <table style="width:100%;font-size:14px;border-collapse:collapse;">
      <tr><td style="padding:6px 0;color:#5b6470;">Beløp per måned</td><td style="padding:6px 0;text-align:right;font-weight:600;">{amount_str} kr</td></tr>
      <tr><td style="padding:6px 0;color:#5b6470;">Neste trekk</td><td style="padding:6px 0;text-align:right;">{next_dt}</td></tr>
      {f'<tr><td style="padding:6px 0;color:#5b6470;">Leveringsdag</td><td style="padding:6px 0;text-align:right;">{leverdag}</td></tr>' if leverdag else ''}
      <tr><td style="padding:6px 0;color:#5b6470;">Frakt</td><td style="padding:6px 0;text-align:right;color:#0a8674;">Inkludert</td></tr>
    </table>
  </div>

  <div style="font-size:14px;line-height:1.6;color:#2a2f38;margin:0 0 22px;">
    <p style="margin:0 0 12px;">Hei {navn},</p>
    <p style="margin:0 0 12px;">Takk for at du startet et abonnement på {kasse_name.lower()}. Vi pakker råvarene på is og kjører dem hjem til deg på leveringsdagen.</p>
    <p style="margin:0 0 12px;">Du kan hoppe over en levering opptil <strong>2 uker</strong> før, eller si opp senest <strong>1 uke</strong> før neste trekk for å få full refusjon. Endre alt på <a href="https://havoyet.no/konto-dashbord" style="color:#0a8674;">Min side</a>.</p>
  </div>

  <div style="text-align:center;margin:0 0 28px;">
    <a href="https://havoyet.no/konto-dashbord" style="display:inline-block;background:#0a8674;color:#ffffff;text-decoration:none;font-weight:600;font-size:14px;padding:12px 22px;border-radius:999px;">Til Min side</a>
  </div>

  <div style="font-size:11px;color:#9aa1ac;text-align:center;border-top:1px solid #ececec;padding:14px 0 0;">
    Abonnement-ID: {sub_id}<br/>
    Spørsmål? Svar på denne e-posten eller skriv til <a href="mailto:erik@havoyet.no" style="color:#0a8674;">erik@havoyet.no</a>.
  </div>
</div>
""".strip()

    text_body = (
        f"{test_banner_text}"
        f"Hei {navn},\n\n"
        f"Takk for at du startet et abonnement på {kasse_name}.\n\n"
        f"Beløp per måned: {amount_str} kr\n"
        f"Neste trekk: {next_dt}\n"
        f"{('Leveringsdag: ' + leverdag + chr(10)) if leverdag else ''}"
        f"Frakt: Inkludert\n\n"
        f"Du kan hoppe over en levering opptil 2 uker før, eller si opp senest "
        f"1 uke før neste trekk for å få full refusjon. Endre alt på "
        f"https://havoyet.no/konto-dashbord.\n\n"
        f"Abonnement-ID: {sub_id}\n"
        f"Spørsmål? Svar på denne e-posten eller skriv til erik@havoyet.no."
    )

    subject = f"{subject_prefix}Velkommen som abonnent — {kasse_name}"
    return _send_via_resend(
        from_email=None,
        from_name="Havøyet",
        subject=subject,
        body=text_body,
        to_email=to_email,
        html_body=html_body,
    )

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
    så frontenden kan bekrefte betaling med kortdata in-line uten redirect.

    Når `requires_capture` er True (ordren har hel-fisk med vekt-reservasjon),
    settes capture_method='manual' så kortet blir HOLDT på maks-beløp uten
    trekk. Endelig sum belastes via /api/admin/orders/<nr>/capture når
    butikken har veid fisken. Stripe-auth varer ~7 dager."""
    if not _stripe_configured():
        return jsonify({"error": "Kortbetaling er ikke konfigurert"}), 503
    data = request.get_json(silent=True) or {}
    ordrenr  = data.get("ordrenr") or ("H" + str(int(time.time() * 1000))[-8:])
    amount   = int(data.get("amount", 0))   # i øre
    if amount <= 0:
        return jsonify({"error": "Ugyldig beløp"}), 400
    customer = data.get("customer") or {}
    requires_capture  = bool(data.get("requires_capture"))
    reservation_items = data.get("reservation_items") or []
    # Hopp over beløpsvalidering for reservasjon (hel-fisk): da HOLDES kortet på
    # estimert MAKS-beløp (> ordrens sum), så amount != sum er forventet og riktig.
    if not requires_capture:
        deny = _check_payment_amount(ordrenr, amount)
        if deny: return deny
    try:
        intent_kwargs = dict(
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
        if requires_capture:
            intent_kwargs["capture_method"] = "manual"
            intent_kwargs["metadata"]["reservation"] = "1"
        intent = _stripe.PaymentIntent.create(**intent_kwargs)
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
        # Lagre reservasjons-info så admin senere kan capture med faktisk vekt
        "requires_capture":  requires_capture,
        "reservation_items": reservation_items if requires_capture else None,
    }
    _stripe_save_payments(payments)

    return jsonify({
        "ok":           True,
        "clientSecret": intent.client_secret,
        "paymentIntent": intent.id,
        "ordrenr":      ordrenr,
        "requires_capture": requires_capture,
    })


@app.route("/api/admin/orders/<ordrenr>/capture", methods=["POST"])
def api_admin_order_capture(ordrenr):
    """Capture en hel-fisk-reservasjon med faktisk vekt.

    Body: { items: [{ slug, actualWeightKg }] }
    Beregner totalt beløp = sum(item.pricePerKg × actualWeightKg × qty) for
    reservasjons-linjer, og kaller stripe.PaymentIntent.capture(amount_to_capture=...).
    Stripe frigjør differansen mellom autorisert og captured beløp automatisk.
    """
    if not _stripe_configured():
        return jsonify({"error": "Stripe er ikke konfigurert"}), 503
    # Beskytt mot uvedkommende — krever admin-cookie/token. Reuse eksisterende
    # admin-auth hvis tilgjengelig; ellers minst basic-token-sjekk.
    if not _require_admin(request):
        return jsonify({"error": "Kun admin kan capture"}), 403
    data = request.get_json(silent=True) or {}
    items_in = data.get("items") or []
    if not items_in:
        return jsonify({"error": "Mangler items med faktisk vekt"}), 400

    # Finn PaymentIntent for ordren
    payments = _stripe_load_payments()
    pi_record = None
    pi_id = None
    for k, v in payments.items():
        if str(v.get("ordrenr")) == str(ordrenr) and v.get("requires_capture"):
            pi_record = v
            pi_id = k
            break
    if not pi_record:
        return jsonify({"error": f"Fant ingen reservasjon for ordre {ordrenr}"}), 404
    if pi_record.get("state") in ("CAPTURED", "REFUNDED", "CANCELED"):
        return jsonify({"error": f"Reservasjonen er allerede {pi_record.get('state')}"}), 409

    # Map slug → reservation item (pricePerKg, helVekt, qty)
    res_items = pi_record.get("reservation_items") or []
    by_slug = { (r.get("slug") or ""): r for r in res_items }

    # Regn ut faktisk capture-beløp i øre (Stripe bruker minste valutaenhet)
    total_ore = 0
    breakdown = []
    for it in items_in:
        slug = it.get("slug")
        kg   = float(it.get("actualWeightKg") or 0)
        if kg <= 0:
            return jsonify({"error": f"Ugyldig vekt for {slug}"}), 400
        ref = by_slug.get(slug)
        if not ref:
            return jsonify({"error": f"Slug {slug} er ikke en reservasjons-linje"}), 400
        price_per_kg = float(ref.get("pricePerKg") or 0)
        qty = int(ref.get("qty") or 1)
        line_total_kr = price_per_kg * kg * qty
        line_total_ore = int(round(line_total_kr * 100))
        total_ore += line_total_ore
        breakdown.append({
            "slug": slug,
            "actualWeightKg": kg,
            "qty": qty,
            "pricePerKg": price_per_kg,
            "lineTotalKr": round(line_total_kr, 2),
        })

    if total_ore <= 0:
        return jsonify({"error": "Beregnet capture-beløp er 0"}), 400

    # Stripe tillater ikke capture > autorisert beløp. Hvis fisken veier mer
    # enn maks → cappes til autorisert beløp og restdifferansen må kreves separat.
    authorized_ore = int(pi_record.get("amount") or 0)
    if total_ore > authorized_ore:
        capped_ore = authorized_ore
        owed_extra_kr = round((total_ore - authorized_ore) / 100.0, 2)
    else:
        capped_ore = total_ore
        owed_extra_kr = 0.0

    try:
        captured = _stripe.PaymentIntent.capture(pi_id, amount_to_capture=capped_ore)
    except Exception as e:
        return jsonify({"error": f"Stripe capture feilet: {e}"}), 502

    # Persistér
    pi_record["state"]            = "CAPTURED"
    pi_record["captured_amount"]  = capped_ore
    pi_record["captured_at"]      = time.time()
    pi_record["actual_breakdown"] = breakdown
    if owed_extra_kr > 0:
        pi_record["owed_extra_kr"] = owed_extra_kr
    payments[pi_id] = pi_record
    _stripe_save_payments(payments)

    return jsonify({
        "ok": True,
        "ordrenr": ordrenr,
        "paymentIntent": pi_id,
        "capturedAmountKr": round(capped_ore / 100.0, 2),
        "reservedAmountKr": round(authorized_ore / 100.0, 2),
        "owedExtraKr": owed_extra_kr,
        "breakdown": breakdown,
        "stripeStatus": getattr(captured, "status", None),
    })


def _require_admin(req):
    """Admin-auth-sjekk. Bruker eksisterende _user_from_request()-mekanisme
    (samme som /api/subscription/*-endepunktene). Returnerer True hvis OK."""
    try:
        user, _tok = _user_from_request()
    except Exception:
        user = None
    return bool(user and user.get("role") == "admin")

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
        # Fail-closed: uten webhook-secret kan vi ikke stole på payloaden — hvem
        # som helst kunne ellers POSTe en falsk «checkout.session.completed» og
        # markere en ordre betalt. Krev at STRIPE_WEBHOOK_SECRET er satt.
        return jsonify({"error": "Webhook ikke konfigurert (mangler STRIPE_WEBHOOK_SECRET)"}), 503

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
            was_pending = False
            target_order = None
            for o in _manual_orders:
                if str(o.get("ordrenr") or o.get("id") or "").strip() == str(ordrenr):
                    was_pending = _is_pending_order_status(o.get("status"))
                    o["status"] = "PAID"
                    o["paymentStatus"] = "paid"
                    o["paid_at"] = datetime.now().isoformat()
                    order_found = True
                    target_order = o
                    break
            if order_found:
                _save_sync_state()
                _notify_admins(
                    "payment_received",
                    f"[Havøyet] Kortbetaling mottatt #{ordrenr}",
                    f"Beløp: {amount/100:.2f} kr (kort via Stripe Elements)\n"
                    f"Ordre: {ordrenr}\nPaymentIntent: {pi_id}",
                )
                # Fyr av "new_order" hvis ordren nettopp flippet fra pending → PAID.
                # Dette er sikkerhetsnett for tilfellene hvor frontend ikke rakk å
                # re-POSTe ordren etter Stripe-bekreftelsen — admin skal fortsatt
                # få sitt vanlige bestillings-varsel.
                if was_pending and target_order:
                    try:
                        navn_n = ((target_order.get("kunde") or {}).get("navn") or "?").strip()
                        _notify_admins(
                            "new_order",
                            f"[Havøyet] Ny bestilling #{ordrenr} — {navn_n} ({target_order.get('sum', 0)} kr)",
                            _format_order_lines(target_order),
                            html_body=_format_order_email_html(
                                target_order,
                                "Bestillingen er fullført og betalt med kort.",
                                "new_order",
                            ),
                        )
                    except Exception as e:
                        print(f"[ADMIN-NOTIFY] new_order varsel (Stripe→PAID) feilet: {e}")
                    try:
                        _send_customer_order_confirmation(target_order)
                    except Exception as e:
                        print(f"[CUSTOMER-CONFIRM] feilet (Stripe webhook): {e}")
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
    """Refunder en kundeordre — automatisk via Stripe ELLER Vipps, basert på
    hva ordren ble betalt med. Støtter delvis refusjon (amount_ore < total).

    Body: { amount_ore: int, reason?: str, note?: str, lines?: list }
    """
    user, err = _admin_required_stripe()
    if err: return err
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

    # 1) Prøv Stripe først — disse har en payment_intent lagret
    pi_id, payment_rec = _find_payment_intent_for_order(ordrenr)
    vipps_ref, vipps_rec = (None, None)
    if not pi_id:
        # 2) Ikke funnet i Stripe — prøv Vipps
        vipps_ref, vipps_rec = _find_vipps_reference_for_order(ordrenr)
    if not pi_id and not vipps_ref:
        # Ingen API-referanse — la admin velge "manuell refusjon" eksplisitt
        # via ?manual=1. Da logges refusjonen kun lokalt på ordren slik at
        # regnskap og status stemmer; admin må selv refundere i Vipps-appen
        # eller via kortets bakkanal.
        if not body.get("manual"):
            return jsonify({
                "ok": False,
                "error": f"Fant ingen Stripe- eller Vipps-betaling for ordre {ordrenr}. "
                         f"Vipps-betalingen ble importert manuelt (CSV/PDF) og kan ikke refunderes via Vipps API. "
                         f"Refunder via Vipps-appen og bruk 'Logg manuell refusjon'.",
                "needs_manual": True,
                "betaling": (next((str((o.get('kunde') or {}).get('betaling') or '') for o in _manual_orders if str(o.get('ordrenr') or '').strip() == str(ordrenr).strip()), '')),
            }), 404

    refund_record = None
    paid_amount = 0
    if not pi_id and not vipps_ref and body.get("manual"):
        # Manuell refusjon — ingen ekstern API-kall. Admin har gjort refusjonen
        # selv (f.eks. via Vipps-appen) og logger her for at admin-historikken
        # og REFUNDED-status skal bli riktig.
        refund_record = {
            "id":             f"manual-{ordrenr}-{int(time.time())}",
            "amount_ore":     amount_ore,
            "amount_kr":      round(amount_ore / 100, 2),
            "reason":         reason,
            "note":           note,
            "lines":          lines,
            "status":         "manual_logged",
            "provider":       "manual",
            "created_at":     time.time(),
            "by":             (user or {}).get("email"),
        }
        # Hent ordrens totalsum for å avgjøre om dette er full refusjon
        for o in _manual_orders:
            if str(o.get("ordrenr") or "").strip() == str(ordrenr).strip():
                paid_amount = int(round(float(o.get("sum") or o.get("total") or 0) * 100))
                break
    elif pi_id:
        if not _stripe_configured():
            return jsonify({"ok": False, "error": "Stripe ikke konfigurert"}), 503
        try:
            kwargs = {"payment_intent": pi_id, "amount": amount_ore}
            if reason: kwargs["reason"] = reason
            refund = _stripe.Refund.create(**kwargs)
        except Exception as e:
            return jsonify({"ok": False, "error": f"Stripe-refusjon feilet: {e}"}), 502
        refund_record = {
            "id":             refund.id,
            "amount_ore":     refund.amount,
            "amount_kr":      round(refund.amount / 100, 2),
            "reason":         reason,
            "note":           note,
            "lines":          lines,
            "status":         refund.status,
            "payment_intent": pi_id,
            "provider":       "stripe",
            "created_at":     time.time(),
            "by":             (user or {}).get("email"),
        }
        paid_amount = int((payment_rec or {}).get("amount") or 0)
    elif vipps_ref:
        # Vipps refusjon via ePayment API
        if not _vipps_configured():
            return jsonify({"ok": False, "error": "Vipps ikke konfigurert"}), 503
        ok, vbody, status = _vipps_refund(vipps_ref, amount_ore, idem_suffix=str(int(time.time()*1000)))
        if not ok:
            err_msg = (vbody.get("detail") or vbody.get("title") or vbody.get("error")
                       or vbody.get("raw") or f"HTTP {status}")
            return jsonify({"ok": False, "error": f"Vipps-refusjon feilet: {err_msg}"}), 502
        # Vipps returnerer bl.a. pspReference, state etter refund
        refund_id = (vbody.get("pspReference") or vbody.get("reference")
                     or f"vipps-{vipps_ref}-{int(time.time())}")
        refund_record = {
            "id":             refund_id,
            "amount_ore":     amount_ore,
            "amount_kr":      round(amount_ore / 100, 2),
            "reason":         reason,
            "note":           note,
            "lines":          lines,
            "status":         vbody.get("state") or "refunded",
            "vipps_reference": vipps_ref,
            "provider":       "vipps",
            "created_at":     time.time(),
            "by":             (user or {}).get("email"),
        }
        paid_amount = int((vipps_rec or {}).get("amount") or 0)

    # Logg refusjonen mot ordren i _manual_orders så den vises i admin
    # (kun lesing + mutasjon av elementer — ingen rebinding, så ingen `global`)
    try:
        for o in _manual_orders:
            if str(o.get("ordrenr") or "").strip() == str(ordrenr).strip():
                refunds = o.setdefault("refunds", [])
                refunds.append(refund_record)
                refunded_total = sum(int(r.get("amount_ore") or 0) for r in refunds)
                if paid_amount and refunded_total >= paid_amount:
                    o["status"] = "REFUNDED"
                break
        _save_sync_state()
    except Exception as e:
        print(f"[refund] Klarte ikke oppdatere _manual_orders for {ordrenr}: {e}")

    provider = refund_record.get("provider", "stripe")
    _notify_admins(
        "refund_issued",
        f"[Havøyet] Refusjon utstedt ({refund_record['amount_kr']:.2f} kr) — ordre {ordrenr} via {provider.title()}",
        f"Ordrenr: {ordrenr}\nKanal: {provider.title()}\nRefund-ID: {refund_record['id']}\n"
        f"Beløp: {refund_record['amount_kr']:.2f} kr\nÅrsak: {reason or '—'}\n"
        f"Notat: {note or '—'}\nUtført av: {(user or {}).get('email')}",
    )
    return jsonify({"ok": True, "refund": refund_record, "ordrenr": ordrenr, "provider": provider})


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

def _require_admin_user():
    """Fail-closed admin-gate: krever innlogget bruker med rolle 'admin'
    (Bearer-token) ELLER gyldig X-Admin-Token. I motsetning til
    _is_admin_request() faller denne ALDRI åpen når ADMIN_API_TOKEN er uset.
    Returnerer None hvis tilgang OK, ellers (json_response, status) for 403.
    Brukes på endepunkt som lekker kunde-PII / e-postliste / admin-data."""
    try:
        user, _ = _user_from_request()
        if user and (user.get("role") or "").lower() == "admin":
            return None
    except Exception:
        pass
    if ADMIN_API_TOKEN:
        tok = (request.headers.get("X-Admin-Token") or "").strip()
        if tok and _hmac_mod.compare_digest(tok, ADMIN_API_TOKEN):
            return None
    return (jsonify({"error": "Admin-tilgang kreves"}), 403)


def _check_payment_amount(ordrenr, amount_ore):
    """Validér betalingsbeløp mot den PRE-LAGREDE ordren (checkout lagrer ordren
    med status AWAITING_PAYMENT via /api/orders/new FØR betaling opprettes).
    Returnerer (json, 400) ved avvik, ellers None.

    Best-effort: hvis modulen mangler, ordren ikke er pre-lagret, eller noe
    kaster, logges det og betalingen slippes gjennom — vi vil ALDRI blokkere en
    legitim betaling pga. en valideringsfeil. Lukker det trivielle «betal 1 kr
    for en ekte ordre»-angrepet; en angriper som hopper over /api/orders/new får
    en betaling uten matchende ordre (synlig avvik for admin)."""
    if not _validate_order_payment or not ordrenr:
        return None
    target = str(ordrenr).strip()
    order = next((o for o in (_manual_orders or [])
                  if str(o.get("ordrenr") or o.get("id") or "").strip() == target), None)
    if order is None:
        print(f"[PAY] kan ikke validere beløp — ordre {target} ikke pre-lagret (slipper gjennom)")
        return None
    try:
        ok, why = _validate_order_payment(order, amount_ore)
    except Exception as e:
        print(f"[PAY] valideringsfeil for {target}: {e} (slipper gjennom)")
        return None
    if not ok:
        print(f"[PAY] AVVIST beløp for ordre {target}: {why} (amount={amount_ore} øre)")
        return jsonify({"error": "Betalingsbeløpet stemmer ikke med ordren. Last siden på nytt og prøv igjen."}), 400
    return None


def _client_ip():
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.headers.get("X-Real-IP") or request.remote_addr or "?"

# ── Enkel in-memory rate-limiting (Render kjører 1 gunicorn-worker → eksakt) ──
_rate_buckets = {}  # key → [timestamps]
def _rate_limited(key, max_n, window_s):
    """True hvis `key` har gjort > max_n forsøk innen window_s sekunder.
    Teller samtidig opp dette forsøket. In-memory (nullstilles ved restart)."""
    now = time.time()
    bucket = [t for t in _rate_buckets.get(key, []) if now - t < window_s]
    bucket.append(now)
    _rate_buckets[key] = bucket
    if len(_rate_buckets) > 5000:  # enkel opprydding så dict-en ikke vokser uten grense
        for k in list(_rate_buckets.keys())[:1000]:
            _rate_buckets.pop(k, None)
    return len(bucket) > max_n


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
        # BSF = Bergen Seilforening. 'none' | 'pending' | 'approved' | 'rejected'.
        # Settes ved registrering (selvrapportert → pending). Admin godkjenner.
        "bsfMemberStatus": u.get("bsf_member_status") or "none",
    }

@app.route("/api/auth/login", methods=["POST"])
def api_auth_login():
    data = request.get_json(force=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    if _rate_limited(f"login:{_client_ip()}:{email}", 10, 300):
        return jsonify({"ok": False, "error": "For mange innloggingsforsøk. Vent noen minutter og prøv igjen."}), 429
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
    if _rate_limited(f"forgot:{_client_ip()}", 5, 600):
        return jsonify({"ok": True, "message": "Hvis e-posten er registrert, har vi sendt deg en lenke."}), 429
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
    if _rate_limited(f"reset:{_client_ip()}", 10, 600):
        return jsonify({"ok": False, "error": "For mange forsøk. Vent litt og prøv igjen."}), 429
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
    if _rate_limited(f"register:{_client_ip()}", 8, 3600):
        return jsonify({"ok": False, "error": "For mange registreringer fra denne tilkoblingen. Prøv igjen senere."}), 429
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "Ugyldig e-post"}), 400
    if len(password) < 8:
        return jsonify({"ok": False, "error": "Passordet må være minst 8 tegn"}), 400
    if _find_user(email):
        return jsonify({"ok": False, "error": "E-posten er allerede registrert. Logg inn i stedet."}), 409
    # BSF-medlemskap: selvrapportert ved registrering → status 'pending'
    # til admin godkjenner. Standardverdi 'none' hvis ikke huket av.
    bsf_member = bool(data.get("bsf_member"))
    new_user = {
        "email": email,
        "role": "customer",
        "password_hash": generate_password_hash(password),
        "must_set_password": False,
        "created_at": int(time.time()),
        "bsf_member_status": "pending" if bsf_member else "none",
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


@app.route("/api/auth/users/<path:user_email>/bsf-status", methods=["PATCH"])
def api_set_bsf_status(user_email):
    """Admin endrer BSF-medlemskap-status. Body: {status: 'approved'|'rejected'|'pending'|'none'}."""
    if not _is_admin_request():
        return jsonify({"error": "Mangler admin-token"}), 401
    target = (user_email or "").strip().lower()
    u = _find_user(target)
    if not u:
        return jsonify({"error": "Ukjent bruker"}), 404
    data = request.get_json(force=True, silent=True) or {}
    status = (data.get("status") or "").strip().lower()
    if status not in ("none", "pending", "approved", "rejected"):
        return jsonify({"error": "Ugyldig status"}), 400
    u["bsf_member_status"] = status
    _save_sync_state()
    return jsonify({"ok": True, "user": _public_user(u)})

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

def _client_geo():
    """Grov geolokasjon fra Vercel-edge-headere (videresendes ved proxy til
    Render). Vi lagrer KUN land/region/by — aldri IP — så vi holder løftet om
    anonym analyse i samtykkebanneret."""
    h = request.headers
    country = (h.get("x-vercel-ip-country") or "").strip().upper()
    region  = (h.get("x-vercel-ip-country-region") or "").strip()
    city    = (h.get("x-vercel-ip-city") or "").strip()
    if city:
        try:
            from urllib.parse import unquote
            city = unquote(city)
        except Exception:
            pass
    return {"country": country, "region": region, "city": city}


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
    geo = _client_geo()
    seen_sids = set()
    accepted = 0
    for raw in events[:100]:
        if not isinstance(raw, dict):
            continue
        t = raw.get("type")
        if t not in ("pageview", "click", "scroll", "exit", "funnel_step", "heartbeat"):
            continue
        # Heartbeat: oppdater bare last_event_at på sesjonen, ikke lagre eventet
        if t == "heartbeat":
            sid = str(raw.get("sid") or "")[:64]
            if sid:
                with ANALYTICS_LOCK:
                    sess = _analytics["sessions"].get(sid)
                    if sess:
                        sess["last_event_at"] = int(raw.get("ts") or time.time() * 1000)
            accepted += 1
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
        if ev.get("sid"):
            seen_sids.add(ev["sid"])
        _analytics_record_event(ev)
        accepted += 1
    # Fest grov geo (land/by) på sesjonene i denne batchen — kun hvis ikke satt.
    if geo.get("country") and seen_sids:
        with ANALYTICS_LOCK:
            for _sid in seen_sids:
                _s = _analytics["sessions"].get(_sid)
                if _s and not _s.get("geo"):
                    _s["geo"] = geo
    return jsonify({"ok": True, "accepted": accepted})

def _range_to_ms_window(rng: str, from_iso: str = "", to_iso: str = "") -> tuple[int, int] | None:
    """Returnerer (start_ms, end_ms) for en analytics-periode. None hvis ugyldig."""
    if rng == "all":
        return (0, int(time.time() * 1000) + 1)
    now_oslo = datetime.now(_OSLO_TZ)
    end_dt = (now_oslo + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if rng == "today":
        start_dt = now_oslo.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt   = start_dt + timedelta(days=1)
    elif rng == "week":
        start_dt = end_dt - timedelta(days=7)
    elif rng == "month":
        start_dt = end_dt - timedelta(days=30)
    elif rng == "quarter":
        start_dt = end_dt - timedelta(days=90)
    elif rng == "halfyear":
        start_dt = end_dt - timedelta(days=182)
    elif rng == "year":
        start_dt = end_dt - timedelta(days=365)
    elif rng == "custom":
        try:
            start_dt = datetime.strptime(from_iso.strip(), "%Y-%m-%d").replace(tzinfo=_OSLO_TZ)
            end_dt   = datetime.strptime(to_iso.strip(),   "%Y-%m-%d").replace(tzinfo=_OSLO_TZ) + timedelta(days=1)
            if end_dt <= start_dt or (end_dt - start_dt).days > 730:
                return None
        except Exception:
            return None
    else:
        return None
    return (int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000))


@app.route("/api/analytics/summary", methods=["GET"])
def api_analytics_summary():
    user, err = _analytics_admin_required()
    if err: return err
    now = int(time.time() * 1000)
    cutoff_24h = max(0, now - 24 * 60 * 60 * 1000)
    cutoff_7d  = max(0, now - 7 * 24 * 60 * 60 * 1000)
    events     = _analytics.get("events", []) or []
    sessions   = _analytics.get("sessions", {}) or {}

    # Periode-filtrerte totals (når admin bytter periode i grafen,
    # skal stat-kortene øverst respektere det). Default 'all' = alle tall.
    rng = (request.args.get("range") or "all").lower().strip()
    window = _range_to_ms_window(rng, request.args.get("from") or "", request.args.get("to") or "")
    if window is None:
        return jsonify({"ok": False, "error": "ugyldig range"}), 400
    start_ms, end_ms = window

    p_pageviews = 0
    p_clicks    = 0
    p_session_ids: set[str] = set()
    for ev in events:
        ts = ev.get("ts", 0) or 0
        if ts < start_ms or ts >= end_ms:
            continue
        t = ev.get("type")
        if t == "pageview":
            p_pageviews += 1
            sid = ev.get("sid")
            if sid: p_session_ids.add(sid)
        elif t == "click":
            p_clicks += 1
    # Sesjoner i periode: started_at innen window
    p_sessions_count = 0
    p_devices: set[str] = set()
    for sid, s in sessions.items():
        if (s.get("started_at") or 0) < start_ms or (s.get("started_at") or 0) >= end_ms:
            continue
        p_sessions_count += 1
        did = s.get("did")
        if did: p_devices.add(did)

    # Total-tall (på tvers av all tid, beholdt for bakoverkompat)
    pageviews  = sum(1 for e in events if e.get("type") == "pageview")
    pv_24h     = sum(1 for e in events if e.get("type") == "pageview" and e.get("ts", 0) >= cutoff_24h)
    pv_7d      = sum(1 for e in events if e.get("type") == "pageview" and e.get("ts", 0) >= cutoff_7d)
    sess_24h   = sum(1 for s in sessions.values() if (s.get("started_at") or 0) >= cutoff_24h)
    devices    = len({s.get("did") for s in sessions.values() if s.get("did")})
    clicks     = sum(1 for e in events if e.get("type") == "click")

    return jsonify({
        "ok": True,
        "range":     rng,
        "period":    {
            "sessions":  p_sessions_count,
            "devices":   len(p_devices),
            "pageviews": p_pageviews,
            "clicks":    p_clicks,
        },
        "totals":    {"events": len(events), "sessions": len(sessions),
                      "devices": devices, "pageviews": pageviews, "clicks": clicks},
        "last_24h":  {"pageviews": pv_24h, "sessions": sess_24h},
        "last_7d":   {"pageviews": pv_7d},
    })

@app.route("/api/analytics/timeseries", methods=["GET"])
def api_analytics_timeseries():
    """Antall besøkende per dag (eller time for 'today') i valgt periode.

    Query:
      range = today | week | month | quarter | halfyear | year | custom
      from  = YYYY-MM-DD (kun for custom)
      to    = YYYY-MM-DD (kun for custom, inklusiv)
    """
    user, err = _analytics_admin_required()
    if err: return err

    rng = (request.args.get("range") or "week").lower().strip()
    now_oslo = datetime.now(_OSLO_TZ)
    granularity = "day"

    if rng == "today":
        start_dt = now_oslo.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt   = start_dt + timedelta(days=1)
        granularity = "hour"
    elif rng == "week":
        end_dt   = (now_oslo + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_dt = end_dt - timedelta(days=7)
    elif rng == "month":
        end_dt   = (now_oslo + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_dt = end_dt - timedelta(days=30)
    elif rng == "quarter":
        end_dt   = (now_oslo + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_dt = end_dt - timedelta(days=90)
    elif rng == "halfyear":
        end_dt   = (now_oslo + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_dt = end_dt - timedelta(days=182)
    elif rng == "year":
        end_dt   = (now_oslo + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_dt = end_dt - timedelta(days=365)
    elif rng == "custom":
        try:
            f = (request.args.get("from") or "").strip()
            t = (request.args.get("to") or "").strip()
            start_dt = datetime.strptime(f, "%Y-%m-%d").replace(tzinfo=_OSLO_TZ)
            end_dt   = datetime.strptime(t, "%Y-%m-%d").replace(tzinfo=_OSLO_TZ) + timedelta(days=1)
            if end_dt <= start_dt:
                return jsonify({"ok": False, "error": "to må være etter from"}), 400
            # cap maks 2 år for å unngå overload
            if (end_dt - start_dt).days > 730:
                return jsonify({"ok": False, "error": "maks 2 år"}), 400
        except Exception:
            return jsonify({"ok": False, "error": "ugyldig from/to (forventet YYYY-MM-DD)"}), 400
    else:
        return jsonify({"ok": False, "error": "ukjent range"}), 400

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms   = int(end_dt.timestamp() * 1000)

    # Initier alle buckets (slik at tomme dager også vises)
    buckets = {}  # key -> {"sessions": set, "pageviews": int}
    keys_order = []
    if granularity == "hour":
        for h in range(24):
            key = f"{h:02d}:00"
            buckets[key] = {"sessions": set(), "pageviews": 0}
            keys_order.append(key)
    else:
        cur = start_dt
        while cur < end_dt:
            key = cur.strftime("%Y-%m-%d")
            buckets[key] = {"sessions": set(), "pageviews": 0}
            keys_order.append(key)
            cur = cur + timedelta(days=1)

    def _bucket_key(ts_ms):
        d = datetime.fromtimestamp(ts_ms / 1000, tz=_OSLO_TZ)
        if granularity == "hour":
            return f"{d.hour:02d}:00"
        return d.strftime("%Y-%m-%d")

    # Sesjoner = unike besøkende (basert på started_at)
    for sid, s in (_analytics.get("sessions", {}) or {}).items():
        ts = s.get("started_at") or 0
        if ts < start_ms or ts >= end_ms:
            continue
        key = _bucket_key(ts)
        if key in buckets:
            buckets[key]["sessions"].add(sid)

    # Pageviews per bucket
    for ev in (_analytics.get("events", []) or []):
        if ev.get("type") != "pageview":
            continue
        ts = ev.get("ts") or 0
        if ts < start_ms or ts >= end_ms:
            continue
        key = _bucket_key(ts)
        if key in buckets:
            buckets[key]["pageviews"] += 1

    rows = [{
        "label":     k,
        "sessions":  len(buckets[k]["sessions"]),
        "pageviews": buckets[k]["pageviews"],
    } for k in keys_order]

    return jsonify({
        "ok": True,
        "range": rng,
        "granularity": granularity,
        "from": start_dt.isoformat(),
        "to":   end_dt.isoformat(),
        "buckets": rows,
        "total_sessions":  sum(r["sessions"]  for r in rows),
        "total_pageviews": sum(r["pageviews"] for r in rows),
    })


def _analytics_range_window():
    """Leser range/from/to fra request og returnerer (start_ms, end_ms).
    Standard er 'all' (hele tid). Returnerer None hvis ugyldig range."""
    rng = (request.args.get("range") or "all").lower().strip()
    return _range_to_ms_window(rng, request.args.get("from") or "", request.args.get("to") or "")


def _filtered_analytics(window):
    """Returnerer (sessions_filtered_dict, events_filtered_list) der bare
    poster innenfor window=(start_ms, end_ms) er med."""
    if window is None:
        return {}, []
    start_ms, end_ms = window
    sessions = {sid: s for sid, s in (_analytics.get("sessions", {}) or {}).items()
                if start_ms <= (s.get("started_at") or 0) < end_ms}
    events = [ev for ev in (_analytics.get("events", []) or [])
              if start_ms <= (ev.get("ts") or 0) < end_ms]
    return sessions, events


def _web_orders_completed_in_window(window):
    """Antall ekte nettside-ordre (kasse-checkout) registrert betalt med
    ordredato i vinduet. Brukes som gulv for «Fullført ordre»-steget i
    funnelen — klient-tracking underteller (adblock/avslått samtykke)."""
    start_ms, end_ms = window
    paid = _paid_ordrenrs()
    n = 0
    for o in (_manual_orders or []):
        if not isinstance(o, dict) or o.get("manual"):
            continue
        src   = (o.get("source") or "").lower()
        kilde = (o.get("kilde") or "").lower()
        if src in ("admin", "shopify") or "shopify" in kilde or "import" in kilde:
            continue
        store = o.get("store")
        if store and store != "Havøyet":
            continue
        if str(o.get("ordrenr") or o.get("id") or "") not in paid:
            continue
        d = str(o.get("dato") or o.get("created_at") or "")[:10]
        try:
            ts = int(datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=_OSLO_TZ).timestamp() * 1000)
        except (ValueError, TypeError):
            continue
        if start_ms <= ts < end_ms:
            n += 1
    return n


@app.route("/api/analytics/funnel", methods=["GET"])
def api_analytics_funnel():
    user, err = _analytics_admin_required()
    if err: return err
    window = _analytics_range_window()
    if window is None:
        return jsonify({"ok": False, "error": "ugyldig range"}), 400
    sessions, _ = _filtered_analytics(window)
    steps  = ["session_start", "view_pdp", "add_to_cart", "begin_checkout", "order_complete"]
    counts = {s: 0 for s in steps}
    # Abonnement-signup (Sjømatkasse) er en EGEN flyt uten handlekurv og holdes
    # UTENFOR produkt-trakten. «Startet kassen» = kun de som gikk videre til
    # kassen etter å ha lagt noe i handlekurven. Abonnement rapporteres som eget
    # tall så det ikke blåser opp kasse-steget. NB: historiske økter (før
    # tracker-skillet 2026-06-08) lagret abonnement som begin_checkout og kan
    # ikke skilles tilbakevirkende — skillet gjelder nye økter.
    subscriptions = 0
    for sess in sessions.values():
        counts["session_start"] += 1
        f = sess.get("funnel") or {}
        for s in steps[1:]:
            if s in f:
                counts[s] += 1
        if "begin_subscription" in f:
            subscriptions += 1
    # «Fullført ordre» skal speile virkeligheten: bruk ekte betalte
    # nettside-ordre i perioden som gulv (klient-events blokkeres av
    # adblock / avslått samtykke / Vipps-app-retur).
    try:
        real_orders = _web_orders_completed_in_window(window)
        if real_orders > counts["order_complete"]:
            counts["order_complete"] = real_orders
    except Exception:
        pass
    # Hvert steg telles uavhengig og sant: add_to_cart = økter som la noe i
    # handlekurven (eller kom tilbake med en lagret kurv — se app.jsx), og
    # begin_checkout = økter som faktisk gikk inn i kassen (/kasse). Klienten
    # fyrer nå add_to_cart også for gjenopprettede kurver, så kurv-steget ≥
    # kasse-steget naturlig (ekte kurv-frafall blir synlig). «rate» cappes på
    # 100 % som forsvar mot historiske/sjeldne ikke-monotone vinduer.
    rows = []
    base = counts[steps[0]] or 1
    for i, s in enumerate(steps):
        n = counts[s]
        rows.append({
            "step":       s,
            "count":      n,
            "rate":       min(100.0, round(n / (counts[steps[i-1]] or 1) * 100, 1)) if i > 0 else 100.0,
            "rate_total": min(100.0, round(n / base * 100, 1)),
        })
    return jsonify({"ok": True, "steps": rows, "subscriptions": subscriptions})

@app.route("/api/analytics/deepdive", methods=["GET"])
def api_analytics_deepdive():
    """«Mer data» for Daglig rapport — alt vi kan utlede av analytics-sesjoner
    + ordrehistorikk for valgt periode, i ett kall. Blokker:
      visitors (nye/tilbakevendende/ukjente), devices, browsers, engagement,
      referrers, top_products, cart_products, clicks, hours, geo, buyers."""
    import re
    user, err = _analytics_admin_required()
    if err: return err
    window = _analytics_range_window()
    if window is None:
        return jsonify({"ok": False, "error": "ugyldig range"}), 400
    start_ms, end_ms = window
    sessions, events = _filtered_analytics(window)

    # — Nye vs tilbakevendende besøkende —
    # Stabil enhets-ID (did) finnes kun hos besøkende med statistikk-samtykke.
    # Anonyme får per-fane-ID («anon-…») og kan ikke gjenkjennes på tvers av
    # økter — de rapporteres ærlig som «ukjent».
    first_seen = {}
    for s2 in (_analytics.get("sessions", {}) or {}).values():
        d2 = s2.get("did") or ""
        if not d2 or d2.startswith("anon"):
            continue
        t2 = s2.get("started_at") or 0
        if d2 not in first_seen or t2 < first_seen[d2]:
            first_seen[d2] = t2
    vis_new = vis_ret = vis_unknown = 0
    for s in sessions.values():
        did = s.get("did") or ""
        if not did or did.startswith("anon"):
            vis_unknown += 1
        elif (s.get("started_at") or 0) > first_seen.get(did, 0):
            vis_ret += 1
        else:
            vis_new += 1

    # — Enheter + nettlesere (fra User-Agent) —
    dev = {"mobil": 0, "nettbrett": 0, "desktop": 0}
    brw = {}
    for s in sessions.values():
        ua = s.get("user_agent") or ""
        if re.search(r"iPad|Tablet", ua, re.I):
            dev["nettbrett"] += 1
        elif re.search(r"Mobi|Android.+Mobile|iPhone", ua, re.I):
            dev["mobil"] += 1
        else:
            dev["desktop"] += 1
        if "Edg/" in ua:
            b = "Edge"
        elif "OPR/" in ua or "Opera" in ua:
            b = "Opera"
        elif "Firefox/" in ua:
            b = "Firefox"
        elif "Chrome/" in ua or "CriOS" in ua:
            b = "Chrome"
        elif "Safari/" in ua:
            b = "Safari"
        else:
            b = "Annet"
        brw[b] = brw.get(b, 0) + 1

    # — Engasjement —
    durs, page_counts = [], []
    bounce = 0
    for s in sessions.values():
        d_ms = max(0, (s.get("last_event_at") or 0) - (s.get("started_at") or 0))
        if d_ms > 0:
            durs.append(d_ms)
        n_pages = len(s.get("pages") or [])
        page_counts.append(n_pages)
        if n_pages <= 1:
            bounce += 1
    scrolls = [ev.get("max_scroll") or 0 for ev in events if ev.get("type") == "exit"]
    n_sess = len(sessions) or 1
    engagement = {
        "avg_duration_s":  round(sum(durs) / len(durs) / 1000.0, 1) if durs else 0,
        "avg_pages":       round(sum(page_counts) / n_sess, 1),
        "bounce_pct":      round(bounce / n_sess * 100, 1),
        "avg_scroll_pct":  round(sum(scrolls) / len(scrolls), 1) if scrolls else 0,
        "total_time_min":  round(sum(durs) / 1000.0 / 60.0, 1),
    }

    # — Trafikk-kilder (referrer per økt) —
    refs = {}
    for s in sessions.values():
        r = (s.get("referrer") or "").strip()
        if not r:
            label = "Direkte / bokmerke"
        else:
            m = re.match(r"https?://([^/]+)", r)
            host = (m.group(1) if m else r).lower().replace("www.", "")
            if "havoyet" in host or "havøyet" in host:
                label = "Internt (havoyet.no)"
            elif "google" in host:
                label = "Google"
            elif "facebook" in host or "fb." in host:
                label = "Facebook"
            elif "instagram" in host:
                label = "Instagram"
            elif "bing" in host:
                label = "Bing"
            else:
                label = host[:60]
        refs[label] = refs.get(label, 0) + 1
    referrers = sorted(refs.items(), key=lambda kv: -kv[1])[:12]

    # — Mest sette produkter (pageviews på /produkt/<slug>) —
    prod = {}
    for ev in events:
        if ev.get("type") != "pageview":
            continue
        m = re.match(r"^/produkt/([^/?#]+)", ev.get("path") or "")
        if m:
            slug = m.group(1)[:60]
            prod[slug] = prod.get(slug, 0) + 1
    top_products = sorted(prod.items(), key=lambda kv: -kv[1])[:12]

    # — Lagt i kurv (meta-tekst fra add_to_cart-events) —
    cart = {}
    for ev in events:
        if ev.get("type") == "funnel_step" and ev.get("step") == "add_to_cart":
            meta = (ev.get("meta") or "").strip()
            label = meta[:60] if meta else "(ukjent produkt)"
            cart[label] = cart.get(label, 0) + 1
    cart_products = sorted(cart.items(), key=lambda kv: -kv[1])[:12]

    # — Mest klikkede elementer —
    clk = {}
    for ev in events:
        if ev.get("type") == "click" and ev.get("target"):
            t = str(ev["target"])[:80]
            clk[t] = clk.get(t, 0) + 1
    top_clicks = sorted(clk.items(), key=lambda kv: -kv[1])[:12]

    # — Aktivitet per time (Oslo-tid) —
    hours = [0] * 24
    for s in sessions.values():
        ts = s.get("started_at") or 0
        if ts:
            h = datetime.fromtimestamp(ts / 1000.0, _OSLO_TZ).hour
            hours[h] += 1

    # — Geografi (fra Vercel-edge-geo på sesjonene) —
    geo_cnt = {}
    for s in sessions.values():
        g = s.get("geo") or {}
        city = (g.get("city") or "").strip()
        country = (g.get("country") or "").strip()
        if city:
            label = city if country in ("NO", "Norway", "Norge", "") else f"{city} ({country})"
        elif country:
            label = country
        else:
            label = "Ukjent"
        geo_cnt[label] = geo_cnt.get(label, 0) + 1
    geo_rows = sorted(geo_cnt.items(), key=lambda kv: -kv[1])[:12]

    # — Kjøpere: førstegang vs gjentakende —
    # Matcher dagens betalte nettside-ordre mot HELE ordrehistorikken
    # (inkl. gamle Shopify-importer) på normalisert telefon/epost.
    def _cust_keys(o):
        k = o.get("kunde") or {}
        keys = set()
        tlf = re.sub(r"\D", "", str(k.get("tlf") or ""))
        if len(tlf) >= 8:
            keys.add("t:" + tlf[-8:])
        ep = str(k.get("epost") or "").strip().lower()
        if ep:
            keys.add("e:" + ep)
        return keys
    paid = _paid_ordrenrs()
    buyers = []
    for o in (_manual_orders or []):
        if not isinstance(o, dict):
            continue
        nr = str(o.get("ordrenr") or o.get("id") or "")
        if nr not in paid:
            continue
        d = str(o.get("dato") or o.get("created_at") or "")[:10]
        try:
            ts = int(datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=_OSLO_TZ).timestamp() * 1000)
        except (ValueError, TypeError):
            continue
        if not (start_ms <= ts < end_ms):
            continue
        keys = _cust_keys(o)
        prior = 0
        if keys:
            for o2 in (_manual_orders or []):
                if not isinstance(o2, dict) or o2 is o:
                    continue
                d2 = str(o2.get("dato") or o2.get("created_at") or "")[:10]
                if not d2 or d2 >= d:
                    continue
                if keys & _cust_keys(o2):
                    prior += 1
        buyers.append({
            "ordrenr":    o.get("ordrenr"),
            "navn":       (o.get("kunde") or {}).get("navn") or "",
            "sum":        float(o.get("sum") or o.get("total") or 0),
            "first_time": prior == 0,
            "prior_orders": prior,
        })
    buyers_summary = {
        "first_time": sum(1 for b in buyers if b["first_time"]),
        "returning":  sum(1 for b in buyers if not b["first_time"]),
    }

    return jsonify({
        "ok": True,
        "visitors": {
            "total":     len(sessions),
            "new":       vis_new,
            "returning": vis_ret,
            "unknown":   vis_unknown,
        },
        "devices":      dev,
        "browsers":     sorted(brw.items(), key=lambda kv: -kv[1]),
        "engagement":   engagement,
        "referrers":    [{"label": l, "count": n} for l, n in referrers],
        "top_products": [{"slug": s_, "views": n} for s_, n in top_products],
        "cart_products": [{"label": l, "count": n} for l, n in cart_products],
        "top_clicks":   [{"target": t, "count": n} for t, n in top_clicks],
        "hours":        hours,
        "geo":          [{"label": l, "count": n} for l, n in geo_rows],
        "buyers":       {"orders": buyers, "summary": buyers_summary},
    })


@app.route("/api/analytics/dropoff", methods=["GET"])
def api_analytics_dropoff():
    user, err = _analytics_admin_required()
    if err: return err
    window = _analytics_range_window()
    if window is None:
        return jsonify({"ok": False, "error": "ugyldig range"}), 400
    sessions, _ = _filtered_analytics(window)
    cnt = {}
    for sess in sessions.values():
        if (sess.get("funnel") or {}).get("order_complete"):
            continue
        p = sess.get("last_path") or "(ukjent)"
        cnt[p] = cnt.get(p, 0) + 1
    rows = sorted(cnt.items(), key=lambda kv: -kv[1])[:20]
    return jsonify({"ok": True, "rows": [{"path": p, "count": n} for p, n in rows]})

@app.route("/api/analytics/geo", methods=["GET"])
def api_analytics_geo():
    """Hvor besøkende kommer fra (grov plassering fra Vercel-edge). Aggregert
    pr. by for Norge + land-fordeling for utenlandsk trafikk. Ingen IP lagres."""
    user, err = _analytics_admin_required()
    if err: return err
    window = _analytics_range_window()
    if window is None:
        return jsonify({"ok": False, "error": "ugyldig range"}), 400
    sessions, _ = _filtered_analytics(window)
    by_city, by_country = {}, {}
    no_count = foreign = unknown = total = 0
    for s in sessions.values():
        total += 1
        g = s.get("geo") or {}
        country = (g.get("country") or "").upper()
        if not country:
            unknown += 1
            continue
        if country == "NO":
            no_count += 1
            city = (g.get("city") or "").strip() or "Ukjent by"
            by_city[city] = by_city.get(city, 0) + 1
        else:
            foreign += 1
            by_country[country] = by_country.get(country, 0) + 1
    cities    = sorted(by_city.items(),    key=lambda kv: -kv[1])[:20]
    countries = sorted(by_country.items(), key=lambda kv: -kv[1])[:10]
    return jsonify({
        "ok": True, "total": total,
        "norway": no_count, "foreign": foreign, "unknown": unknown,
        "cities":    [{"name": n, "count": c} for n, c in cities],
        "countries": [{"code": n, "count": c} for n, c in countries],
    })

@app.route("/api/analytics/pages", methods=["GET"])
def api_analytics_pages():
    user, err = _analytics_admin_required()
    if err: return err
    window = _analytics_range_window()
    if window is None:
        return jsonify({"ok": False, "error": "ugyldig range"}), 400
    _, events = _filtered_analytics(window)
    pv, ck, ex, t_ms, scr = {}, {}, {}, {}, {}
    # Tracker-en kan sende flere exit-events per sidevisning (visibilitychange
    # + pagehide + tab-bytting). Tell maks ÉN exit per (økt, side), ellers blir
    # exits > pageviews og exit-raten umulig (267–315 %). Tracker-en er også
    # rettet til å sende én exit per visning, men dette dekker historiske data.
    seen_exit = set()
    for ev in events:
        p, t = ev.get("path") or "", ev.get("type")
        if   t == "pageview": pv[p] = pv.get(p, 0) + 1
        elif t == "click":    ck[p] = ck.get(p, 0) + 1
        elif t == "exit":
            sid = ev.get("sid") or ""
            if sid:
                key = (sid, p)
                if key in seen_exit:
                    continue          # dobbel-exit for samme økt+side — hopp over
                seen_exit.add(key)
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
    window = _analytics_range_window()
    if window is None:
        return jsonify({"ok": False, "error": "ugyldig range"}), 400
    sessions, _ = _filtered_analytics(window)
    seq_count = {}
    for sess in sessions.values():
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
    window = _analytics_range_window()
    if window is None:
        return jsonify({"ok": False, "error": "ugyldig range"}), 400
    sessions, _ = _filtered_analytics(window)
    items = sorted(sessions.items(),
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
    phone = (session.get("customer") or {}).get("phone") or ""
    sid = session.get("id")
    subj = f"[Havøyet chat] {name} ber om hjelp"
    # Plain-text fallback
    history_text = ""
    for m in (session.get("messages") or [])[-12:]:
        who = {"customer": name or "Kunde", "ai": "Bot", "admin": "Admin"}.get(m.get("role"), m.get("role"))
        history_text += f"{who}: {m.get('text','')}\n\n"
    body = (
        f"Ny chat-henvendelse fra havoyet.no\n"
        f"{'-'*54}\n"
        f"Navn:    {name}\n"
        f"E-post:  {email or '(ikke oppgitt)'}\n"
        f"Tid:     {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        f"Spørsmål:\n{customer_question}\n\n"
        f"Samtalehistorikk:\n{history_text}\n"
        f"Åpne i admin: https://admin.havoyet.no/#chat\n"
    )
    html_body = _format_message_email_html(
        source="Chat",
        navn=name, epost=email, tlf=phone,
        melding=customer_question,
        session_id=sid,
        history=session.get("messages") or [],
    )
    sent_any = False
    # Send via varselssystemet så alle admin-mottakere med «Ny melding» avkrysset
    # får chat-handoff-en (e-post med Reply-To til kunden + SMS/push/telegram
    # etter kanal-avkrysningene). CHAT_HUMAN_RECIPIENTS beholdes kun som
    # fallback hvis ingen mottakere er registrert eller alle sendinger feiler.
    try:
        if _notify_admins("new_message", subj, body, html_body=html_body, reply_to=(email or None)):
            sent_any = True
    except Exception as e:
        print(f"[CHAT] varsel via mottaker-liste feilet: {e}")
    if not sent_any:
        for to in CHAT_HUMAN_RECIPIENTS:
            try:
                ok, _ = _send_admin_mail(to, subj, body, html_body=html_body)
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


# ── NYHETSBREV-ÅRSHJUL (52-ukers redaksjonsplan) ──────────────────────────
# Lagrer planlagte temaer per ISO-uke + defaults. Cron-jobben (Phase 2) leser
# planen og lager Claude-utkast på planlagt dato. Nøkkelformat: "YYYY-Www".
YEARPLAN_FILE = os.path.join(STATE_DIR, "havoyet_newsletter_yearplan.json")
_yearplan = {"defaults": {}, "weeks": {}}
_yearplan_lock = threading.Lock()

_YEARPLAN_DEFAULTS = {
    "weekday": 1,            # 0=mandag .. 6=søndag
    "send_time": "09:00",
    "def_tone": "Personlig og direkte",
    "def_type": "Ukentlig fiskebrev",
    "enabled": False,
}


def _load_yearplan():
    global _yearplan
    try:
        if os.path.exists(YEARPLAN_FILE):
            with open(YEARPLAN_FILE, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
                _yearplan = {
                    "defaults": {**_YEARPLAN_DEFAULTS, **(data.get("defaults") or {})},
                    "weeks": data.get("weeks") or {},
                }
        else:
            _yearplan = {"defaults": dict(_YEARPLAN_DEFAULTS), "weeks": {}}
    except Exception as e:
        print(f"[YEARPLAN] Kunne ikke laste: {e}")
        _yearplan = {"defaults": dict(_YEARPLAN_DEFAULTS), "weeks": {}}


def _save_yearplan():
    try:
        tmp = YEARPLAN_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_yearplan, f, ensure_ascii=False, indent=2)
        os.replace(tmp, YEARPLAN_FILE)
    except Exception as e:
        print(f"[YEARPLAN] save feilet: {e}")


_load_yearplan()


def _yp_week_key_valid(key):
    # Format: "2026-W19"
    try:
        if not isinstance(key, str) or "-W" not in key:
            return False
        y, w = key.split("-W", 1)
        yi, wi = int(y), int(w)
        return 2020 <= yi <= 2099 and 1 <= wi <= 53
    except Exception:
        return False


@app.route("/api/admin/newsletter-yearplan", methods=["GET", "PUT"])
def api_yearplan_root():
    if not _is_admin_request():
        return jsonify({"error": "Forbidden"}), 403
    if request.method == "GET":
        year_q = request.args.get("year")
        with _yearplan_lock:
            defaults = dict(_yearplan.get("defaults") or {})
            if year_q:
                prefix = f"{year_q}-W"
                weeks = {k: v for k, v in (_yearplan.get("weeks") or {}).items() if k.startswith(prefix)}
            else:
                weeks = dict(_yearplan.get("weeks") or {})
        return jsonify({"ok": True, "defaults": {**_YEARPLAN_DEFAULTS, **defaults}, "weeks": weeks})
    # PUT — oppdater defaults
    data = request.get_json(silent=True) or {}
    new_defaults = data.get("defaults") or {}
    if not isinstance(new_defaults, dict):
        return jsonify({"error": "'defaults' må være objekt"}), 400
    cleaned = {}
    for k, v in new_defaults.items():
        if k not in _YEARPLAN_DEFAULTS:
            continue
        if k == "weekday":
            try:
                vi = int(v)
                if 0 <= vi <= 6: cleaned[k] = vi
            except Exception:
                pass
        elif k == "enabled":
            cleaned[k] = bool(v)
        elif k == "send_time":
            if isinstance(v, str) and len(v) <= 5 and ":" in v:
                cleaned[k] = v
        else:
            if isinstance(v, str) and v.strip():
                cleaned[k] = v.strip()[:120]
    with _yearplan_lock:
        _yearplan["defaults"] = {**_YEARPLAN_DEFAULTS, **(_yearplan.get("defaults") or {}), **cleaned}
        _save_yearplan()
        return jsonify({"ok": True, "defaults": _yearplan["defaults"]})


@app.route("/api/admin/newsletter-yearplan/week/<week_key>", methods=["PUT", "DELETE"])
def api_yearplan_week(week_key):
    if not _is_admin_request():
        return jsonify({"error": "Forbidden"}), 403
    if not _yp_week_key_valid(week_key):
        return jsonify({"error": "Ugyldig uke-nøkkel (forventet YYYY-Www)"}), 400
    if request.method == "DELETE":
        with _yearplan_lock:
            removed = (_yearplan.get("weeks") or {}).pop(week_key, None)
            if removed is not None:
                _save_yearplan()
        return jsonify({"ok": True, "removed": removed is not None})
    # PUT — opprett/oppdater uka
    data = request.get_json(silent=True) or {}
    week_entry = {
        "tema": (data.get("tema") or "").strip()[:1000],
        "tone": (data.get("tone") or None) if data.get("tone") else None,
        "type": (data.get("type") or None) if data.get("type") else None,
        "skip": bool(data.get("skip")),
        "updated_at": int(time.time() * 1000),
    }
    # Bevar tidligere auto-gen-metadata hvis det finnes (settes av cron i Phase 2)
    with _yearplan_lock:
        prev = (_yearplan.get("weeks") or {}).get(week_key) or {}
        for keep in ("auto_generated_at", "draft_id"):
            if keep in prev:
                week_entry[keep] = prev[keep]
        _yearplan.setdefault("weeks", {})[week_key] = week_entry
        _save_yearplan()
    return jsonify({"ok": True, "week": week_entry})


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
    deny = _require_admin_user()
    if deny: return deny
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
    # max_uses: hvis satt, sjekk at brukstelling ikke er nådd
    max_uses = d.get("max_uses")
    if max_uses is not None:
        try:
            if int(d.get("uses") or 0) >= int(max_uses):
                return False
        except (TypeError, ValueError):
            pass
    return True


def _is_bsf_approved(email):
    """True hvis brukeren er godkjent Bergen Seilforening-medlem."""
    if not email:
        return False
    u = _find_user(email)
    return bool(u and u.get("bsf_member_status") == "approved")


def _discount_applies_to_user(d, email):
    """Sjekker om rabatten gjelder for denne brukeren basert på target_type.
    target_type: 'anyone' (default) | 'email' (en bestemt bruker) | 'bsf_member' (godkjent BSF-medlem) | 'newsletter' (legacy = kun_nyhetsbrev)."""
    tt = d.get("target_type")
    # Backward-compat: gamle rabatter har kun_nyhetsbrev-flagg
    if not tt:
        tt = "newsletter" if d.get("kun_nyhetsbrev") else "anyone"
    if tt == "anyone":
        return True
    if tt == "newsletter":
        return bool(email and _is_active_subscriber(email))
    if tt == "bsf_member":
        return _is_bsf_approved(email)
    if tt == "email":
        target = (d.get("target_email") or "").strip().lower()
        return bool(email and target and email.strip().lower() == target)
    return False


def _active_discounts_for(user_email=None):
    """Returnerer rabatter som faktisk gjelder akkurat nå for den gitte brukeren.
    Tar hensyn til target_type (anyone/email/bsf_member/newsletter)."""
    out = []
    for d in _discounts:
        if not _is_discount_currently_active(d):
            continue
        if not _discount_applies_to_user(d, user_email):
            continue
        out.append(d)
    return out


def _normalize_code(s):
    return (s or "").strip().upper()


def _find_discount_by_code(code):
    code_n = _normalize_code(code)
    if not code_n:
        return None
    for d in _discounts:
        if _normalize_code(d.get("code")) == code_n:
            return d
    return None


@app.route("/api/discounts/validate", methods=["POST"])
def api_discounts_validate():
    """Validerer en rabattkode oppgitt i kassen.
    Body: {code, email?, product_handles?: [..]}.
    Returnerer {ok, discount, applies_to_handles[]} hvis gyldig, ellers {ok:false, error}."""
    data = request.get_json(force=True, silent=True) or {}
    code = _normalize_code(data.get("code"))
    email = _normalize_email(data.get("email"))
    if not code:
        return jsonify({"ok": False, "error": "Mangler rabattkode"}), 400
    d = _find_discount_by_code(code)
    if not d:
        return jsonify({"ok": False, "error": "Ukjent rabattkode"}), 404
    if not _is_discount_currently_active(d):
        return jsonify({"ok": False, "error": "Rabattkoden er utløpt eller ikke aktiv"}), 410
    if not _discount_applies_to_user(d, email):
        return jsonify({"ok": False, "error": "Denne koden gjelder ikke for kontoen din"}), 403
    # Filtrer hvilke handles fra ordren rabatten gjelder for
    cart_handles = data.get("product_handles") or []
    applies_to = (d.get("applies_to") or "all").lower()
    if applies_to == "products":
        allowed = set([(h or "").strip().lower() for h in (d.get("product_handles") or [])])
        matching = [h for h in cart_handles if (h or "").strip().lower() in allowed]
    else:
        matching = list(cart_handles)
    return jsonify({"ok": True, "discount": d, "applies_to_handles": matching})


@app.route("/api/discounts/<discount_id>/mark-used", methods=["POST"])
def api_discount_mark_used(discount_id):
    """Øker uses-teller når en kode faktisk brukes i en ordre. Idempotent
    på order_id hvis sendt (lagres i used_order_ids så samme ordre ikke
    teller dobbelt ved retry)."""
    d = next((x for x in _discounts if x.get("id") == discount_id), None)
    if not d:
        return jsonify({"error": "Ikke funnet"}), 404
    data = request.get_json(force=True, silent=True) or {}
    order_id = (data.get("order_id") or "").strip()
    used_ids = d.setdefault("used_order_ids", [])
    if order_id and order_id in used_ids:
        return jsonify({"ok": True, "discount": d, "already_counted": True})
    if order_id:
        used_ids.append(order_id)
    d["uses"] = int(d.get("uses") or 0) + 1
    _save_sync_state()
    return jsonify({"ok": True, "discount": d})


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
    try:
        prosent = float(data.get("prosent") or 0)
    except (TypeError, ValueError):
        return jsonify({"error": "Ugyldig prosent"}), 400
    free_shipping = bool(data.get("free_shipping", False))
    if prosent < 0 or prosent >= 100:
        return jsonify({"error": "Prosent må være mellom 1 og 99"}), 400
    # 0 % er kun lov når koden gir gratis frakt (ren frakt-kode)
    if prosent <= 0 and not free_shipping:
        return jsonify({"error": "Prosent må være mellom 1 og 99"}), 400
    # Nye felter: applies_to ('all'|'products'), product_handles[], code,
    # target_type ('anyone'|'email'|'bsf_member'|'newsletter'), target_email,
    # max_uses. Backward-compat: 'handle' beholdes som single-produkt for
    # nyhetsbrev-rabatter; nye rabatter bruker applies_to+product_handles.
    applies_to = (data.get("applies_to") or "").strip().lower()
    product_handles = data.get("product_handles") or []
    single_handle = (data.get("handle") or "").strip()
    if not applies_to:
        applies_to = "products" if (single_handle or product_handles) else "all"
    if applies_to == "products":
        if not product_handles and single_handle:
            product_handles = [single_handle]
        product_handles = [str(h).strip() for h in product_handles if str(h).strip()]
        if not product_handles:
            return jsonify({"error": "Mangler produkter (applies_to=products)"}), 400
    else:
        product_handles = []
    target_type = (data.get("target_type") or "").strip().lower()
    if not target_type:
        target_type = "newsletter" if data.get("kun_nyhetsbrev", False) else "anyone"
    if target_type not in ("anyone", "email", "bsf_member", "newsletter"):
        return jsonify({"error": "Ugyldig target_type"}), 400
    target_email = ""
    if target_type == "email":
        target_email = _normalize_email(data.get("target_email"))
        if not target_email:
            return jsonify({"error": "Mangler target_email"}), 400
    code = _normalize_code(data.get("code"))
    if code and _find_discount_by_code(code):
        return jsonify({"error": "Rabattkoden finnes allerede"}), 409
    max_uses = data.get("max_uses")
    if max_uses not in (None, ""):
        try:
            max_uses = int(max_uses)
            if max_uses <= 0:
                max_uses = None
        except (TypeError, ValueError):
            max_uses = None
    else:
        max_uses = None
    start = (data.get("start") or _today_str()).strip()
    slutt = (data.get("slutt") or "").strip()
    if not _valid_date(start):
        return jsonify({"error": "Ugyldig start-dato (forventer YYYY-MM-DD)"}), 400
    if slutt and not _valid_date(slutt):
        return jsonify({"error": "Ugyldig slutt-dato (forventer YYYY-MM-DD)"}), 400
    now = datetime.now().isoformat()
    new = {
        "id": "d_" + _uuid.uuid4().hex[:12],
        "handle": single_handle or (product_handles[0] if product_handles else ""),
        "applies_to": applies_to,
        "product_handles": product_handles,
        "code": code or None,
        "target_type": target_type,
        "target_email": target_email or None,
        "max_uses": max_uses,
        "uses": 0,
        "used_order_ids": [],
        "prosent": prosent,
        "free_shipping": free_shipping,
        "start": start,
        "slutt": slutt or None,
        "beskrivelse": (data.get("beskrivelse") or "").strip()[:200],
        "kun_nyhetsbrev": target_type == "newsletter",
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
    for f in ("kun_nyhetsbrev", "aktiv", "free_shipping"):
        if f in data:
            d[f] = bool(data[f])
    # Nye felter
    if "applies_to" in data:
        v = (data["applies_to"] or "").strip().lower()
        if v in ("all", "products"):
            d["applies_to"] = v
    if "product_handles" in data and isinstance(data["product_handles"], list):
        d["product_handles"] = [str(h).strip() for h in data["product_handles"] if str(h).strip()]
    if "code" in data:
        new_code = _normalize_code(data["code"]) or None
        if new_code and new_code != _normalize_code(d.get("code")):
            existing = _find_discount_by_code(new_code)
            if existing and existing.get("id") != d.get("id"):
                return jsonify({"error": "Rabattkoden finnes allerede"}), 409
        d["code"] = new_code
    if "target_type" in data:
        v = (data["target_type"] or "").strip().lower()
        if v in ("anyone", "email", "bsf_member", "newsletter"):
            d["target_type"] = v
            if v == "newsletter":
                d["kun_nyhetsbrev"] = True
    if "target_email" in data:
        d["target_email"] = _normalize_email(data["target_email"]) or None
    if "max_uses" in data:
        v = data["max_uses"]
        if v in (None, ""):
            d["max_uses"] = None
        else:
            try:
                mv = int(v)
                d["max_uses"] = mv if mv > 0 else None
            except (TypeError, ValueError):
                pass
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
        "category_config":        _category_config,
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
    global _product_overrides, _category_config, _reviews, _customer_favorites, _admin_notifiers
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
    _category_config       = _maybe("category_config",        _category_config)
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


# ════════════════════════════════════════════════════════════════════════════
# SALGSSTRATEGI ("underbevisstheten") — én kilde til sannhet for all AI + admin
# ────────────────────────────────────────────────────────────────────────────
# Lagrer den fulle forretningsplanen (intern), en kundetrygg kortversjon for
# chat-boten, strukturerte mål/KPI-er, og en etterlevelses-logg. Samme
# atomiske JSON-mønster som resten av appen. Brukes av:
#   • Vercel /api/_strategi.js  → injiserer i nyhetsbrev/ukens-fisk/daglig rapport
#   • Vercel /api/chat-ai.js    → henter kun kundetrygg posture (scope=chat)
#   • admin.html «Strategi»-fane → vis/rediger + fremdrift + etterlevelse
# ════════════════════════════════════════════════════════════════════════════
STRATEGI_FILE     = os.path.join(STATE_DIR, "havoyet_strategi.json")
STRATEGI_LOG_FILE = os.path.join(STATE_DIR, "havoyet_strategi_log.jsonl")
_strategi      = {}
_strategi_lock = threading.Lock()

# Kundetrygg salgsholdning — det ENESTE chat-boten får. Ingen interne tall.
_STRATEGI_CHAT_POSTURE_DEFAULT = (
    "== SALGSHOLDNING (intern — påvirker tone og hva du vektlegger; "
    "nevn ALDRI tall, mål eller strategi til kunden) ==\n"
    "Du representerer Havøyet. Hold tonen varm, personlig og ærlig — aldri "
    "masete eller selgende. Konkurrenten er tidsmangel og middagsstress, ikke "
    "andre butikker.\n"
    "Når det passer naturlig OG du har dekning i kunnskapsbasen:\n"
    "  • Fremhev gjerne skalldyrkassene (flaggskipet) når kunden er usikker.\n"
    "  • Minn vennlig om gratis frakt over 1 100 kr hvis kunden er nær grensen.\n"
    "  • Nevn «Skalldyrfredag» som et hyggelig fast konsept hvis fredag/helg "
    "kommer opp.\n"
    "Dette overstyrer ALDRI faktareglene over: ikke funn på fakta, ikke press. "
    "Er du usikker, foreslå en person."
)

# Hele forretningsplanen (intern). Seedes ved første oppstart; admin kan redigere.
_STRATEGI_FULL_DEFAULT = """# Havøyet — salgsstrategi og kontekst for Claude

Du er en dedikert salgs- og markedsføringsassistent for **Havøyet**, et Bergen-basert sjømatselskap som leverer fersk sjømat hjem til private og bedrifter. Du kjenner selskapet, strategien og kundereisen i detalj. Bruk alltid denne konteksten aktivt når du hjelper teamet.

## Om selskapet
Havøyet leverer fersk norsk sjømat hjem til dør i Bergen og omegn. Fisken hentes via Domstein på fiskekaia, fileteres og pakkes på is av teamet selv. Sortimentet inkluderer både villfanget og oppdrett. Aldri fryst. Tidlig vekstfase med sterkt produkt og bevist kundebehov.
Posisjon: "Ekte restaurantkvalitet hjemme."
Nettside: havoyet.no · Leveringsdager: alle hverdager kl. 13–18 · Frist fisk: kl. 12 dagen før · Frist skalldyr: søndag før leveringsuke · Frakt: 199 (<700) / 59 (700–1100) / gratis (>1100) · Kapasitet ~120 ordre/mnd.

### Nøkkeltall (per juni 2026)
Omsetning YTD ~100 000 kr · 65 kunder · 9 gjenkjøp (3+) · snittordre ~1 100 kr · 0 abonnenter · 1 736 besøk → 2 kjøp (0,12 %) · 47 påbegynte checkout uten fullføring. MÅL: 1 000 000 kr innen 31.12.2026.

### Sortiment
Skalldyrkasser (608–1 765 kr, flaggskip, 4 størrelser) · enkeltprodukter (torsk, brosme, laks, kamskjell, reker, sjøkreps m.m.) · catering/event til bedrifter · julegavekasser (sesong).

## Strategisk rammeverk
Fire vekstmotorer (prioritert): 1) Konvertering (0,12 %→1 %) · 2) Gjenkjøp (aktivere 65 kunder personlig) · 3) B2B/event (catering, kundekveld, julegaver) · 4) Abonnement (gjenkjøp → fast levering).

Tre faser i 2026:
- Fase 1 — Bygg motoren (jun–aug): mål 50 000 kr/mnd innen aug. UGC, ukentlig Skalldyrfredag, personlig gjenkjøpsoppsøking, nettsidefiks.
- Fase 2 — Skaler B2B (sep–okt): mål 100 000 kr/mnd innen okt. Eiendomsmeglere, advokater, konsulenter i Bergen. Referral. Partneravtaler med eventplanleggere.
- Fase 3 — Juleteft (nov–des): mål 1 million totalt. Bedriftsgavekasser, jule-catering, avisomtale, dropzone undersøkes.

## Kundereise og SMS-trakt (manuelt og personlig — aldri automatisert)
- Steg 1 — Dag 2: Tilbakemelding. Bygg relasjon, vis at dere bryr dere, få innsikt.
- Steg 2 — Dag 14: Ukens fangst. Top of mind, naturlig gjenkjøp, koble til forrige kjøp.
- Steg 3 — Dag 42: Abonnementstilbud. Konverter engangskjøper til fast abonnent (velg frekvens, spar 10 %).
Personaliseringsvariabler: [fornavn], [produktnavn], [fangst], [antall uker siden], anledning, antall i husstand, allergier/preferanser, område/bydel.

## Konverteringstiltak for havoyet.no
1) Forsideoverskrift «Fersk sjømat levert hjem i Bergen» · 2) Tre CTA over folden (Bestill skalldyrkasse / Se ukens fangst / Bedrift & event) · 3) Tillitssignaler (Aldri fryst · Håndfiletert · Levert på is · Ingen minstekjøp) · 4) Synlig bestillingsfrist · 5) Leveringsinfo på produktsider · 6) Kundeanmeldelser på forside/produktsider · 7) Kasser øverst i navigasjon · 8) Livsstilsbilder framfor hvit bakgrunn · 9) Forlatt handlekurv-SMS 1–2 t etter · 10) Hotjar/Clarity skjermopptak.

## Ukentlig aktivitetsplan
Man: SMS/ring 10 tidligere kunder · kontakt 5 bedrifter · KPI-tavle. Tir: video til Skalldyrfredag · følg opp tilbud. Ons: publiser Skalldyrfredag (IG/TikTok/SMS/e-post). Tor: kontakt 10 kunder · pakk ordrer. Fre: lever · ta bilde/video · publiser kundehistorier. Søn: planlegg uke · gå gjennom pipeline.

## KPI-mål per måned (nye kunder · gjenkjøp · bedriftsevent · abonnenter · omsetning)
Juni 20·10·1·2·25 000 | Juli 25·15·2·5·38 000 | August 30·20·3·10·55 000 | September 35·25·4·15·70 000 | Oktober 40·30·5·20·100 000 | Desember 50·40·8·30·180 000.

## B2B-strategi
Målgrupper i Bergen: eiendomsmeglere (Paradis/Nordås/Fana), advokat-/revisjonsfirmaer i sentrum, konsulenter, eventplanleggere/venues. Rekkefølge: LinkedIn → e-post → telefon (aldri kald telefon). Produkter: skalldyrfest/kundekveld 5 000–20 000 · gavekasser 608–1 765/stk · julegaver nov–des · catering til styremøter. Partner: 10–15 % provisjon per oppdrag.

## Skalldyrfredag-konseptet
Fast ukentlig løfte (ikke kampanje): video + SMS + e-post + sosiale medier på fast dag. Budskap: «Skal du virkelig stå i kø på Meny fredag?» Mål: 50 kasser/mnd.

## Produktpresentasjon for umiddelbart salg
Prinsipp: folk kjøper med øynene og nesen — la dem se, lukte og smake. Online: livsstilsbilder på kjøkkenbord (ikke produktfoto på hvit bakgrunn), kort video av fersk fisk/skalldyr som fileteres/pakkes på is, pris + «bestill innen»-frist rett ved bildet, fri-frakt-grense synlig. Fysisk: smaksprøver, isdisk, fortell hvor fisken kom fra. Alltid ÉN tydelig neste-handling.
Skalldyrkassen er showstopperen (viktigste enkelttiltak): kassen MÅ se fantastisk ut — på stand, i foto og i video. Invester i fin isbunn, tang/sitronskiver som garnityr, og god belysning. Folk stopper for en vakker kasse og bestiller fordi de ser den for seg hjemme. Ta nytt stilbilde til hvert marked; varier garnityret med sesong (sitron/dill om sommeren, gresskar om høsten) så innholdet alltid føles ferskt.

## Stand og marked (salgskanal — umiddelbart salg + tillit)
Stand er en av de mest undervurderte kanalene for Havøyet nå: direkte menneskelig kontakt, umiddelbar tilbakemelding, og «ansikt bak fisken» — bygger akkurat tilliten online-trakten mangler. Mål med stand er ikke bare dagssalg, men å fylle e-postliste + booke leveringer.
På standen (fra interesse til bestilling i ett steg):
- Stor, synlig QR-kode som går rett til BESTILLINGSSIDEN (ikke forsiden). Standtilbud kun for besøkende: «10 % rabatt på første bestilling — scan her». Mål: e-postadresse eller bestilling før de forlater standen.
- iPad/skjerm som spiller en 60-sekunders video av prosessen (fra Domstein til dør) — bevegelse stopper folk og forteller historien du ikke rekker i en kort prat.
- Smaksprøver, isdisk, bestillingsark for levering samme uke, og kontaktinnsamling til nyhetsbrev.
Konkrete muligheter i Bergen:
- Bondens Marked på Fisketorget — annenhver lørdag 10–16, gjentakende, perfekt match (fisk/vilt). Søk som utstiller via bondensmarked.no / Bergen kommune. Mest tilgjengelige kanal.
- Bergen Matfestival — ~3.–5. september 2026 (Festplassen/Byparken), starten av Fase 2. Søk stand i god tid (matfest.no / lokalmat.no).
- Bergen Sjømatfestival — februar (neste runde feb 2027, gratis publikumsdag på Fisketorget). Planlegg tidlig.

## Utgående salg (oppsøkende — vi styrer hvem og når)
Den eneste kanalen der vi velger hvem vi snakker med, uten å vente på å bli funnet. Fire spor:
1. LinkedIn — varmt utgående B2B (Prioritet 1): eiendomsmeglere, advokatfirmaer, konsulentselskaper i Bergen. Søk opp daglig leder/partner, send kort PERSONLIG melding (ikke salgsbrev), nevn noe konkret om dem, tilby prøvekasse til neste kundekveld. Mål: 10 meldinger/uke. Eksempel: «Hei [navn]. Jeg ser dere arrangerer kundekveld jevnlig. Vi leverer fersk skalldyr og sjømat til den typen arrangementer i Bergen — ville du hatt interesse av å høre mer?»
2. Oppsøk nabolaget direkte (rask effekt): premium-boligstrøk Paradis/Fana/Nordås/Hop. Ring på med en liten smaksprøve + enkel flyer. Ikke salg — introduksjon: «Vi leverer fersk sjømat i dette nabolaget — her er en smaksprøve.» Folk husker det; ingen andre gjør det.
3. Velforeninger og borettslag (skalerbart): kontakt styret i Fana/Nordås/Paradis, tilby «nabolagsrabatt» på første bestilling for alle beboere. Én e-post fra styret til 80 husstander slår 80 individuelle henvendelser.
4. Treningssentre/helsestudioer (uutforsket): SATS/Elixia i Fana — rett målgruppe (helsebevisst, god økonomi, opptatt av kvalitetsmat). Tilby å stå i resepsjonen én lørdag med smaksprøver og flyers.

## Salgskanaler (prioritert)
1) Nettside — øke konverteringsraten (høyest ROI, ingen ekstra trafikk nødvendig) · 2) Utgående salg — stand, nabolag, LinkedIn B2B (NY) · 3) personlig gjenkjøpsoppsøking · 4) UGC og sosiale medier · 5) e-post/SMS ukentlig fangst (SMS sendes manuelt) · 6) B2B oppsøking via LinkedIn · 7) referral-program (200 kr til begge) · 8) pressekontakt — BA og Bergens Tidende · 9) dropzone/hentepunkt — undersøkes nærmere.

## Tone og merkevare
Aldri mas/salgspress — varm, personlig, ærlig. Historiefortelling om opphav. Premium uten snobbing. Grunnlegger-stemme (Erik) brukes aktivt. Konkurrent = tidsmangel/middagsstress/takeaway, ikke Meny/Rema.

## Instruksjoner for Claude-assistenten
Alltid: 1) Ha strategien i bakhodet — alle råd støtter én av de fire vekstmotorene. 2) Prioriter etter fase (jun–aug / sep–okt / nov–des). 3) Vær konkret (Bergen, sjømat, Havøyets kundeprofil) — ikke generelle råd. 4) Respekter merkevaren — ingen kald massetelefon, ingen generiske kampanjer, alltid personlig og ekte. 5) Foreslå konkrete SMS/e-post/LinkedIn-maler med personaliseringsvariabler. 6) Plasser hver kundekontakt i riktig trakt-steg (dag 2 / 14 / 42). 7) Hold fokus på 1 million i 2026 — vurder alltid tiltak mot om de bringer oss nærmere.
"""

# Strukturerte mål/KPI — dr* fremdrift-fanen. Måned → (nye_kunder, gjenkjøp, bedriftsevent, abonnenter, omsetning)
_STRATEGI_KPI_DEFAULT = {
    "2026-06": {"nye_kunder": 20, "gjenkjop": 10, "bedriftsevent": 1, "abonnenter": 2,  "omsetning": 25000},
    "2026-07": {"nye_kunder": 25, "gjenkjop": 15, "bedriftsevent": 2, "abonnenter": 5,  "omsetning": 38000},
    "2026-08": {"nye_kunder": 30, "gjenkjop": 20, "bedriftsevent": 3, "abonnenter": 10, "omsetning": 55000},
    "2026-09": {"nye_kunder": 35, "gjenkjop": 25, "bedriftsevent": 4, "abonnenter": 15, "omsetning": 70000},
    "2026-10": {"nye_kunder": 40, "gjenkjop": 30, "bedriftsevent": 5, "abonnenter": 20, "omsetning": 100000},
    "2026-12": {"nye_kunder": 50, "gjenkjop": 40, "bedriftsevent": 8, "abonnenter": 30, "omsetning": 180000},
}
_STRATEGI_ENGINES = ["Konvertering", "Gjenkjøp", "B2B/event", "Abonnement"]
_STRATEGI_PHASES = [
    {"navn": "Fase 1 — Bygg motoren",  "fra": "2026-06", "til": "2026-08", "maned_mal": 50000},
    {"navn": "Fase 2 — Skaler B2B",     "fra": "2026-09", "til": "2026-10", "maned_mal": 100000},
    {"navn": "Fase 3 — Juleteft",       "fra": "2026-11", "til": "2026-12", "maned_mal": 180000},
]

def _strategi_seed():
    return {
        "version": 1,
        "prompt_full": _STRATEGI_FULL_DEFAULT,
        "chat_posture": _STRATEGI_CHAT_POSTURE_DEFAULT,
        "arsmal": {"label": "Omsetning 2026", "target": 1000000, "unit": "kr"},
        "konvertering": {"baseline": 0.12, "target": 1.0, "unit": "%"},
        "vekstmotorer": list(_STRATEGI_ENGINES),
        "faser": [dict(p) for p in _STRATEGI_PHASES],
        "kpi_mal": {k: dict(v) for k, v in _STRATEGI_KPI_DEFAULT.items()},
        "b2b_events": {},   # manuelt ført pr måned "YYYY-MM" → antall (ingen datakilde)
        "updated_at": _now_iso_utc(),
        "updated_by": "seed",
    }

def _load_strategi():
    global _strategi
    if not os.path.exists(STRATEGI_FILE):
        _strategi = _strategi_seed()
        _save_strategi()
        print("[STRATEGI] Seedet ny forretningsplan")
        return
    try:
        with open(STRATEGI_FILE, "r", encoding="utf-8") as f:
            _strategi = json.load(f) or {}
        # Fyll inn manglende felter fra seed (forward-compat ved nye felter)
        seed = _strategi_seed()
        for k, v in seed.items():
            _strategi.setdefault(k, v)
        print(f"[STRATEGI] Lastet (v{_strategi.get('version')})")
    except Exception as e:
        print(f"[STRATEGI] Kunne ikke laste: {e}")
        _strategi = _strategi_seed()

def _save_strategi():
    try:
        tmp = STRATEGI_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_strategi, f, ensure_ascii=False)
        os.replace(tmp, STRATEGI_FILE)
    except Exception as e:
        print(f"[STRATEGI] Lagring feilet: {e}")

def _strategi_log_append(entry):
    try:
        entry.setdefault("ts", _now_iso_utc())
        with _strategi_lock:
            with open(STRATEGI_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[STRATEGI] Logg feilet: {e}")

def _strategi_internal_ok():
    """Server-til-server-auth for Vercel-funksjonene (intern token)."""
    itok = os.environ.get("STRATEGI_INTERNAL_TOKEN", "")
    if not itok:
        return False
    got = request.headers.get("X-Strategi-Token", "")
    try:
        return bool(got) and secrets.compare_digest(got, itok)
    except Exception:
        return False

def _strategi_progress():
    """Beregner ekte fremdrift mot mål fra ordrer/kunder/abonnement."""
    today = datetime.now().date()
    year  = today.year
    ym    = today.strftime("%Y-%m")

    def _d(s):
        return str(s or "")[:10]
    def _ym(s):
        return str(s or "")[:7]
    def _yr(s):
        return str(s or "")[:4]

    # ── Omsetning (samme kilder som Økonomi-fanen: web + vipps + kort) ──
    try:
        paid_set = _paid_ordrenrs()
    except Exception:
        paid_set = set()
    web_orders = [o for o in _manual_orders
                  if str(o.get("ordrenr") or o.get("id")) in paid_set]
    def _tot(o):
        try:
            return float(o.get("sum") or o.get("total") or 0)
        except (TypeError, ValueError):
            return 0.0
    def _odate(o):
        return o.get("dato") or o.get("created_at") or ""

    rev_year  = sum(_tot(o) for o in web_orders if _yr(_odate(o)) == str(year))
    rev_month = sum(_tot(o) for o in web_orders if _ym(_odate(o)) == ym)
    try:
        for r in _vipps_imported_payments.values():
            amt = (r.get("amount_ore") or 0) / 100.0
            if _yr(r.get("date")) == str(year):  rev_year  += amt
            if _ym(r.get("date")) == ym:         rev_month += amt
        for r in _card_payments_imported.values():
            amt = (r.get("amount_ore") or 0) / 100.0
            if r.get("type") == "Refusjon":
                amt = -amt
            if _yr(r.get("date")) == str(year):  rev_year  += amt
            if _ym(r.get("date")) == ym:         rev_month += amt
    except Exception:
        pass

    # ── Kunder ──
    def _cdate(c):
        return c.get("created_at") or c.get("dato") or ""
    new_cust_year  = sum(1 for c in _customers if _yr(_cdate(c)) == str(year))
    new_cust_month = sum(1 for c in _customers if _ym(_cdate(c)) == ym)

    # ── Gjenkjøp (3+ betalte ordrer, gruppert på tlf/epost) ──
    from collections import Counter
    cnt = Counter()
    for o in web_orders:
        k = (o.get("kunde") or {})
        key = (k.get("tlf") or k.get("telefon") or k.get("epost") or k.get("navn") or "").strip().lower()
        if key:
            cnt[key] += 1
    repeat3 = sum(1 for v in cnt.values() if v >= 3)

    # ── Abonnenter (aktive Stripe-abonnement) ──
    def _sub_active(s):
        st = (s.get("status") or "").lower()
        return st in ("active", "trialing") or bool(s.get("active"))
    active_subs = sum(1 for s in _subscriptions.values() if _sub_active(s))
    if active_subs == 0:
        active_subs = len(_subscriptions)   # fallback hvis status-felt mangler

    # ── Konvertering siste 30 dager (vekstmotor #1, mål 1 %) ──
    now_ms = int(time.time() * 1000)
    ms30 = now_ms - 30 * 24 * 3600 * 1000
    sessions = (_analytics.get("sessions") or {})
    sess30 = sum(1 for s in sessions.values() if (s.get("started_at") or 0) >= ms30)
    def _pdate(s):
        s = str(s or "")[:10]
        for f in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(s, f).date()
            except ValueError:
                continue
        return None
    d30 = today - timedelta(days=30)
    ord30 = 0
    for o in web_orders:
        dd = _pdate(_odate(o))
        if dd and d30 <= dd <= today:
            ord30 += 1
    conv_rate = round((ord30 / sess30) * 100.0, 2) if sess30 else 0.0

    kpi = (_strategi.get("kpi_mal") or {}).get(ym, {})
    b2b_done = int((_strategi.get("b2b_events") or {}).get(ym, 0) or 0)
    arsmal = (_strategi.get("arsmal") or {}).get("target", 1000000)
    konv_mal = float((_strategi.get("konvertering") or {}).get("target", 1.0) or 1.0)

    def _pct(cur, tgt):
        try:
            return round(min(100.0, (float(cur) / float(tgt)) * 100.0), 1) if tgt else 0.0
        except Exception:
            return 0.0

    return {
        "ok": True,
        "as_of": datetime.now().isoformat(),
        "maned": ym,
        "ar": {"target": arsmal, "current": round(rev_year, 0), "pct": _pct(rev_year, arsmal)},
        "konvertering": {"target": konv_mal, "current": conv_rate, "orders": ord30, "sessions": sess30, "pct": _pct(conv_rate, konv_mal)},
        "kpi": {
            "omsetning":    {"target": kpi.get("omsetning", 0),    "current": round(rev_month, 0)},
            "nye_kunder":   {"target": kpi.get("nye_kunder", 0),   "current": new_cust_month},
            "gjenkjop":     {"target": kpi.get("gjenkjop", 0),     "current": repeat3},
            "bedriftsevent":{"target": kpi.get("bedriftsevent", 0),"current": b2b_done},
            "abonnenter":   {"target": kpi.get("abonnenter", 0),   "current": active_subs},
        },
        "totalt": {
            "kunder": len(_customers),
            "gjenkjop3": repeat3,
            "aktive_abonnement": active_subs,
            "omsetning_ar": round(rev_year, 0),
        },
    }

@app.route("/api/strategi", methods=["GET", "PUT"])
def api_strategi():
    """GET ?scope=chat → kun kundetrygg posture (åpen, brukes av chat-ai.js).
       GET (full)      → hele planen (krever admin Bearer eller intern token).
       PUT             → oppdater planen (kun admin)."""
    with _strategi_lock:
        if not _strategi:
            _load_strategi()

    if request.method == "PUT":
        user, _ = _user_from_request()
        if not user:
            return jsonify({"ok": False, "error": "Auth påkrevd"}), 401
        data = request.get_json(silent=True) or {}
        allowed = ("prompt_full", "chat_posture", "arsmal", "konvertering",
                   "vekstmotorer", "faser", "kpi_mal", "b2b_events")
        with _strategi_lock:
            for k in allowed:
                if k in data:
                    _strategi[k] = data[k]
            _strategi["version"] = int(_strategi.get("version", 1)) + 1
            _strategi["updated_at"] = _now_iso_utc()
            _strategi["updated_by"] = (user or {}).get("email")
            _save_strategi()
        _strategi_log_append({
            "kind": "plan_edited",
            "by": (user or {}).get("email"),
            "version": _strategi.get("version"),
        })
        return jsonify({"ok": True, "version": _strategi.get("version")})

    # GET
    scope = request.args.get("scope")
    if scope == "chat":
        resp = jsonify({
            "ok": True,
            "chat_posture": _strategi.get("chat_posture") or "",
            "updated_at": _strategi.get("updated_at"),
        })
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        return resp

    user, _ = _user_from_request()
    if not user and not _strategi_internal_ok():
        return jsonify({"ok": False, "error": "Auth påkrevd"}), 401
    return jsonify({"ok": True, "strategi": _strategi})

@app.route("/api/strategi/logg", methods=["POST"])
def api_strategi_logg():
    """Registrer en AI-handling for etterlevelses-sporing.
       Body: { surface, action, vekstmotor, fase, aligned, score, summary }.
       Aksepterer intern token (Vercel) eller admin Bearer."""
    user, _ = _user_from_request()
    if not user and not _strategi_internal_ok():
        # Hvis ingen intern token er konfigurert i det hele tatt, tillat (dev/oppstart)
        if os.environ.get("STRATEGI_INTERNAL_TOKEN"):
            return jsonify({"ok": False, "error": "Auth påkrevd"}), 401
    data = request.get_json(silent=True) or {}
    kind = data.get("kind") if data.get("kind") in ("ai_action", "tiltak_done") else "ai_action"
    entry = {
        "kind": kind,
        "surface":   str(data.get("surface") or "ukjent")[:60],
        "action":    str(data.get("action") or "")[:160],
        "vekstmotor": str(data.get("vekstmotor") or "")[:40],
        "fase":      str(data.get("fase") or "")[:40],
        "aligned":   bool(data.get("aligned", True)),
        "score":     data.get("score"),
        "summary":   str(data.get("summary") or "")[:400],
        "by":        (user or {}).get("email") if user else "system",
    }
    _strategi_log_append(entry)
    return jsonify({"ok": True})

@app.route("/api/strategi/event", methods=["POST"])
def api_strategi_event():
    """Admin: tell bedriftsevent opp/ned for en måned (ingen automatisk datakilde)."""
    user, _ = _user_from_request()
    if not user:
        return jsonify({"ok": False, "error": "Auth påkrevd"}), 401
    data = request.get_json(silent=True) or {}
    try:
        delta = int(data.get("delta", 1))
    except (TypeError, ValueError):
        delta = 1
    ym = data.get("month") or datetime.now().strftime("%Y-%m")
    with _strategi_lock:
        if not _strategi:
            _load_strategi()
        ev = _strategi.setdefault("b2b_events", {})
        cur = max(0, int(ev.get(ym, 0) or 0) + delta)
        ev[ym] = cur
        _strategi["updated_at"] = _now_iso_utc()
        _save_strategi()
    _strategi_log_append({"kind": "b2b_event", "by": (user or {}).get("email"),
                          "month": ym, "delta": delta, "total": cur})
    return jsonify({"ok": True, "month": ym, "total": cur})

@app.route("/api/strategi/etterlevelse")
def api_strategi_etterlevelse():
    """Admin: aggregert etterlevelse fra loggen. ?days=N (default 30)."""
    user, _ = _user_from_request()
    if not user:
        return jsonify({"ok": False, "error": "Auth påkrevd"}), 401
    try:
        days = max(1, min(365, int(request.args.get("days", "30") or 30)))
    except (TypeError, ValueError):
        days = 30
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    rows = []
    try:
        if os.path.exists(STRATEGI_LOG_FILE):
            with open(STRATEGI_LOG_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        r = json.loads(line)
                    except Exception:
                        continue
                    if r.get("kind") in ("ai_action", "tiltak_done") and (r.get("ts") or "") >= cutoff:
                        rows.append(r)
    except Exception as e:
        print(f"[STRATEGI] etterlevelse les feilet: {e}")
    ai_rows   = [r for r in rows if r.get("kind") == "ai_action"]
    done_rows = [r for r in rows if r.get("kind") == "tiltak_done"]
    total = len(ai_rows)
    aligned = sum(1 for r in ai_rows if r.get("aligned"))
    by_engine = {}
    by_surface = {}
    for r in ai_rows:
        e = r.get("vekstmotor") or "—"
        s = r.get("surface") or "—"
        by_engine[e] = by_engine.get(e, 0) + 1
        by_surface[s] = by_surface.get(s, 0) + 1
    recent = list(reversed(rows[-40:]))
    return jsonify({
        "ok": True,
        "days": days,
        "total": total,
        "aligned": aligned,
        "aligned_pct": round((aligned / total) * 100.0, 1) if total else 0.0,
        "gjennomfort": len(done_rows),
        "by_engine": by_engine,
        "by_surface": by_surface,
        "recent": recent,
    })

@app.route("/api/strategi/fremdrift")
def api_strategi_fremdrift():
    """Admin: fremdrift mot mål, beregnet fra ekte data."""
    user, _ = _user_from_request()
    if not user:
        return jsonify({"ok": False, "error": "Auth påkrevd"}), 401
    try:
        return jsonify(_strategi_progress())
    except Exception as e:
        import traceback as _tb
        print(f"[STRATEGI] fremdrift feilet: {e}\n{_tb.format_exc()}")
        return jsonify({"ok": False, "error": str(e)}), 200


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

try:
    _load_strategi()
except Exception as _e:
    print(f"[BOOT-WSGI] _load_strategi feilet: {_e}")

_NORSK_DAGER = ("søndag","mandag","tirsdag","onsdag","torsdag","fredag","lørdag")
_NORSK_MND   = ("januar","februar","mars","april","mai","juni","juli","august",
                "september","oktober","november","desember")


def _fmt_levdag_for_eta(iso_str: str) -> tuple[str, str]:
    """Tar en ISO-dato ('2026-05-19') og returnerer (full, kort).
       full  → 'tirsdag 19. mai'
       kort  → 'i dag' / 'i morgen' / 'tirsdag 19. mai'
       Begge tomme strenger hvis input ikke er en gyldig dato.
    """
    if not iso_str:
        return "", ""
    try:
        d = datetime.strptime(iso_str.strip()[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return "", ""
    today = date.today()
    full = f"{_NORSK_DAGER[(d.weekday() + 1) % 7]} {d.day}. {_NORSK_MND[d.month - 1]}"
    diff = (d - today).days
    if diff == 0: kort = "i dag"
    elif diff == 1: kort = "i morgen"
    else: kort = full
    return full, kort


def _send_route_eta_notification(order: dict, eta_clock: str, tracking_url: str,
                                 ignore_enabled: bool = False) -> tuple[bool, str]:
    """Send leveringstids-varsling til en kunde basert på route_eta-mal.

    Sender KUN e-post (Resend, med SMTP-fallback). SMS-kanal er fjernet for
    denne varslingstypen — admin trykker «Send melding til kunder» på Rute-
    siden og forventer at det «bare skal funke» som mail.

    ignore_enabled=True hopper over enabled-sjekken (brukes av test-endepunktet).
    """
    if not isinstance(order, dict):
        return False, "no-order"
    cfg = (_customer_notify_config or {}).get("route_eta") or {}
    if not ignore_enabled and not cfg.get("enabled", True):
        return False, "disabled-by-config"

    kunde = order.get("kunde") or {}
    nr    = order.get("ordrenr") or order.get("id") or "?"
    navn  = (kunde.get("navn") or kunde.get("name") or "").strip()
    epost = (kunde.get("epost") or kunde.get("email") or "").strip()

    if not epost:
        return False, "no-email"

    levdag_iso = (kunde.get("leveringsdag") or order.get("delivery") or "").strip()
    leveringsdato_full, leveringsdato_kort = _fmt_levdag_for_eta(levdag_iso)

    tmpl_vars = {
        "navn": navn or "kunde",
        "ordrenr": nr,
        "eta_clock": eta_clock or "—",
        "tracking_url": tracking_url or "",
        "kontolenke": f"{PUBLIC_SITE_URL}/konto",
        "leveringsdag": levdag_iso,
        "leveringsdato": leveringsdato_full,
        "leveringsdato_kort": leveringsdato_kort,
    }
    subject = "Din leveringstid — Havøyet"
    body_template = cfg.get("body") or (
        "Hei {navn},\n\nVi leverer bestillingen din #{ordrenr} {leveringsdato_kort} "
        "ca. kl. {eta_clock}.\n\nPasser ikke tidspunktet? Send oss en melding eller "
        "svar på denne e-posten, så finner vi en tid som passer bedre for deg.\n\n"
        "Følg live: {tracking_url}\n\n— Havøyet"
    )
    body = _kv_render(body_template, **tmpl_vars)

    if not (RESEND_API_KEY or (SMTP_USER and SMTP_PASS)):
        return False, "no-mail-config"

    # Låst MØRK HTML-mail (mørk bakgrunn + helt hvit tekst, alle moduser). Robust mot
    # Apple Mail: (1) bakgrunnsfargene er 1x1 background-image (Apple recolorer ikke
    # bilde-bakgrunner → kortet forblir ekte mørkt), (2) usynlige zero-width-tegn etter
    # tall hindrer auto-lenking av dato/tid (ellers blir de blå med ulik farge).
    def _esc(s):
        return str(s if s is not None else "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    def _nodetect(s):
        out = []
        for ch in str(s if s is not None else ""):
            out.append(ch)
            if ch.isdigit():
                out.append("​")
        return "".join(out)
    _konto = tmpl_vars["kontolenke"]
    _PAGE = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGPgkhAHAABpADotZXrpAAAAAElFTkSuQmCC"
    _CARD = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGMQl5UGAACeAFD/7Uz0AAAAAElFTkSuQmCC"
    _CHIP = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGPgFxECAABrADbKNvMVAAAAAElFTkSuQmCC"
    _head = (
        "<!doctype html><html><head><meta charset=\"utf-8\">"
        "<meta name=\"color-scheme\" content=\"dark\">"
        "<meta name=\"supported-color-schemes\" content=\"dark\">"
        "<meta name=\"format-detection\" content=\"telephone=no,date=no,address=no,email=no\">"
        "<style>a[x-apple-data-detectors]{color:inherit !important;text-decoration:none !important;font-weight:inherit !important;}</style></head>"
    )
    html_body = (
        _head +
        f'<body style="margin:0;background:#0A1817;background-image:url({_PAGE})">'
        '<div style="padding:24px 12px;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif">'
        f'<div style="max-width:520px;margin:0 auto;background:#171D1B;background-image:url({_CARD});border:1px solid #2C3632;border-radius:14px;overflow:hidden">'
        '<div style="background:#0E3A38;padding:18px 28px"><span style="color:#FFFFFF;font-weight:700;font-size:18px">Havøyet</span></div>'
        '<div style="padding:28px 28px 10px">'
        f'<p style="font-size:15px;color:#FFFFFF;margin:0 0 16px">Hei {_esc(navn or "kunde")},</p>'
        '<p style="font-size:15px;color:#FFFFFF;margin:0 0 18px">Leveringen din er satt opp:</p>'
        f'<div style="background:#0F1412;background-image:url({_CHIP});border-radius:12px;padding:18px;text-align:center;margin:0 0 20px">'
        f'<div style="font-size:12px;color:#FFFFFF;text-transform:uppercase;letter-spacing:0.08em;font-weight:600">Bestilling #{_esc(nr)}</div>'
        f'<div style="font-size:20px;font-weight:700;color:#FFFFFF;margin-top:8px">{_nodetect(_esc(tmpl_vars["leveringsdato"]))}</div>'
        f'<div style="font-size:30px;font-weight:800;color:#41C1BA;margin-top:2px">ca. kl. {_nodetect(_esc(eta_clock or "—"))}</div>'
        '</div>'
        '<p style="font-size:14px;line-height:1.6;color:#FFFFFF;margin:0 0 18px">Tidspunktet er et estimat — vi kan komme litt før eller senere avhengig av trafikken og rekkefølgen på dagens stopp. Skulle vi måtte omrokere ruten mye, gir vi deg beskjed så fort vi vet nytt tidspunkt.</p>'
        '<div style="background:#0F1412;background-image:url(' + _CHIP + ');border:1px solid #41C1BA;border-radius:12px;padding:16px 18px;margin:0 0 20px">'
        '<p style="font-size:14px;line-height:1.6;color:#41C1BA;font-weight:700;margin:0">Passer ikke tidspunktet?</p>'
        '<p style="font-size:14px;line-height:1.6;color:#FFFFFF;margin:6px 0 0">Send oss en melding eller svar på denne e-posten, så finner vi en tid som passer bedre for deg.</p>'
        '</div>'
        f'<div style="text-align:center;margin:0 0 20px"><a href="{_esc(_konto)}" style="display:inline-block;background:#41C1BA;color:#0A1817;font-weight:700;font-size:15px;padding:14px 30px;border-radius:10px;text-decoration:none">Følg leveringen live &rarr;</a></div>'
        '<p style="font-size:13px;line-height:1.6;color:#FFFFFF;margin:0">Minuttene oppdateres fra bilens posisjon så snart sjåføren er ute på ruten. Har du spørsmål, svar gjerne på denne e-posten.</p>'
        '</div></div></div></body></html>'
    )

    mail_ok = False
    mail_detail = "skipped"
    if RESEND_API_KEY:
        mail_ok, mail_detail = _send_via_resend(
            CONTACT_TO, "Havøyet", subject, body, to_email=epost, reply_to=CONTACT_TO,
            html_body=html_body,
        )
    if not mail_ok and SMTP_USER and SMTP_PASS:
        mail_ok, mail_detail = _send_via_smtp(
            CONTACT_TO, "Havøyet", subject, body, to_email=epost, reply_to=CONTACT_TO,
        )

    return mail_ok, f"mail={mail_detail}"


@app.route("/api/admin/route/eta-test", methods=["GET", "POST"])
def admin_route_eta_test():
    """Sender en test av leveringstids-e-posten til en valgt adresse (default
    CONTACT_TO). Bygger en dummy-ordre — sender INGEN ekte kunde noe. Brukes for
    å se hvordan mailen ser ut. Params (query eller JSON): to, navn, eta, dato."""
    if not _is_admin_request():
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    to   = (request.args.get("to")   or data.get("to")   or CONTACT_TO).strip()
    navn = (request.args.get("navn") or data.get("navn") or "Test Testesen").strip()
    eta  = (request.args.get("eta")  or data.get("eta")  or "15:00").strip()
    dato = (request.args.get("dato") or data.get("dato") or datetime.now().strftime("%Y-%m-%d")).strip()
    dummy = {
        "ordrenr": "TEST123",
        "kunde": {"navn": navn, "epost": to, "leveringsdag": dato},
    }
    ok, detail = _send_route_eta_notification(
        dummy, eta, f"{PUBLIC_SITE_URL}/konto", ignore_enabled=True,
    )
    return jsonify({"ok": bool(ok), "detail": detail, "to": to})


# ── ABAX ETA-integrasjon (kunder ser "X minutter til levering") ──────────
def _driver_set_order_status(order_id, new_status):
    """Kalles fra sjåfør-appen (tracking_routes) når et stopp markeres som levert
    eller leveringen angres. Speiler logikken i `/api/manual-orders/<id>/status`
    så admin-siden og kunde-min-side viser samme tilstand som om admin satte den
    selv — inkludert e-post/SMS/Telegram-varsler.
    """
    global _manual_orders
    for o in _manual_orders:
        if str(o.get("ordrenr") or o.get("id")) != str(order_id):
            continue
        old_status = o.get("status", "")
        if old_status == new_status:
            return  # ingen endring, ikke spam varsler
        o["status"] = new_status
        _save_sync_state()
        nr = o.get("ordrenr") or o.get("id") or "?"
        change_summary = (
            f"Status endret fra '{old_status}' til '{new_status}' "
            f"(markert av sjåfør i rute-appen)."
        )
        is_delivered = (
            str(new_status).upper() in ("DONE", "LEVERT")
            or "lever" in str(new_status).lower()
        )
        try:
            if is_delivered:
                _notify_admins(
                    "order_delivered",
                    f"[Havøyet] Bestilling #{nr} er levert",
                    change_summary + "\n\n" + _format_order_lines(o),
                    html_body=_format_order_email_html(o, change_summary, "order_delivered"),
                )
                _notify_customer_order_update(o, "order_delivered", change_summary)
            else:
                _notify_admins(
                    "order_updated",
                    f"[Havøyet] Bestilling #{nr} oppdatert",
                    change_summary + "\n\n" + _format_order_lines(o),
                    html_body=_format_order_email_html(o, change_summary, "order_updated"),
                )
                _notify_customer_order_update(o, "order_updated", change_summary)
        except Exception as e:
            print(f"[driver status] notify failed for #{nr}: {e}")
        return
    print(f"[driver status] ordre #{order_id} ikke funnet i manual_orders")


try:
    from tracking_routes import register_tracking
    register_tracking(
        app,
        manual_orders_ref=lambda: _manual_orders,
        save_state=_save_sync_state,
        state_dir=STATE_DIR,
        admin_check=_user_from_request,
        sms_sender=_send_admin_sms,
        # «Send tidspunkt til kunden» på rute-siden skal ALLTID sende mailen når den
        # trykkes — derfor ignore_enabled=True (uavhengig av route_eta-bryteren).
        route_eta_sender=(lambda o, e, u: _send_route_eta_notification(o, e, u, ignore_enabled=True)),
        tracking_base_url=os.environ.get("TRACKING_PUBLIC_URL", "https://bestilling.havoyet.no"),
        status_hook=_driver_set_order_status,
        admin_notifier=_notify_admins,
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
import struct as _struct
import subprocess as _sp
import tempfile as _tempfile
import uuid as _uuid
import zlib as _zlib
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

# Etikettene er designet 50 × 103 mm (591 × 1217 px @ 300 dpi). For at CUPS på
# Pi-en (driverless «everywhere»-kø) skal printe i NØYAKTIG fysisk størrelse —
# og aldri en lang strimmel — må PNG-en bære dpi-info (pHYs-chunk), noe
# html2canvas ikke setter.
LABEL_DPI          = 300
LABEL_ASPECT       = 103.0 / 50.0   # høyde/bredde = 2.06
LABEL_ASPECT_SLACK = 0.55           # godta 1.51–2.61 (avrundingsslakk)

def _png_dimensions(raw):
    """Les bredde/høyde fra IHDR. Returner (w, h) eller (None, None)."""
    try:
        if raw[:8] == b"\x89PNG\r\n\x1a\n" and raw[12:16] == b"IHDR":
            return _struct.unpack(">II", raw[16:24])
    except Exception:
        pass
    return None, None

def _png_set_dpi(raw, dpi=LABEL_DPI):
    """Sett/erstatt pHYs-chunk (piksler per meter) så fysisk størrelse er
    entydig for CUPS. Returnerer original uendret ved enhver feil."""
    try:
        if raw[:8] != b"\x89PNG\r\n\x1a\n":
            return raw
        ppm  = int(round(dpi / 0.0254))
        body = _struct.pack(">II", ppm, ppm) + b"\x01"
        phys = (_struct.pack(">I", len(body)) + b"pHYs" + body +
                _struct.pack(">I", _zlib.crc32(b"pHYs" + body) & 0xFFFFFFFF))
        out, pos, inserted = bytearray(raw[:8]), 8, False
        while pos + 12 <= len(raw):
            length = _struct.unpack(">I", raw[pos:pos+4])[0]
            ctype  = raw[pos+4:pos+8]
            end    = pos + 12 + length
            if ctype == b"pHYs":          # dropp eksisterende
                pos = end
                continue
            if not inserted and ctype != b"IHDR":   # rett etter IHDR
                out += phys
                inserted = True
            out += raw[pos:end]
            pos = end
        return bytes(out) if inserted else raw
    except Exception:
        return raw

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

    # Vern mot «lang strimmel»: etiketten skal være ~50×103 mm (h/b ≈ 2.06).
    # Alt med helt andre proporsjoner (f.eks. en hel side fanget i ett bilde)
    # avvises før det når skriveren.
    w, h = _png_dimensions(raw)
    if w and h:
        ratio = h / w
        if abs(ratio - LABEL_ASPECT) > LABEL_ASPECT_SLACK:
            return jsonify({
                "ok": False,
                "error": (f"Feil etikett-proporsjoner ({w}×{h} px, h/b={ratio:.2f}). "
                          "Forventet ~50×103 mm — bruk «Skriv ut denne»-knappen per etikett."),
            }), 400

    # Stemple 300 dpi inn i PNG-en → CUPS printer nøyaktig 50×103 mm
    raw = _png_set_dpi(raw)

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
        "message": ("Skriver ut nå …" if worker_active
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
    _load_strategi()

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
