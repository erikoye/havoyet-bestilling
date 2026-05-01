"""Importer Shopify-kundeeksport til LIVE Flask-backend på Render.

I motsetning til import_customers.py (som skriver til lokal /tmp) POSTer
denne hver kunde til https://havoyet-bestilling.onrender.com/api/customers,
slik at de havner i Render sin persistent disk og blir synlige i admin
umiddelbart — ingen restart nødvendig.

Idempotent: server-siden returnerer 409 hvis kunden allerede finnes
(dedup på navn+telefon).

Bruk:
    python3 import_customers_remote.py [csv-fil] [--base URL]

Default-CSV: /Users/eriko/Downloads/customers_export.csv
Default-base: https://havoyet-bestilling.onrender.com
"""
import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error

DEFAULT_BASE = "https://havoyet-bestilling.onrender.com"
DEFAULT_CSV  = "/Users/eriko/Downloads/customers_export.csv"


def normalize_phone(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip().lstrip("'")
    digits = "".join(c for c in raw if c.isdigit() or c == "+")
    if digits.startswith("+") and len(digits) >= 9:
        return digits
    if digits.startswith("00") and len(digits) >= 10:
        return "+" + digits[2:]
    if len(digits) == 8:
        return "+47" + digits
    if len(digits) == 10 and digits.startswith("47"):
        return "+" + digits
    return raw


def build_address(row):
    parts = []
    a1 = (row.get("Default Address Address1") or "").strip()
    a2 = (row.get("Default Address Address2") or "").strip()
    if a1: parts.append(a1)
    if a2: parts.append(a2)
    zipc = (row.get("Default Address Zip") or "").strip()
    city = (row.get("Default Address City") or "").strip()
    line2 = " ".join(p for p in [zipc, city] if p)
    if line2: parts.append(line2)
    return ", ".join(parts)


def build_kommentar(row):
    bits = []
    spent = (row.get("Total Spent") or "").strip()
    orders = (row.get("Total Orders") or "").strip()
    try:
        if orders and int(float(orders)) > 0:
            bits.append(f"{int(float(orders))} ordre")
    except ValueError:
        pass
    try:
        if spent and float(spent) > 0:
            bits.append(f"{int(float(spent))} kr totalt")
    except ValueError:
        pass
    tags = (row.get("Tags") or "").strip()
    if tags:
        bits.append(f"tags: {tags}")
    note = (row.get("Note") or "").strip()
    if note:
        bits.append(note)
    src = "Shopify-import"
    if bits:
        return f"[{src}] " + " · ".join(bits)
    return f"[{src}]"


def post_customer(base, payload):
    req = urllib.request.Request(
        f"{base}/api/customers",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode("utf-8"))
        except Exception:
            body = {"error": str(e)}
        return e.code, body
    except Exception as e:
        return 0, {"error": str(e)}


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    base = DEFAULT_BASE
    for i, a in enumerate(sys.argv):
        if a == "--base" and i + 1 < len(sys.argv):
            base = sys.argv[i + 1].rstrip("/")
    csv_path = args[0] if args else DEFAULT_CSV

    if not os.path.exists(csv_path):
        print(f"FEIL: finner ikke {csv_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Importerer fra {csv_path} → {base}")
    print()

    added, dup, empty, errors = 0, 0, 0, 0
    rows_total = 0

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        rows_total = len(rows)

        for idx, row in enumerate(rows, 1):
            fn = (row.get("First Name") or "").strip()
            ln = (row.get("Last Name") or "").strip()
            navn = f"{fn} {ln}".strip()
            if not navn:
                empty += 1
                continue
            ep = (row.get("Email") or "").strip().lower()
            phone_raw = row.get("Phone") or row.get("Default Address Phone") or ""
            tlf = normalize_phone(phone_raw)
            payload = {
                "navn":       navn,
                "tlf":        tlf,
                "epost":      ep,
                "adresse":    build_address(row),
                "kommentar":  build_kommentar(row),
                "total_spent": float(row.get("Total Spent") or 0),
                "total_orders": int(float(row.get("Total Orders") or 0)),
                "shopify_id": (row.get("Customer ID") or "").lstrip("'"),
            }
            status, body = post_customer(base, payload)
            if status == 200:
                added += 1
                tag = "+"
            elif status == 409:
                dup += 1
                tag = "·"
            else:
                errors += 1
                tag = "!"
            print(f"  [{idx:>3}/{rows_total}] {tag} {navn:<40} {body.get('error','OK')[:60]}")
            time.sleep(0.05)  # vær snill med Render

    print()
    print(f"Total rader i CSV:      {rows_total}")
    print(f"  Lagt til:             {added}")
    print(f"  Duplikat (eksisterer): {dup}")
    print(f"  Mangler navn:         {empty}")
    print(f"  Feil:                 {errors}")


if __name__ == "__main__":
    main()
