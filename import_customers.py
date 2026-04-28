"""Importer Shopify-kundeeksport (CSV) til Havøyet sync-state.

Leser CSV → lager kunde-poster {id, navn, tlf, epost, adresse, kommentar} →
skriver inn i /tmp/havoyet_sync_state.json under "customers".
Idempotent: dedup på e-post (case-insensitive), så det er trygt å kjøre flere
ganger. Etter import: restart Flask så endringene plukkes opp.

Bruk:
    python3 import_customers.py /Users/eriko/Downloads/customers_export.csv
"""
import csv, json, os, sys, uuid
from datetime import datetime

SYNC_STATE_FILE = os.path.join("/tmp", "havoyet_sync_state.json")


def normalize_phone(raw: str) -> str:
    """Strip Excel-apostrof og whitespace; returner +47XXXXXXXX hvis mulig."""
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
    return raw  # behold original hvis vi ikke klarer å normalisere


def build_address(row: dict) -> str:
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


def build_kommentar(row: dict) -> str:
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


def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "/Users/eriko/Downloads/customers_export.csv"
    if not os.path.exists(csv_path):
        print(f"FEIL: finner ikke {csv_path}", file=sys.stderr)
        sys.exit(1)

    # Last eksisterende sync-state
    state = {}
    if os.path.exists(SYNC_STATE_FILE):
        try:
            with open(SYNC_STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
        except Exception as e:
            print(f"ADVARSEL: klarte ikke lese {SYNC_STATE_FILE}: {e}")
    customers = list(state.get("customers", []))
    existing_emails = {(c.get("epost") or "").lower() for c in customers if c.get("epost")}

    # Les CSV
    added, skipped_dup, skipped_empty = 0, 0, 0
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            fn = (row.get("First Name") or "").strip()
            ln = (row.get("Last Name") or "").strip()
            navn = f"{fn} {ln}".strip()
            epost = (row.get("Email") or "").strip().lower()
            phone_raw = row.get("Phone") or row.get("Default Address Phone") or ""
            tlf = normalize_phone(phone_raw)
            if not navn:
                skipped_empty += 1
                continue
            if epost and epost in existing_emails:
                skipped_dup += 1
                continue
            adresse = build_address(row)
            kommentar = build_kommentar(row)
            customers.append({
                "id": str(uuid.uuid4()),
                "navn": navn,
                "tlf": tlf,
                "epost": epost,
                "adresse": adresse,
                "kommentar": kommentar,
                "created_at": datetime.now().isoformat(),
                "shopify_id": (row.get("Customer ID") or "").lstrip("'"),
            })
            if epost:
                existing_emails.add(epost)
            added += 1

    # Skriv tilbake
    state["customers"] = customers
    with open(SYNC_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)

    print(f"Importert: {added} nye kunder")
    print(f"  hoppet over (duplikat e-post): {skipped_dup}")
    print(f"  hoppet over (mangler navn):    {skipped_empty}")
    print(f"  totalt nå i sync-state:        {len(customers)}")
    print()
    print(f"Skrevet til {SYNC_STATE_FILE}.")
    print("Restart Flask-backenden så endringene plukkes opp i admin-UIet.")


if __name__ == "__main__":
    main()
