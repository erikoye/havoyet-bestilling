#!/usr/bin/env python3
"""Engangsscript: opprett synthetisk test-abonnement + send kvittering-mail.
Spør om admin-passord interaktivt, logger inn mot
https://havoyet-bestilling.onrender.com, og kaller
/api/subscription/admin-test-create.

Sub-en flagges som test internt (id med prefiks `test_`), men mailen og
admin-listingen ser ut som en faktisk bestilling."""
import getpass
import json
import sys
import urllib.request
import urllib.error

BASE  = "https://havoyet-bestilling.onrender.com"
EMAIL = "erik@havoyet.no"


def _post(path, body, token=None):
    data = json.dumps(body).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode("utf-8"))
        except Exception:
            body = {"error": str(e)}
        return e.code, body


def main():
    print(f"Logger inn som {EMAIL} på {BASE} ...")
    pw = getpass.getpass("Admin-passord: ")
    status, login = _post("/api/auth/login", {"email": EMAIL, "password": pw})
    if status != 200 or not login.get("ok"):
        print(f"Login feilet ({status}): {login}")
        sys.exit(1)
    token = login.get("token")
    if not token:
        print(f"Login OK, men ingen token i svaret: {login}")
        sys.exit(1)
    print("Logget inn. Oppretter test-sub + sender kvitterings-mail ...")

    body = {
        "email": EMAIL,
        "navn":  "Erik Øye",
        "amount": 149000,
        "send_mail": True,
        "hide_test_markers": True,
        "kasse": {
            "name": "Sjømatkasse — 2 personer",
            "size": "2pers",
            "meta": {"voksne": 2, "barn": 0, "leverdag": "Torsdag"},
        },
        "description": "Sjømatkasse — månedlig abonnement",
    }
    status, res = _post("/api/subscription/admin-test-create", body, token=token)
    print(f"\nStatus: {status}")
    print(json.dumps(res, indent=2, ensure_ascii=False))
    if status == 200 and res.get("ok"):
        print("\n✓ Sub opprettet:", res.get("subscription_id"))
        print("✓ Mail-status: ", res.get("mail"))
        print("\nSjekk:")
        print("  1) erik@havoyet.no inbox for kvitteringen")
        print("  2) admin.havoyet.no → Abonnementer (trykk Oppdater)")


if __name__ == "__main__":
    main()
