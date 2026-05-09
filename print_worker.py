#!/usr/bin/env python3
"""
Havøyet etikett-print worker (Raspberry Pi)

Kjører som systemd-tjeneste på Pi-en. Poller Render-API'et etter ventende
print-jobber og sender dem til den lokale Brother QL-1110NWB via CUPS (lp).

Konfig (miljøvariabler):
  PRINT_API_BASE       — f.eks. https://bestilling.havoyet.no  (default: localhost:5001)
  PRINT_WORKER_TOKEN   — Bearer-token (må matche server-siden)
  PRINTER_NAME         — CUPS-kø-navn (default: brother-ql1110)
  POLL_INTERVAL        — sekunder mellom polls (default: 3)
  HEARTBEAT_INTERVAL   — sekunder mellom heartbeats (default: 30)

Start manuelt:
  python3 print_worker.py

Logger: stdout/stderr → systemd journal
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

VERSION              = "1.0.0"
API_BASE             = os.environ.get("PRINT_API_BASE", "http://localhost:5001").rstrip("/")
TOKEN                = os.environ.get("PRINT_WORKER_TOKEN", "")
PRINTER_NAME         = os.environ.get("PRINTER_NAME", "brother-ql1110")
POLL_INTERVAL        = float(os.environ.get("POLL_INTERVAL", "3"))
HEARTBEAT_INTERVAL   = float(os.environ.get("HEARTBEAT_INTERVAL", "30"))
HOSTNAME             = socket.gethostname()


def _hdr():
    h = {"User-Agent": f"havoyet-print-worker/{VERSION}"}
    if TOKEN:
        h["Authorization"] = f"Bearer {TOKEN}"
    return h


def _http_get(path, timeout=10):
    req = urllib.request.Request(API_BASE + path, headers=_hdr())
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def _http_post(path, payload=None, timeout=10):
    body = b""
    headers = dict(_hdr())
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(API_BASE + path, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def fetch_pending(limit=5):
    try:
        status, raw = _http_get(f"/api/print/queue?limit={limit}")
        if status == 200:
            return json.loads(raw).get("jobs", [])
        log(f"queue fetch fikk {status}: {raw[:200]}")
    except urllib.error.HTTPError as e:
        log(f"queue HTTP {e.code}: {e.read()[:200]!r}")
    except Exception as e:
        log(f"queue feil: {e}")
    return []


def fetch_png(job_id):
    status, raw = _http_get(f"/api/print/queue/{job_id}/png", timeout=20)
    if status != 200:
        raise RuntimeError(f"PNG-henting feilet ({status})")
    return raw


def ack(job_id, success, error=None, attempts=1):
    try:
        _http_post(f"/api/print/queue/{job_id}/ack", {
            "success": success, "error": error, "attempts": attempts,
        })
    except Exception as e:
        log(f"ack feilet for {job_id}: {e}")


def heartbeat():
    try:
        _http_post("/api/print/worker/heartbeat", {
            "host": HOSTNAME, "printer": PRINTER_NAME, "version": VERSION,
        })
    except Exception as e:
        log(f"heartbeat feilet: {e}")


def lp_print(png_bytes, job_id):
    """Skriv PNG til midlertidig fil og kjør `lp -d PRINTER_NAME`."""
    fd, path = tempfile.mkstemp(prefix=f"havoyet_{job_id[:8]}_", suffix=".png")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(png_bytes)
        out = subprocess.run(
            ["lp", "-d", PRINTER_NAME, path],
            capture_output=True, text=True, timeout=30,
        )
        if out.returncode != 0:
            return False, (out.stderr.strip() or out.stdout.strip() or "lp ukjent feil")
        return True, out.stdout.strip()
    finally:
        try: os.remove(path)
        except Exception: pass


def log(msg):
    print(f"[print-worker] {msg}", flush=True)


def main():
    log(f"start: api={API_BASE} printer={PRINTER_NAME} host={HOSTNAME}")
    if not TOKEN:
        log("ADVARSEL: PRINT_WORKER_TOKEN ikke satt — auth deaktivert")
    last_hb = 0.0
    while True:
        now = time.time()
        if now - last_hb >= HEARTBEAT_INTERVAL:
            heartbeat()
            last_hb = now
        jobs = fetch_pending(limit=3)
        if jobs:
            for job in jobs:
                jid = job["id"]
                product = job.get("product", "—")
                log(f"jobb {jid[:8]} → {product}")
                try:
                    png = fetch_png(jid)
                    ok, info = lp_print(png, jid)
                    ack(jid, ok, error=None if ok else info)
                    log(f"  {'OK' if ok else 'FEIL'}: {info}")
                except Exception as e:
                    ack(jid, False, error=str(e))
                    log(f"  EXCEPTION: {e}")
            # Etter en batch: kort pause før neste poll
            time.sleep(0.5)
        else:
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("avsluttet av bruker")
        sys.exit(0)
    except Exception as e:
        log(f"FATAL: {e}")
        sys.exit(1)
