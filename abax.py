"""ABAX API-klient (OAuth2) — for å hente live varebil-posisjon.

ABAX bruker OIDC/OAuth2 (IdentityServer). Discovery:
    https://identity.abax.cloud/.well-known/openid-configuration
    - authorize:  https://identity.abax.cloud/connect/authorize
    - token:      https://identity.abax.cloud/connect/token

Scopes som er relevante for kjørebok/posisjon:
    openid offline_access
    open_api open_api_vehicles open_api_trips open_api_streams

Krever fra ABAX:
  - ABAX_CLIENT_ID
  - ABAX_CLIENT_SECRET
  - ABAX_REDIRECT_URI       (admin sin OAuth-callback-URL)
  - (valgfritt) ABAX_SCOPES, ABAX_AUTH_URL, ABAX_TOKEN_URL, ABAX_API_BASE
"""
from __future__ import annotations

import json
import os
import secrets
import threading
import time
from pathlib import Path
from typing import Any, Optional
import requests
from urllib.parse import urlencode

DEFAULT_AUTH_URL = "https://identity.abax.cloud/connect/authorize"
DEFAULT_TOKEN_URL = "https://identity.abax.cloud/connect/token"
DEFAULT_API_BASE = "https://api.abax.cloud/v1"
DEFAULT_SCOPES = (
    "openid offline_access "
    "open_api open_api_vehicles open_api_trips open_api_streams"
)

_TOKEN_FILE = "abax_tokens.json"
_REFRESH_LOCK = threading.Lock()
_GUARD_SEC = 60


class AbaxError(Exception):
    pass


class AbaxNotConnected(AbaxError):
    """Ingen tokens lagret — admin må gjennomføre OAuth-flyt."""


class AbaxClient:
    """Trådsikker ABAX-klient. Cache-er tokens på persistent disk."""

    def __init__(self, state_dir: str | Path):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._token_path = self.state_dir / _TOKEN_FILE

        self.client_id = os.environ.get("ABAX_CLIENT_ID", "")
        self.client_secret = os.environ.get("ABAX_CLIENT_SECRET", "")
        self.auth_url = os.environ.get("ABAX_AUTH_URL", DEFAULT_AUTH_URL)
        self.token_url = os.environ.get("ABAX_TOKEN_URL", DEFAULT_TOKEN_URL)
        self.api_base = os.environ.get("ABAX_API_BASE", DEFAULT_API_BASE).rstrip("/")
        self.redirect_uri = os.environ.get("ABAX_REDIRECT_URI", "")
        self.scopes = os.environ.get("ABAX_SCOPES", DEFAULT_SCOPES)

    # ── Konfig-status ──────────────────────────────────────────────────────
    def is_configured(self) -> bool:
        return bool(self.client_id and self.client_secret and self.redirect_uri)

    def is_connected(self) -> bool:
        return self._token_path.exists()

    def status(self) -> dict:
        st = {
            "vendor": "ABAX",
            "configured": self.is_configured(),
            "connected": self.is_connected(),
            "missing_env": [],
        }
        for key in ("ABAX_CLIENT_ID", "ABAX_CLIENT_SECRET", "ABAX_REDIRECT_URI"):
            if not os.environ.get(key):
                st["missing_env"].append(key)
        if self.is_connected():
            tok = self._load_tokens()
            st["expires_at"] = tok.get("expires_at")
            st["scope"] = tok.get("scope")
        return st

    # ── OAuth2 authorization-code-flyt ────────────────────────────────────
    def build_authorize_url(self, state: str | None = None) -> tuple[str, str]:
        if not self.is_configured():
            raise AbaxError("Mangler ABAX_CLIENT_ID / SECRET / REDIRECT_URI i env-vars.")
        state = state or secrets.token_urlsafe(24)
        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "scope": self.scopes,
            "state": state,
        }
        return f"{self.auth_url}?{urlencode(params)}", state

    def exchange_code(self, code: str) -> dict:
        resp = requests.post(
            self.token_url,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self.redirect_uri,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=15,
        )
        if not resp.ok:
            raise AbaxError(f"Token-exchange feilet ({resp.status_code}): {resp.text[:200]}")
        tokens = resp.json()
        self._save_tokens(tokens)
        return tokens

    # ── Token-håndtering ───────────────────────────────────────────────────
    def _load_tokens(self) -> dict:
        if not self._token_path.exists():
            raise AbaxNotConnected("Ingen tokens lagret — kjør OAuth-flyt fra admin.")
        return json.loads(self._token_path.read_text())

    def _save_tokens(self, tokens: dict) -> None:
        if "expires_in" in tokens and "expires_at" not in tokens:
            tokens["expires_at"] = int(time.time() + int(tokens["expires_in"]))
        if self._token_path.exists():
            try:
                old = json.loads(self._token_path.read_text())
                tokens.setdefault("refresh_token", old.get("refresh_token"))
            except Exception:
                pass
        self._token_path.write_text(json.dumps(tokens, indent=2))
        try:
            os.chmod(self._token_path, 0o600)
        except OSError:
            pass

    def _refresh_if_needed(self) -> str:
        with _REFRESH_LOCK:
            tokens = self._load_tokens()
            now = int(time.time())
            expires_at = int(tokens.get("expires_at") or 0)

            if tokens.get("access_token") and now < expires_at - _GUARD_SEC:
                return tokens["access_token"]

            refresh = tokens.get("refresh_token")
            if not refresh:
                raise AbaxNotConnected("Refresh-token mangler — kjør OAuth-flyt på nytt.")

            resp = requests.post(
                self.token_url,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                timeout=15,
            )
            if not resp.ok:
                raise AbaxError(f"Refresh feilet ({resp.status_code}): {resp.text[:200]}")
            new_tokens = resp.json()
            self._save_tokens(new_tokens)
            return new_tokens["access_token"]

    def disconnect(self) -> None:
        if self._token_path.exists():
            self._token_path.unlink()

    # ── API-kall ──────────────────────────────────────────────────────────
    def _request(self, method: str, path: str, **kw) -> Any:
        token = self._refresh_if_needed()
        url = f"{self.api_base}/{path.lstrip('/')}"
        headers = kw.pop("headers", {}) or {}
        headers["Authorization"] = f"Bearer {token}"
        headers.setdefault("Accept", "application/json")
        resp = requests.request(method, url, headers=headers, timeout=10, **kw)

        if resp.status_code == 401:
            tokens = self._load_tokens()
            tokens["expires_at"] = 0
            self._save_tokens(tokens)
            token = self._refresh_if_needed()
            headers["Authorization"] = f"Bearer {token}"
            resp = requests.request(method, url, headers=headers, timeout=10, **kw)

        if not resp.ok:
            raise AbaxError(f"{method} {path} → HTTP {resp.status_code}: {resp.text[:200]}")
        if not resp.content:
            return None
        return resp.json()

    # ── Høy-nivå-metoder ──────────────────────────────────────────────────
    def list_vehicles(self) -> list[dict]:
        """Returnerer flåten. ABAX svarer med {items: [...]} eller liknende."""
        data = self._request("GET", "vehicles")
        if isinstance(data, dict):
            for k in ("items", "data", "vehicles", "results"):
                if isinstance(data.get(k), list):
                    return data[k]
        if isinstance(data, list):
            return data
        return []

    def get_position(self, vehicle_id: str) -> Optional[dict]:
        """Live-posisjon for et kjøretøy via POST /v1/vehicles/locations."""
        raw = self._request(
            "POST",
            "vehicles/locations",
            json={"vehicleIds": [vehicle_id]},
        )
        item = _first_item_for_vehicle(raw, vehicle_id)
        if item is None:
            return None

        # ABAX svarer typisk med {latitude,longitude} eller nested location.
        candidates = [item]
        for k in ("location", "position", "last_position", "lastPosition", "current_position"):
            nested = item.get(k) if isinstance(item, dict) else None
            if isinstance(nested, dict):
                candidates.append(nested)

        for cand in candidates:
            lat = cand.get("latitude") or cand.get("lat")
            lon = cand.get("longitude") or cand.get("lng") or cand.get("lon")
            if lat is None or lon is None:
                loc = cand.get("location")
                if isinstance(loc, dict):
                    lat = lat or loc.get("latitude") or loc.get("lat")
                    lon = lon or loc.get("longitude") or loc.get("lng")
            if lat is not None and lon is not None:
                return {
                    "lat": float(lat),
                    "lon": float(lon),
                    "speed_kmh": _to_kmh(cand.get("speed") or cand.get("velocity")),
                    "heading": cand.get("heading") or cand.get("course"),
                    "timestamp": (
                        cand.get("timestamp")
                        or cand.get("recorded_at")
                        or cand.get("received_at")
                        or cand.get("ts")
                        or cand.get("receivedAt")
                        or cand.get("recordedAt")
                    ),
                    "raw": cand,
                }
        return None

    def get_drive_state(self, vehicle_id: str) -> Optional[str]:
        """Returnerer kjøretilstand ('driving', 'stopped', 'idling', …) via POST /v1/vehicles/drive-states."""
        try:
            raw = self._request(
                "POST",
                "vehicles/drive-states",
                json={"vehicleIds": [vehicle_id]},
            )
        except AbaxError:
            return None
        item = _first_item_for_vehicle(raw, vehicle_id)
        if not isinstance(item, dict):
            return None
        for k in ("driveState", "drive_state", "state", "status"):
            val = item.get(k)
            if isinstance(val, str):
                return val.lower()
        return None


def _to_kmh(speed: Any) -> Optional[float]:
    if speed is None:
        return None
    try:
        v = float(speed)
    except (TypeError, ValueError):
        return None
    return round(v * 3.6, 1) if v < 70 else round(v, 1)


def _first_item_for_vehicle(raw: Any, vehicle_id: str) -> Optional[dict]:
    """Pakker ut første relevante objekt fra et ABAX-batch-svar.

    ABAX wrapper typisk listene i {items: [...]} eller {data: [...]}. Når det
    finnes flere kjøretøy, plukker vi det som matcher vehicle_id (eller første
    elementet hvis ID-feltet ikke finnes).
    """
    if raw is None:
        return None
    items: list = []
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        for k in ("items", "data", "vehicles", "results", "locations", "driveStates"):
            v = raw.get(k)
            if isinstance(v, list):
                items = v
                break
        if not items and any(k in raw for k in ("latitude", "longitude", "location", "driveState", "state")):
            return raw
    if not items:
        return None
    for it in items:
        if not isinstance(it, dict):
            continue
        for key in ("vehicleId", "vehicle_id", "id"):
            if str(it.get(key)) == str(vehicle_id):
                return it
    first = items[0]
    return first if isinstance(first, dict) else None
