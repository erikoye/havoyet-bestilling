"""ETA-beregning: kunde-adresse → koordinater → minutter til levering.

- Geocoding via Nominatim (gratis OSM, in-memory cache)
- Ruteberegning via OSRM (gratis demo-server)
- Begge har fallback til haversine + 50 km/t hvis nett feiler
"""
from __future__ import annotations

import time
from typing import Optional, Tuple
import math
import requests

_USER_AGENT = "HavoyetBestilling/1.0 (kontakt: erik@havoyet.no)"
_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
_OSRM_URL = "https://router.project-osrm.org/route/v1/driving"
_OSRM_TRIP_URL = "https://router.project-osrm.org/trip/v1/driving"
_OSRM_TABLE_URL = "https://router.project-osrm.org/table/v1/driving"

# Cache: address_string -> (timestamp, (lat, lon))
_GEOCODE_CACHE: dict[str, tuple[float, tuple[float, float] | None]] = {}
_GEOCODE_TTL = 60 * 60 * 24 * 90  # 90 dager — adresser flytter sjelden


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def geocode(address: str, postnr: str | None = None, poststed: str | None = None) -> Optional[Tuple[float, float]]:
    """Slå opp adresse → (lat, lon). Returnerer None ved feil. Cachet."""
    if not address:
        return None
    parts = [p for p in (address.strip(), postnr, poststed) if p]
    parts.append("Norge")
    query = ", ".join(parts)

    now = time.time()
    cached = _GEOCODE_CACHE.get(query)
    if cached and now - cached[0] < _GEOCODE_TTL:
        return cached[1]

    try:
        resp = requests.get(
            _NOMINATIM_URL,
            params={
                "q": query,
                "format": "json",
                "limit": 1,
                "countrycodes": "no",
                "accept-language": "no",
            },
            headers={"User-Agent": _USER_AGENT},
            timeout=5,
        )
        if not resp.ok:
            return None
        results = resp.json()
        if not results:
            _GEOCODE_CACHE[query] = (now, None)
            return None
        first = results[0]
        coords = (float(first["lat"]), float(first["lon"]))
        _GEOCODE_CACHE[query] = (now, coords)
        return coords
    except (requests.RequestException, ValueError, KeyError):
        return None


def osrm_route(from_lat: float, from_lon: float, to_lat: float, to_lon: float
               ) -> Optional[dict]:
    """Hent rute fra OSRM. Returnerer {distance_km, duration_min} eller None."""
    try:
        resp = requests.get(
            f"{_OSRM_URL}/{from_lon},{from_lat};{to_lon},{to_lat}",
            params={"overview": "false", "alternatives": "false"},
            timeout=5,
        )
        if not resp.ok:
            return None
        data = resp.json()
        if data.get("code") != "Ok" or not data.get("routes"):
            return None
        route = data["routes"][0]
        return {
            "distance_km": round(route["distance"] / 1000.0, 2),
            "duration_min": round(route["duration"] / 60.0, 1),
            "source": "osrm",
        }
    except (requests.RequestException, ValueError, KeyError):
        return None


def fallback_eta(from_lat: float, from_lon: float, to_lat: float, to_lon: float
                 ) -> dict:
    """Crude fallback: haversine + gjennomsnittsfart. Bruker 50 km/t i tettsted."""
    km = haversine_km(from_lat, from_lon, to_lat, to_lon)
    minutes = (km / 50.0) * 60.0 * 1.4  # 1.4-faktor for at fugleflukt < veivei
    return {
        "distance_km": round(km, 2),
        "duration_min": round(minutes, 1),
        "source": "fallback",
    }


def compute_eta(from_lat: float, from_lon: float, to_lat: float, to_lon: float
                ) -> dict:
    """Returnerer {distance_km, duration_min, source}. Aldri None."""
    return osrm_route(from_lat, from_lon, to_lat, to_lon) \
        or fallback_eta(from_lat, from_lon, to_lat, to_lon)


def order_destination(order: dict) -> Optional[Tuple[float, float]]:
    """Plukk ut leveringskoordinater fra en Havøyet-ordre.

    Foretrekker pre-cached koordinater på ordren (hvis admin har lagret dem),
    faller tilbake til geocoding av kundens leveringsadresse.
    """
    # Direkte koordinater (cachet)
    cached_lat = order.get("destination_lat") or order.get("levering_lat")
    cached_lon = order.get("destination_lon") or order.get("levering_lon")
    if cached_lat and cached_lon:
        try:
            return float(cached_lat), float(cached_lon)
        except (TypeError, ValueError):
            pass

    # Bygge adresse fra ordre-feltene
    kunde = order.get("kunde") or {}
    levering = order.get("levering") or {}

    addr = (
        levering.get("adresse")
        or order.get("leveringsadresse")
        or kunde.get("adresse")
    )
    postnr = (
        levering.get("postnr")
        or order.get("leveringspostnr")
        or kunde.get("postnr")
    )
    poststed = (
        levering.get("poststed")
        or order.get("leveringspoststed")
        or kunde.get("poststed")
    )

    if not addr:
        return None

    return geocode(str(addr), str(postnr) if postnr else None,
                   str(poststed) if poststed else None)


def osrm_trip(coords: list[tuple[float, float]], *, source_first: bool = True,
              roundtrip: bool = False) -> Optional[dict]:
    """TSP-optimalisering via OSRM /trip. coords = [(lat, lon), ...].

    Returnerer:
      {
        "order": [0, 2, 1, 3],          # nye indekser inn i input-listen
        "legs": [{distance_km, duration_min}, ...],
        "total_distance_km": float,
        "total_duration_min": float,
        "geometry": GeoJSON (LineString),
        "source": "osrm"
      }
    eller None ved feil. Krever minst 2 koordinater.
    """
    if not coords or len(coords) < 2:
        return None
    coord_str = ";".join(f"{lon},{lat}" for lat, lon in coords)
    params = {
        "source": "first" if source_first else "any",
        "destination": "any",
        "roundtrip": "true" if roundtrip else "false",
        "geometries": "geojson",
        "overview": "full",
        "steps": "false",
    }
    try:
        resp = requests.get(f"{_OSRM_TRIP_URL}/{coord_str}", params=params, timeout=15)
        if not resp.ok:
            return None
        data = resp.json()
        if data.get("code") != "Ok" or not data.get("trips") or not data.get("waypoints"):
            return None
        trip = data["trips"][0]
        waypoints = data["waypoints"]
        # waypoint_index forteller hvor input-coord nr i havnet i optimalisert rute.
        # Vi snur det til ordnet liste av input-indekser.
        order_pairs = [(wp.get("waypoint_index"), i) for i, wp in enumerate(waypoints)
                       if wp.get("waypoint_index") is not None]
        order_pairs.sort(key=lambda x: x[0])
        order = [orig_idx for _, orig_idx in order_pairs]
        legs = [
            {
                "distance_km": round(l.get("distance", 0) / 1000.0, 2),
                "duration_min": round(l.get("duration", 0) / 60.0, 1),
            }
            for l in trip.get("legs", [])
        ]
        return {
            "order": order,
            "legs": legs,
            "total_distance_km": round(trip.get("distance", 0) / 1000.0, 2),
            "total_duration_min": round(trip.get("duration", 0) / 60.0, 1),
            "geometry": trip.get("geometry"),
            "source": "osrm",
        }
    except (requests.RequestException, ValueError, KeyError, IndexError):
        return None


def osrm_table_one_to_many(source: tuple[float, float],
                           destinations: list[tuple[float, float]]) -> Optional[list[dict]]:
    """Hent kjøretid + distanse fra ÉN source til mange destinasjoner i én request.

    Returnerer liste {distance_km, duration_min, source} i samme rekkefølge som
    destinasjons-listen, eller None ved feil.
    """
    if not destinations:
        return []
    coords = [source] + destinations
    coord_str = ";".join(f"{lon},{lat}" for lat, lon in coords)
    dest_indexes = ";".join(str(i) for i in range(1, len(coords)))
    params = {
        "sources": "0",
        "destinations": dest_indexes,
        "annotations": "duration,distance",
    }
    try:
        resp = requests.get(f"{_OSRM_TABLE_URL}/{coord_str}", params=params, timeout=10)
        if not resp.ok:
            return None
        data = resp.json()
        if data.get("code") != "Ok":
            return None
        durations = (data.get("durations") or [[]])[0]  # sec
        distances = (data.get("distances") or [[]])[0]  # m
        out = []
        for i in range(len(destinations)):
            d_sec = durations[i] if i < len(durations) else None
            d_m = distances[i] if i < len(distances) else None
            out.append({
                "distance_km": round((d_m or 0) / 1000.0, 2) if d_m is not None else None,
                "duration_min": round((d_sec or 0) / 60.0, 1) if d_sec is not None else None,
                "source": "osrm",
            })
        return out
    except (requests.RequestException, ValueError, KeyError, IndexError):
        return None
