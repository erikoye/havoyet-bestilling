"""ETA-beregning: kunde-adresse → koordinater → minutter til levering.

Hybrid-strategi:
  1) Google Maps API (hvis GOOGLE_MAPS_API_KEY er satt) — bruker faktisk
     trafikkdata for realistiske ETAs, særlig viktig i Bergen rushtid.
  2) Nominatim + OSRM (gratis fallback) — brukes hvis Google-key ikke er
     satt eller Google returnerer feil.
  3) Haversine + 50 km/t (siste fallback) — bare hvis alt nett feiler.
"""
from __future__ import annotations

import os
import time
from typing import Optional, Tuple
import math
import requests

_USER_AGENT = "HavoyetBestilling/1.0 (kontakt: erik@havoyet.no)"
_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
_OSRM_URL = "https://router.project-osrm.org/route/v1/driving"
_OSRM_TRIP_URL = "https://router.project-osrm.org/trip/v1/driving"
_OSRM_TABLE_URL = "https://router.project-osrm.org/table/v1/driving"

# Google Maps Platform endpoints (alle krever ?key=API_KEY)
_GOOGLE_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
_GOOGLE_DIRECTIONS_URL = "https://maps.googleapis.com/maps/api/directions/json"
_GOOGLE_MATRIX_URL = "https://maps.googleapis.com/maps/api/distancematrix/json"


def _google_key() -> str:
    """Leser API-key på kall-tid så Render kan oppdatere uten restart."""
    return (os.environ.get("GOOGLE_MAPS_API_KEY") or "").strip()

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


def google_geocode(query: str) -> Optional[Tuple[float, float]]:
    """Geokoding via Google. Returnerer None ved feil/ikke-funnet."""
    key = _google_key()
    if not key:
        return None
    try:
        resp = requests.get(
            _GOOGLE_GEOCODE_URL,
            params={"address": query, "region": "no", "language": "no", "key": key},
            timeout=5,
        )
        if not resp.ok:
            return None
        data = resp.json()
        if data.get("status") != "OK" or not data.get("results"):
            return None
        loc = data["results"][0]["geometry"]["location"]
        return float(loc["lat"]), float(loc["lng"])
    except (requests.RequestException, ValueError, KeyError):
        return None


def nominatim_geocode(query: str) -> Optional[Tuple[float, float]]:
    """Geokoding via Nominatim (fallback)."""
    try:
        resp = requests.get(
            _NOMINATIM_URL,
            params={
                "q": query, "format": "json", "limit": 1,
                "countrycodes": "no", "accept-language": "no",
            },
            headers={"User-Agent": _USER_AGENT},
            timeout=5,
        )
        if not resp.ok:
            return None
        results = resp.json()
        if not results:
            return None
        first = results[0]
        return (float(first["lat"]), float(first["lon"]))
    except (requests.RequestException, ValueError, KeyError):
        return None


def geocode(address: str, postnr: str | None = None, poststed: str | None = None) -> Optional[Tuple[float, float]]:
    """Slå opp adresse → (lat, lon). Google først, Nominatim som fallback. Cachet."""
    if not address:
        return None
    parts = [p for p in (address.strip(), postnr, poststed) if p]
    parts.append("Norge")
    query = ", ".join(parts)

    now = time.time()
    cached = _GEOCODE_CACHE.get(query)
    if cached and now - cached[0] < _GEOCODE_TTL:
        return cached[1]

    coords = google_geocode(query) or nominatim_geocode(query)
    _GEOCODE_CACHE[query] = (now, coords)
    return coords


def google_directions_eta(from_lat: float, from_lon: float, to_lat: float, to_lon: float,
                          *, with_traffic: bool = False) -> Optional[dict]:
    """ETA fra A → B via Google Directions.

    with_traffic=True bruker Directions Advanced (kostbart: NOK 93/1K kall).
    Default er False (basic tier: NOK 47/1K kall) — vi sparer ~50% når sanntids-
    trafikk ikke er kritisk (f.eks. live-poll, proximity-watcher).
    """
    key = _google_key()
    if not key:
        return None
    try:
        params = {
            "origin": f"{from_lat},{from_lon}",
            "destination": f"{to_lat},{to_lon}",
            "mode": "driving",
            "key": key,
        }
        if with_traffic:
            params["departure_time"] = "now"
            params["traffic_model"] = "best_guess"
        resp = requests.get(_GOOGLE_DIRECTIONS_URL, params=params, timeout=5)
        if not resp.ok:
            return None
        data = resp.json()
        if data.get("status") != "OK" or not data.get("routes"):
            return None
        leg = data["routes"][0]["legs"][0]
        # duration_in_traffic finnes når mode=driving + departure_time gitt
        duration_sec = (leg.get("duration_in_traffic") or leg.get("duration") or {}).get("value", 0)
        distance_m = (leg.get("distance") or {}).get("value", 0)
        return {
            "distance_km": round(distance_m / 1000.0, 2),
            "duration_min": round(duration_sec / 60.0, 1),
            "source": "google",
        }
    except (requests.RequestException, ValueError, KeyError, IndexError):
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
    """Returnerer {distance_km, duration_min, source}. Aldri None.
    Forsøker Google først (trafikk-bevisst), så OSRM, så haversine-fallback.
    """
    return (
        google_directions_eta(from_lat, from_lon, to_lat, to_lon)
        or osrm_route(from_lat, from_lon, to_lat, to_lon)
        or fallback_eta(from_lat, from_lon, to_lat, to_lon)
    )


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


def google_directions_fixed_order(coords: list[tuple[float, float]]) -> Optional[dict]:
    """Google Directions med fast rekkefølge (ingen optimalisering).

    coords = [start, stop1, stop2, ..., end]
    Returnerer {legs, total_distance_km, total_duration_min, geometry} —
    samme form som google_directions_trip, men respekterer input-rekkefølgen.
    """
    key = _google_key()
    if not key or not coords or len(coords) < 2:
        return None
    origin = f"{coords[0][0]},{coords[0][1]}"
    destination = f"{coords[-1][0]},{coords[-1][1]}"
    waypoints = coords[1:-1]
    wp = "|".join(f"{la},{lo}" for la, lo in waypoints) if waypoints else None
    try:
        params = {
            "origin": origin,
            "destination": destination,
            "mode": "driving",
            "departure_time": "now",
            "traffic_model": "best_guess",
            "key": key,
        }
        if wp:
            params["waypoints"] = wp
        resp = requests.get(_GOOGLE_DIRECTIONS_URL, params=params, timeout=15)
        if not resp.ok:
            return None
        data = resp.json()
        if data.get("status") != "OK" or not data.get("routes"):
            return None
        route = data["routes"][0]
        legs = route.get("legs", [])
        out_legs = [
            {
                "distance_km": round((l.get("distance") or {}).get("value", 0) / 1000.0, 2),
                "duration_min": round(
                    ((l.get("duration_in_traffic") or l.get("duration") or {}).get("value", 0)) / 60.0, 1
                ),
            }
            for l in legs
        ]
        total_dist = sum((l.get("distance") or {}).get("value", 0) for l in legs)
        total_dur = sum(((l.get("duration_in_traffic") or l.get("duration") or {}).get("value", 0))
                        for l in legs)
        polyline = (route.get("overview_polyline") or {}).get("points")
        geometry = None
        if polyline:
            try:
                decoded = _decode_polyline(polyline)
                geometry = {"type": "LineString", "coordinates": [[lon, lat] for lat, lon in decoded]}
            except Exception:
                pass
        return {
            "legs": out_legs,
            "total_distance_km": round(total_dist / 1000.0, 2),
            "total_duration_min": round(total_dur / 60.0, 1),
            "geometry": geometry,
            "source": "google",
        }
    except (requests.RequestException, ValueError, KeyError, IndexError):
        return None


def google_directions_trip(coords: list[tuple[float, float]], *,
                            roundtrip: bool = False) -> Optional[dict]:
    """TSP via Google Directions med 'optimize:true' i waypoints.

    coords = [(lat, lon), ...] — første er start (depot), siste er destination.
    roundtrip=True returnerer til depot. Hvis ikke, siste coord er sluttpunkt.

    Returnerer:
      {order, legs, total_distance_km, total_duration_min, geometry, source}
    eller None ved feil.
    """
    key = _google_key()
    if not key or not coords or len(coords) < 2:
        return None
    # Google API har max 25 waypoints (eks. origin/destination) i standard tier
    if len(coords) > 23 and not roundtrip:
        return None

    origin = f"{coords[0][0]},{coords[0][1]}"
    if roundtrip:
        # Tour: depot → alle stopp (optimaliseres) → depot
        waypoints = coords[1:]
        destination = origin
    else:
        # Linje: depot → alle stopp (optimaliseres) → siste stopp ikke optimaliseres
        waypoints = coords[1:-1]
        destination = f"{coords[-1][0]},{coords[-1][1]}"

    # Når vi har minst ett waypoint, bruk optimize:true
    if waypoints:
        wp = "optimize:true|" + "|".join(f"{la},{lo}" for la, lo in waypoints)
    else:
        wp = None

    try:
        params = {
            "origin": origin,
            "destination": destination,
            "mode": "driving",
            "departure_time": "now",
            "traffic_model": "best_guess",
            "key": key,
        }
        if wp:
            params["waypoints"] = wp
        resp = requests.get(_GOOGLE_DIRECTIONS_URL, params=params, timeout=15)
        if not resp.ok:
            return None
        data = resp.json()
        if data.get("status") != "OK" or not data.get("routes"):
            return None
        route = data["routes"][0]
        legs = route.get("legs", [])
        wp_order = route.get("waypoint_order", []) or []

        # Bygg input-index-ordre. coords[0] = depot (alltid index 0).
        # Resterende waypoints er coords[1:1+len(waypoints)].
        # waypoint_order er en permutasjon av [0..len(waypoints)-1].
        order = [0]  # depot først
        for w_idx in wp_order:
            order.append(1 + w_idx)
        if not roundtrip:
            order.append(len(coords) - 1)  # destination sist
        # Hvis ingen waypoints: order = [0, 1] for normal linje

        out_legs = [
            {
                "distance_km": round((l.get("distance") or {}).get("value", 0) / 1000.0, 2),
                "duration_min": round(
                    ((l.get("duration_in_traffic") or l.get("duration") or {}).get("value", 0)) / 60.0, 1
                ),
            }
            for l in legs
        ]
        total_distance = sum((l.get("distance") or {}).get("value", 0) for l in legs)
        total_duration = sum(
            ((l.get("duration_in_traffic") or l.get("duration") or {}).get("value", 0))
            for l in legs
        )

        # Geometry: decode polyline til GeoJSON så frontend kan tegne den
        polyline = (route.get("overview_polyline") or {}).get("points")
        geometry = None
        if polyline:
            try:
                coords_decoded = _decode_polyline(polyline)
                geometry = {
                    "type": "LineString",
                    "coordinates": [[lon, lat] for lat, lon in coords_decoded],
                }
            except Exception:
                pass

        return {
            "order": order,
            "legs": out_legs,
            "total_distance_km": round(total_distance / 1000.0, 2),
            "total_duration_min": round(total_duration / 60.0, 1),
            "geometry": geometry,
            "source": "google",
        }
    except (requests.RequestException, ValueError, KeyError, IndexError):
        return None


def _decode_polyline(s: str) -> list[tuple[float, float]]:
    """Google polyline algorithm → [(lat, lon), ...]."""
    coords = []
    index = lat = lng = 0
    while index < len(s):
        for which in ('lat', 'lng'):
            result = shift = 0
            while True:
                b = ord(s[index]) - 63
                index += 1
                result |= (b & 0x1f) << shift
                shift += 5
                if b < 0x20:
                    break
            dv = ~(result >> 1) if (result & 1) else (result >> 1)
            if which == 'lat':
                lat += dv
            else:
                lng += dv
        coords.append((lat / 1e5, lng / 1e5))
    return coords


def google_matrix_one_to_many(source: tuple[float, float],
                               destinations: list[tuple[float, float]],
                               *, with_traffic: bool = False) -> Optional[list[dict]]:
    """Google Distance Matrix: ÉN source → mange destinasjoner.

    with_traffic=True bruker Distance Matrix Advanced (NOK 93/1K elementer).
    Default er False (basic, NOK 47/1K) — vi sparer 50% på live-poll-flyten
    hvor sanntids-trafikk ikke flytter "X min unna"-tallet vesentlig.
    """
    key = _google_key()
    if not key or not destinations:
        return None if not key else []
    try:
        params = {
            "origins": f"{source[0]},{source[1]}",
            "destinations": "|".join(f"{la},{lo}" for la, lo in destinations),
            "mode": "driving",
            "key": key,
        }
        if with_traffic:
            params["departure_time"] = "now"
            params["traffic_model"] = "best_guess"
        resp = requests.get(_GOOGLE_MATRIX_URL, params=params, timeout=10)
        if not resp.ok:
            return None
        data = resp.json()
        if data.get("status") != "OK" or not data.get("rows"):
            return None
        elements = data["rows"][0].get("elements", [])
        out = []
        for el in elements:
            if el.get("status") != "OK":
                out.append({"distance_km": None, "duration_min": None, "source": "google"})
                continue
            duration_sec = (el.get("duration_in_traffic") or el.get("duration") or {}).get("value", 0)
            distance_m = (el.get("distance") or {}).get("value", 0)
            out.append({
                "distance_km": round(distance_m / 1000.0, 2),
                "duration_min": round(duration_sec / 60.0, 1),
                "source": "google",
            })
        return out
    except (requests.RequestException, ValueError, KeyError, IndexError):
        return None


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


def trip_optimize(coords: list[tuple[float, float]], *, roundtrip: bool = False) -> Optional[dict]:
    """Foretrukket entry-point for TSP: Google først, så OSRM."""
    return (
        google_directions_trip(coords, roundtrip=roundtrip)
        or osrm_trip(coords, source_first=True, roundtrip=roundtrip)
    )


def matrix_one_to_many(source: tuple[float, float],
                        destinations: list[tuple[float, float]]) -> Optional[list[dict]]:
    """Foretrukket entry-point for batched ETA: Google først, så OSRM."""
    return (
        google_matrix_one_to_many(source, destinations)
        or osrm_table_one_to_many(source, destinations)
    )


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
