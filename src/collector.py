import argparse
import time
from datetime import datetime, timezone

import requests

from messaging import ROUTING_KEY
from models import WeatherObservation, db
from monitoring import metrics_registry

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_SOURCE = "open-meteo"

CITY_COORDINATES = {
    "Boulder": {"latitude": 40.0150, "longitude": -105.2705},
    "Denver": {"latitude": 39.7392, "longitude": -104.9903},
    "Colorado Springs": {"latitude": 38.8339, "longitude": -104.8214},
    "Fort Collins": {"latitude": 40.5853, "longitude": -105.0844},
    "Seattle": {"latitude": 47.6062, "longitude": -122.3321},
    "Austin": {"latitude": 30.2672, "longitude": -97.7431},
    "Chicago": {"latitude": 41.8781, "longitude": -87.6298},
    "New York": {"latitude": 40.7128, "longitude": -74.0060},
}


class UnknownCityError(ValueError):
    pass


def canonical_city_name(city):
    """Matches a known city ignoring case; otherwise returns the trimmed input."""
    cleaned = str(city or "").strip()
    for known_city in CITY_COORDINATES:
        if known_city.lower() == cleaned.lower():
            return known_city
    return cleaned


def coordinates_for(city):
    name = canonical_city_name(city)
    if name not in CITY_COORDINATES:
        raise UnknownCityError(f"No coordinates configured for city '{city}'")
    return name, CITY_COORDINATES[name]


def parse_observed_timestamp(raw_time):
    """Parses an ISO timestamp into a naive UTC datetime."""
    if not raw_time:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    parsed = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def fetch_open_meteo_reading(latitude, longitude, http_get=None):
    """Fetches the current reading for a location from the Open-Meteo REST API."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "timezone": "UTC",
    }
    response = (http_get or requests.get)(OPEN_METEO_URL, params=params, timeout=8)
    response.raise_for_status()
    current = response.json().get("current", {})
    missing = [
        field
        for field in ("temperature_2m", "relative_humidity_2m", "wind_speed_10m")
        if current.get(field) is None
    ]
    if missing:
        raise ValueError(f"Open-Meteo response is missing {', '.join(missing)}")

    return {
        "temperature_c": float(current["temperature_2m"]),
        "humidity_pct": float(current["relative_humidity_2m"]),
        "wind_speed_kmh": float(current["wind_speed_10m"]),
        "observed_at": parse_observed_timestamp(current.get("time")),
    }


def store_observation_and_publish(
    city,
    temperature_c,
    humidity_pct,
    wind_speed_kmh,
    observed_at=None,
    source=OPEN_METEO_SOURCE,
    broker=None,
):
    """Saves one raw reading, then announces it so the analyzer can update the day."""
    record = WeatherObservation(
        city=city,
        observed_at=observed_at or datetime.now(timezone.utc).replace(tzinfo=None),
        temperature_c=float(temperature_c),
        humidity_pct=float(humidity_pct),
        wind_speed_kmh=float(wind_speed_kmh),
        source=source,
    )
    db.session.add(record)
    db.session.commit()

    if broker is not None:
        broker.publish(
            ROUTING_KEY,
            {
                "event": ROUTING_KEY,
                "observation_id": record.id,
                "city": record.city,
                "summary_date": record.observed_at.date().isoformat(),
            },
        )
    return record


def collect_city_weather(city, broker=None, metrics=None, http_get=None):
    """Collects one city from Open-Meteo. Returns (observation, created).

    Open-Meteo refreshes its current reading every 15 minutes. If we already
    stored the reading for that timestamp, return it instead of saving a copy
    that would skew the daily average.
    """
    active_metrics = metrics or metrics_registry
    name, coords = coordinates_for(city)
    try:
        reading = fetch_open_meteo_reading(
            coords["latitude"], coords["longitude"], http_get=http_get
        )
    except Exception:
        active_metrics.record_collection(success=False)
        raise

    existing = WeatherObservation.query.filter_by(
        city=name, observed_at=reading["observed_at"], source=OPEN_METEO_SOURCE
    ).first()
    if existing is not None:
        active_metrics.record_collection(success=True)
        return existing, False

    record = store_observation_and_publish(
        city=name,
        temperature_c=reading["temperature_c"],
        humidity_pct=reading["humidity_pct"],
        wind_speed_kmh=reading["wind_speed_kmh"],
        observed_at=reading["observed_at"],
        broker=broker,
    )
    active_metrics.record_collection(success=True)
    return record, True


def collect_all_cities(broker=None, metrics=None):
    for city in CITY_COORDINATES:
        try:
            observation, created = collect_city_weather(city, broker=broker, metrics=metrics)
        except Exception as exc:
            print(f"{city}: collection failed ({exc})", flush=True)
            continue
        state = "saved" if created else "already stored"
        print(
            f"{city}: {observation.temperature_c:.1f} C, "
            f"{observation.humidity_pct:.0f}% humidity, "
            f"{observation.wind_speed_kmh:.1f} km/h wind ({state})",
            flush=True,
        )


if __name__ == "__main__":
    from webapp import create_app

    parser = argparse.ArgumentParser(description="Weather Tracker collector")
    parser.add_argument(
        "--interval",
        type=int,
        default=0,
        help="seconds between collection runs; 0 collects once and exits",
    )
    args = parser.parse_args()

    application = create_app(seed_on_boot=False, embedded_analyzer=False)
    with application.app_context():
        while True:
            collect_all_cities(broker=application.extensions["event_broker"])
            db.session.remove()
            if args.interval <= 0:
                break
            time.sleep(args.interval)
