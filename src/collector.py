from datetime import datetime, timezone
import requests
from messaging import ROUTING_KEY
from models import WeatherObservation, db
from monitoring import metrics_registry

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

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


def resolve_city_coordinates(city):
    normalized = (city or "Boulder").strip()
    for known_city, coords in CITY_COORDINATES.items():
        if known_city.lower() == normalized.lower():
            return known_city, coords
    return normalized, CITY_COORDINATES["Boulder"]


def parse_observed_timestamp(raw_time):
    if not raw_time:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    cleaned = raw_time.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(cleaned)
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def fetch_open_meteo_reading(latitude, longitude, http_get=requests.get):
    """Fetches current weather metrics from the public Open-Meteo REST API."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "timezone": "UTC",
    }
    response = http_get(OPEN_METEO_URL, params=params, timeout=8)
    response.raise_for_status()
    payload = response.json()
    current = payload.get("current", {})
    if "temperature_2m" not in current:
        raise ValueError("Open-Meteo response missing current.temperature_2m")

    return {
        "temperature_c": float(current["temperature_2m"]),
        "humidity_pct": float(current.get("relative_humidity_2m", 0.0)),
        "wind_speed_kmh": float(current.get("wind_speed_10m", 0.0)),
        "observed_at": parse_observed_timestamp(current.get("time")),
    }


def store_observation_and_publish(
    city,
    temperature_c,
    humidity_pct,
    wind_speed_kmh,
    observed_at=None,
    source="open-meteo",
    broker=None,
):
    timestamp = observed_at or datetime.now(timezone.utc).replace(tzinfo=None)
    record = WeatherObservation(
        city=city,
        observed_at=timestamp,
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
                "temperature_c": record.temperature_c,
            },
        )
    return record


def collect_city_weather(
    city="Boulder",
    broker=None,
    metrics=None,
    http_get=requests.get,
):
    canonical_city, coords = resolve_city_coordinates(city)
    active_metrics = metrics or metrics_registry
    try:
        reading = fetch_open_meteo_reading(
            coords["latitude"], coords["longitude"], http_get=http_get
        )
        record = store_observation_and_publish(
            city=canonical_city,
            temperature_c=reading["temperature_c"],
            humidity_pct=reading["humidity_pct"],
            wind_speed_kmh=reading["wind_speed_kmh"],
            observed_at=reading["observed_at"],
            source="open-meteo",
            broker=broker,
        )
        active_metrics.record_collection(success=True)
        return record
    except Exception:
        active_metrics.record_collection(success=False)
        raise


if __name__ == "__main__":
    from app import create_app

    application = create_app(seed_on_boot=False)
    with application.app_context():
        active_broker = application.extensions["event_broker"]
        for target_city in ("Boulder", "Denver", "Colorado Springs", "Fort Collins"):
            obs = collect_city_weather(target_city, broker=active_broker)
            print(
                f"Collected {obs.city}: {obs.temperature_c:.1f} C, "
                f"humidity {obs.humidity_pct:.0f}%, wind {obs.wind_speed_kmh:.1f} km/h"
            )
