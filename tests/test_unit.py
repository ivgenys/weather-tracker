"""Unit tests: pure functions only, no database, network or broker."""
import pytest

from analyzer import classify_weather_alert, compute_daily_metrics
from collector import (
    UnknownCityError,
    canonical_city_name,
    coordinates_for,
    parse_observed_timestamp,
)
from models import normalize_database_uri
from monitoring import MetricsRegistry
from webapp import parse_reading


def test_daily_metrics_for_no_readings_is_none():
    assert compute_daily_metrics([]) is None


def test_daily_metrics_aggregates_readings():
    readings = [
        {"temperature_c": 4.0, "humidity_pct": 60.0, "wind_speed_kmh": 12.0},
        {"temperature_c": 10.0, "humidity_pct": 40.0, "wind_speed_kmh": 24.0},
        {"temperature_c": 16.0, "humidity_pct": 50.0, "wind_speed_kmh": 18.0},
    ]
    assert compute_daily_metrics(readings) == {
        "avg_temp_c": 10.0,
        "min_temp_c": 4.0,
        "max_temp_c": 16.0,
        "avg_humidity_pct": 50.0,
        "max_wind_kmh": 24.0,
        "sample_count": 3,
        "alert_status": "NORMAL",
    }


@pytest.mark.parametrize(
    "min_temp, max_temp, max_wind, expected",
    [
        (7.5, 22.0, 14.0, "NORMAL"),
        (0.1, 31.9, 39.9, "NORMAL"),
        (0.0, 8.0, 15.0, "FREEZE_WARNING"),
        (18.0, 32.0, 10.0, "HEAT_ADVISORY"),
        (9.0, 21.0, 40.0, "HIGH_WIND_ADVISORY"),
        (-5.0, 3.0, 60.0, "FREEZE_WARNING"),  # freeze outranks wind
        (20.0, 35.0, 55.0, "HEAT_ADVISORY"),  # heat outranks wind
    ],
)
def test_alert_thresholds_and_precedence(min_temp, max_temp, max_wind, expected):
    assert classify_weather_alert(min_temp, max_temp, max_wind) == expected


def test_city_names_match_known_cities_ignoring_case():
    assert canonical_city_name("  denver ") == "Denver"
    assert canonical_city_name("Ward Station") == "Ward Station"
    assert canonical_city_name(None) == ""


def test_coordinates_exist_only_for_configured_cities():
    name, coords = coordinates_for("fort collins")
    assert name == "Fort Collins"
    assert coords == {"latitude": 40.5853, "longitude": -105.0844}
    with pytest.raises(UnknownCityError):
        coordinates_for("Atlantis")


def test_postgres_scheme_is_rewritten_for_sqlalchemy():
    assert (
        normalize_database_uri("postgres://user:pw@db.internal:5432/weather")
        == "postgresql://user:pw@db.internal:5432/weather"
    )
    assert normalize_database_uri("sqlite:///:memory:") == "sqlite:///:memory:"


def test_timestamps_are_stored_as_naive_utc():
    assert parse_observed_timestamp("2026-11-18T14:30:00Z").isoformat() == "2026-11-18T14:30:00"
    assert parse_observed_timestamp("2026-11-18T07:30:00-07:00").isoformat() == "2026-11-18T14:30:00"
    assert parse_observed_timestamp("2026-11-18T14:30").isoformat() == "2026-11-18T14:30:00"


def test_parse_reading_accepts_a_valid_reading():
    values, error = parse_reading(
        {"city": "boulder", "temperature_c": "3.5", "humidity_pct": "40", "wind_speed_kmh": "12"}
    )
    assert error is None
    assert values == {
        "city": "Boulder",
        "temperature_c": 3.5,
        "humidity_pct": 40.0,
        "wind_speed_kmh": 12.0,
    }


@pytest.mark.parametrize(
    "fields, expected_error",
    [
        ({"temperature_c": 1, "humidity_pct": 1, "wind_speed_kmh": 1}, "city is required"),
        ({"city": "Boulder", "humidity_pct": 1, "wind_speed_kmh": 1}, "temperature_c is required"),
        (
            {"city": "Boulder", "temperature_c": "warm", "humidity_pct": 1, "wind_speed_kmh": 1},
            "temperature_c must be a number",
        ),
        (
            {"city": "Boulder", "temperature_c": 5, "humidity_pct": 140, "wind_speed_kmh": 1},
            "humidity_pct must be between 0 and 100",
        ),
        (
            {"city": "Boulder", "temperature_c": "nan", "humidity_pct": 50, "wind_speed_kmh": 1},
            "temperature_c must be between -90 and 60",
        ),
    ],
)
def test_parse_reading_rejects_bad_input(fields, expected_error):
    values, error = parse_reading(fields)
    assert values is None
    assert error == expected_error


def test_metrics_registry_counts_requests_errors_and_latency():
    registry = MetricsRegistry()
    registry.record_http_request("GET /", 200, 12.0)
    registry.record_http_request("POST /api/observations", 400, 28.0)
    registry.record_collection(success=True)
    registry.record_collection(success=False)
    registry.record_analysis()

    snapshot = registry.snapshot()
    assert snapshot["http_requests_total"] == 2
    assert snapshot["http_errors_total"] == 1
    assert snapshot["http_request_duration_ms_avg"] == 20.0
    assert snapshot["http_request_duration_ms_max"] == 28.0
    assert snapshot["collector_runs_total"] == 2
    assert snapshot["collector_errors_total"] == 1
    assert snapshot["analyzer_runs_total"] == 1
    assert "weather_tracker_http_requests_total 2" in registry.render_prometheus()
