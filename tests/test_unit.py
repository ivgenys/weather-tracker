import os

os.environ.setdefault("SKIP_DEFAULT_APP_INIT", "true")

from analyzer import classify_weather_alert, compute_daily_metrics
from collector import parse_observed_timestamp, resolve_city_coordinates
from models import normalize_database_uri
from monitoring import MetricsRegistry


def test_compute_daily_metrics_empty_returns_none():
    assert compute_daily_metrics([]) is None


def test_compute_daily_metrics_calculates_aggregates():
    readings = [
        {"temperature_c": 4.0, "humidity_pct": 60.0, "wind_speed_kmh": 12.0},
        {"temperature_c": 10.0, "humidity_pct": 40.0, "wind_speed_kmh": 24.0},
        {"temperature_c": 16.0, "humidity_pct": 50.0, "wind_speed_kmh": 18.0},
    ]
    result = compute_daily_metrics(readings)
    assert result == {
        "avg_temp_c": 10.0,
        "min_temp_c": 4.0,
        "max_temp_c": 16.0,
        "avg_humidity_pct": 50.0,
        "max_wind_kmh": 24.0,
        "sample_count": 3,
        "alert_status": "NORMAL",
    }


def test_classify_weather_alert_thresholds():
    assert classify_weather_alert(-2.5, 8.0, 15.0) == "FREEZE_WARNING"
    assert classify_weather_alert(18.0, 34.5, 10.0) == "HEAT_ADVISORY"
    assert classify_weather_alert(9.0, 21.0, 45.0) == "HIGH_WIND_ADVISORY"
    assert classify_weather_alert(7.5, 22.0, 14.0) == "NORMAL"


def test_resolve_city_coordinates_case_insensitive_and_fallback():
    city, coords = resolve_city_coordinates("denver")
    assert city == "Denver"
    assert coords["latitude"] == 39.7392

    custom_city, fallback_coords = resolve_city_coordinates("CustomStation")
    assert custom_city == "CustomStation"
    assert fallback_coords["latitude"] == 40.0150


def test_normalize_database_uri_converts_postgres_scheme():
    assert (
        normalize_database_uri("postgres://user:pw@db.internal:5432/weather")
        == "postgresql://user:pw@db.internal:5432/weather"
    )
    assert (
        normalize_database_uri("sqlite:///:memory:") == "sqlite:///:memory:"
    )


def test_parse_observed_timestamp_handles_utc_z_suffix():
    ts = parse_observed_timestamp("2026-11-18T14:30:00Z")
    assert ts.year == 2026
    assert ts.month == 11
    assert ts.day == 18
    assert ts.hour == 14
    assert ts.minute == 30
    assert ts.tzinfo is None


def test_metrics_registry_tracks_requests_and_errors():
    registry = MetricsRegistry()
    registry.record_http_request("GET /", 200, 12.0)
    registry.record_http_request("POST /api/observations", 400, 28.0)
    registry.record_collection(success=True)
    registry.record_analysis()

    snap = registry.snapshot()
    assert snap["http_requests_total"] == 2
    assert snap["http_errors_total"] == 1
    assert snap["http_request_duration_ms_avg"] == 20.0
    assert snap["http_request_duration_ms_max"] == 28.0
    assert snap["collector_runs_total"] == 1
    assert snap["analyzer_runs_total"] == 1
    assert "weather_tracker_http_requests_total 2" in registry.render_prometheus()
