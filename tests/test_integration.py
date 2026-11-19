import os

os.environ.setdefault("SKIP_DEFAULT_APP_INIT", "true")

import pytest
from app import create_app
from messaging import InMemoryEventBroker
from models import db
from monitoring import MetricsRegistry


@pytest.fixture()
def test_env(tmp_path):
    db_file = tmp_path / "integration_test.sqlite3"
    metrics = MetricsRegistry()
    broker = InMemoryEventBroker(metrics=metrics)
    flask_app = create_app(
        config_overrides={
            "TESTING": True,
            "SQLALCHEMY_DATABASE_URI": f"sqlite:///{db_file}",
        },
        broker=broker,
        metrics=metrics,
        seed_on_boot=False,
    )
    with flask_app.test_client() as client:
        yield client, flask_app, broker, metrics
    with flask_app.app_context():
        db.session.remove()
        db.drop_all()


def test_rest_post_observation_persists_and_triggers_analyzer(test_env):
    client, _app, broker, _metrics = test_env

    resp = client.post(
        "/api/observations",
        json={
            "city": "Boulder",
            "temperature_c": -3.0,
            "humidity_pct": 68.0,
            "wind_speed_kmh": 21.0,
            "observed_at": "2026-11-18T08:00:00Z",
        },
    )
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["observation"]["city"] == "Boulder"
    assert body["daily_summary"]["alert_status"] == "FREEZE_WARNING"
    assert len(broker.published_messages) == 1
    assert broker.published_messages[0]["routing_key"] == "weather.collected"

    second = client.post(
        "/api/observations",
        json={
            "city": "Boulder",
            "temperature_c": 5.0,
            "humidity_pct": 42.0,
            "wind_speed_kmh": 15.0,
            "observed_at": "2026-11-18T14:00:00Z",
        },
    )
    assert second.status_code == 201

    summaries_resp = client.get("/api/summaries?city=Boulder")
    assert summaries_resp.status_code == 200
    summaries = summaries_resp.get_json()
    assert len(summaries) == 1
    assert summaries[0]["sample_count"] == 2
    assert summaries[0]["avg_temp_c"] == 1.0
    assert summaries[0]["min_temp_c"] == -3.0
    assert summaries[0]["max_temp_c"] == 5.0
    assert summaries[0]["alert_status"] == "FREEZE_WARNING"


def test_rest_post_validation_rejects_missing_fields(test_env):
    client, *_ = test_env
    resp = client.post("/api/observations", json={"city": "Denver"})
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_web_form_submission_and_dashboard_reporting(test_env):
    client, *_ = test_env
    post_resp = client.post(
        "/observations",
        data={
            "city": "Denver",
            "temperature_c": "19.4",
            "humidity_pct": "33.0",
            "wind_speed_kmh": "44.5",
        },
        follow_redirects=True,
    )
    assert post_resp.status_code == 200
    html = post_resp.get_data(as_text=True)
    assert "Denver" in html
    assert "19.4" in html
    assert "HIGH_WIND_ADVISORY" in html


def test_city_filtering_in_observations_api(test_env):
    client, *_ = test_env
    client.post(
        "/api/observations",
        json={"city": "Boulder", "temperature_c": 7.0, "humidity_pct": 50.0, "wind_speed_kmh": 10.0},
    )
    client.post(
        "/api/observations",
        json={"city": "Austin", "temperature_c": 26.0, "humidity_pct": 55.0, "wind_speed_kmh": 12.0},
    )

    austin_only = client.get("/api/observations?city=Austin").get_json()
    assert len(austin_only) == 1
    assert austin_only[0]["city"] == "Austin"


def test_health_and_metrics_monitoring_endpoints(test_env):
    client, *_ = test_env
    health = client.get("/health")
    assert health.status_code == 200
    assert health.get_json()["status"] == "UP"
    assert health.get_json()["database"] == "connected"

    prom_text = client.get("/metrics").get_data(as_text=True)
    assert "weather_tracker_http_requests_total" in prom_text

    json_metrics = client.get("/api/metrics")
    assert json_metrics.status_code == 200
    assert json_metrics.get_json()["http_requests_total"] >= 1
