"""Integration tests: HTTP routes, the SQL database and the event bus working together."""
from models import DailyWeatherSummary, WeatherObservation

FREEZING_MORNING = {
    "city": "Boulder",
    "temperature_c": -3.0,
    "humidity_pct": 68.0,
    "wind_speed_kmh": 21.0,
    "observed_at": "2026-11-18T08:00:00Z",
}
MILD_AFTERNOON = {
    "city": "Boulder",
    "temperature_c": 5.0,
    "humidity_pct": 42.0,
    "wind_speed_kmh": 15.0,
    "observed_at": "2026-11-18T14:00:00Z",
}


def test_posted_reading_is_stored_published_and_summarized(env):
    response = env.client.post("/api/observations", json=FREEZING_MORNING)

    assert response.status_code == 201
    assert response.get_json()["city"] == "Boulder"
    assert response.get_json()["observed_at"] == "2026-11-18T08:00:00"
    assert [m["routing_key"] for m in env.broker.published_messages] == ["weather.collected"]

    summaries = env.client.get("/api/summaries?city=Boulder").get_json()
    assert len(summaries) == 1
    assert summaries[0]["summary_date"] == "2026-11-18"
    assert summaries[0]["sample_count"] == 1
    assert summaries[0]["alert_status"] == "FREEZE_WARNING"


def test_second_reading_updates_the_same_daily_summary(env):
    env.client.post("/api/observations", json=FREEZING_MORNING)
    env.client.post("/api/observations", json=MILD_AFTERNOON)

    summaries = env.client.get("/api/summaries").get_json()
    assert len(summaries) == 1
    assert summaries[0]["sample_count"] == 2
    assert summaries[0]["avg_temp_c"] == 1.0
    assert summaries[0]["min_temp_c"] == -3.0
    assert summaries[0]["max_temp_c"] == 5.0
    assert summaries[0]["max_wind_kmh"] == 21.0


def test_handling_the_same_event_twice_does_not_change_the_summary(env):
    env.client.post("/api/observations", json=FREEZING_MORNING)
    before = env.client.get("/api/summaries").get_json()

    env.broker.publish(
        "weather.collected", {"city": "Boulder", "summary_date": "2026-11-18"}
    )

    after = env.client.get("/api/summaries").get_json()
    assert len(after) == 1
    for field in ("sample_count", "avg_temp_c", "min_temp_c", "max_temp_c", "alert_status"):
        assert after[0][field] == before[0][field]


def test_readings_on_different_days_get_separate_summaries(env):
    env.client.post("/api/observations", json=FREEZING_MORNING)
    env.client.post(
        "/api/observations", json={**MILD_AFTERNOON, "observed_at": "2026-11-19T14:00:00Z"}
    )

    summaries = env.client.get("/api/summaries?city=Boulder").get_json()
    assert [s["summary_date"] for s in summaries] == ["2026-11-19", "2026-11-18"]
    assert [s["alert_status"] for s in summaries] == ["NORMAL", "FREEZE_WARNING"]


def test_invalid_api_input_is_rejected_and_nothing_is_stored(env):
    missing = env.client.post("/api/observations", json={"city": "Denver"})
    out_of_range = env.client.post(
        "/api/observations", json={**MILD_AFTERNOON, "humidity_pct": 250}
    )
    bad_time = env.client.post(
        "/api/observations", json={**MILD_AFTERNOON, "observed_at": "yesterday"}
    )
    not_json = env.client.post("/api/observations", data="hello")
    bad_limit = env.client.get("/api/observations?limit=many")

    assert missing.status_code == 400
    assert missing.get_json() == {"error": "temperature_c is required"}
    assert out_of_range.status_code == 400
    assert bad_time.status_code == 400
    assert not_json.status_code == 400
    assert bad_limit.status_code == 400
    with env.app.app_context():
        assert WeatherObservation.query.count() == 0
    assert env.broker.published_messages == []


def test_web_form_saves_a_reading_and_the_dashboard_reports_it(env):
    response = env.client.post(
        "/observations",
        data={
            "city": "Denver",
            "temperature_c": "19.4",
            "humidity_pct": "33.0",
            "wind_speed_kmh": "44.5",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    page = response.get_data(as_text=True)
    assert "Saved reading for Denver" in page
    assert "19.4" in page
    assert '<span class="badge badge-HIGH_WIND_ADVISORY">' in page


def test_web_form_with_bad_input_shows_an_error_and_stores_nothing(env):
    response = env.client.post(
        "/observations",
        data={"city": "Denver", "temperature_c": "", "humidity_pct": "33", "wind_speed_kmh": "5"},
        follow_redirects=True,
    )

    assert "temperature_c is required" in response.get_data(as_text=True)
    with env.app.app_context():
        assert WeatherObservation.query.count() == 0
        assert DailyWeatherSummary.query.count() == 0


def test_city_filter_applies_to_api_and_dashboard(env):
    env.client.post("/api/observations", json=FREEZING_MORNING)
    env.client.post("/api/observations", json={**MILD_AFTERNOON, "city": "Austin"})

    austin = env.client.get("/api/observations?city=austin").get_json()
    assert [row["city"] for row in austin] == ["Austin"]

    everything = env.client.get("/").get_data(as_text=True)
    austin_page = env.client.get("/?city=Austin").get_data(as_text=True)
    freeze_badge = '<span class="badge badge-FREEZE_WARNING">'
    assert freeze_badge in everything
    assert freeze_badge not in austin_page


def test_health_reports_database_and_broker(env):
    response = env.client.get("/health")

    assert response.status_code == 200
    body = response.get_json()
    assert body["status"] == "UP"
    assert body["database"] == "connected"
    assert body["event_broker"] == "in-process"


def test_metrics_count_requests_and_pipeline_events(env):
    env.client.post("/api/observations", json=FREEZING_MORNING)
    env.client.post("/api/observations", json={"city": "Denver"})

    snapshot = env.client.get("/api/metrics").get_json()
    assert snapshot["http_requests_total"] == 2
    assert snapshot["http_errors_total"] == 1
    assert snapshot["events_published_total"] == 1
    assert snapshot["events_consumed_total"] == 1
    assert snapshot["analyzer_runs_total"] == 1

    prometheus_text = env.client.get("/metrics").get_data(as_text=True)
    assert "weather_tracker_http_requests_total 2" in prometheus_text
    assert "weather_tracker_analyzer_runs_total 1" in prometheus_text
