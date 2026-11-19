"""Tests that replace the outside world with test doubles.

Mock: the Open-Meteo HTTP client. Fake: InMemoryEventBroker. Spy: SpyEventBroker.
"""
from unittest.mock import MagicMock, patch

import pika
import requests

from collector import collect_city_weather, fetch_open_meteo_reading
from messaging import InMemoryEventBroker, RabbitEventBroker
from models import DailyWeatherSummary, WeatherObservation
from monitoring import MetricsRegistry


def open_meteo_response(time="2026-11-18T16:00", temperature=11.3, humidity=38.0, wind=19.2):
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "current": {
            "time": time,
            "temperature_2m": temperature,
            "relative_humidity_2m": humidity,
            "wind_speed_10m": wind,
        }
    }
    return response


class SpyEventBroker:
    """Records every publish call, then passes it on to the real broker."""

    def __init__(self, delegate):
        self._delegate = delegate
        self.calls = []

    def subscribe(self, routing_key, callback):
        self._delegate.subscribe(routing_key, callback)

    def publish(self, routing_key, payload):
        self.calls.append((routing_key, dict(payload)))
        self._delegate.publish(routing_key, payload)


def test_fetch_sends_the_expected_request_and_parses_the_reply():
    http_get = MagicMock(return_value=open_meteo_response())

    reading = fetch_open_meteo_reading(40.0150, -105.2705, http_get=http_get)

    http_get.assert_called_once()
    (url,), kwargs = http_get.call_args
    assert url == "https://api.open-meteo.com/v1/forecast"
    assert kwargs["params"]["latitude"] == 40.0150
    assert kwargs["params"]["longitude"] == -105.2705
    assert kwargs["timeout"] == 8
    assert reading["temperature_c"] == 11.3
    assert reading["humidity_pct"] == 38.0
    assert reading["wind_speed_kmh"] == 19.2
    assert reading["observed_at"].isoformat() == "2026-11-18T16:00:00"


def test_collect_endpoint_stores_one_row_per_open_meteo_timestamp(env):
    with patch("collector.requests.get", return_value=open_meteo_response()):
        first = env.client.post("/api/collect", json={"city": "Boulder"})
        repeat = env.client.post("/api/collect", json={"city": "Boulder"})

    assert first.status_code == 201
    assert repeat.status_code == 200
    assert repeat.get_json()["id"] == first.get_json()["id"]
    assert len(env.broker.published_messages) == 1
    summary = env.client.get("/api/summaries?city=Boulder").get_json()[0]
    assert summary["sample_count"] == 1
    assert summary["avg_temp_c"] == 11.3


def test_collect_endpoint_reports_upstream_failure_without_storing_data(env):
    with patch("collector.requests.get", side_effect=requests.ConnectionError("down")):
        response = env.client.post("/api/collect", json={"city": "Denver"})

    assert response.status_code == 502
    with env.app.app_context():
        assert WeatherObservation.query.count() == 0
    assert env.metrics.snapshot()["collector_errors_total"] == 1


def test_collect_endpoint_rejects_a_city_without_coordinates(env):
    with patch("collector.requests.get") as http_get:
        response = env.client.post("/api/collect", json={"city": "Atlantis"})

    assert response.status_code == 400
    http_get.assert_not_called()


def test_collector_publishes_an_event_that_the_analyzer_turns_into_a_summary(make_app):
    metrics = MetricsRegistry()
    fake_broker = InMemoryEventBroker(metrics=metrics)
    spy = SpyEventBroker(fake_broker)
    app = make_app(spy, metrics)
    http_get = MagicMock(return_value=open_meteo_response(temperature=-4.2, wind=25.0))

    with app.app_context():
        saved, created = collect_city_weather(
            "Fort Collins", broker=spy, metrics=metrics, http_get=http_get
        )

        assert created is True
        assert spy.calls == [
            (
                "weather.collected",
                {
                    "event": "weather.collected",
                    "observation_id": saved.id,
                    "city": "Fort Collins",
                    "summary_date": "2026-11-18",
                },
            )
        ]
        summary = DailyWeatherSummary.query.filter_by(city="Fort Collins").one()
        assert summary.min_temp_c == -4.2
        assert summary.alert_status == "FREEZE_WARNING"


def test_event_is_handled_in_process_when_rabbitmq_is_unreachable():
    metrics = MetricsRegistry()
    fallback = InMemoryEventBroker(metrics=metrics)
    received = []
    fallback.subscribe("weather.collected", received.append)
    broker = RabbitEventBroker("amqp://guest:guest@unreachable:5672/%2F", metrics=metrics, fallback=fallback)

    with patch("messaging.pika.BlockingConnection", side_effect=OSError("Connection refused")):
        broker.publish("weather.collected", {"city": "Boulder", "summary_date": "2026-11-18"})

    assert received == [{"city": "Boulder", "summary_date": "2026-11-18"}]
    assert metrics.snapshot()["events_published_total"] == 1
    assert metrics.snapshot()["events_consumed_total"] == 1


def test_event_is_handled_in_process_when_no_queue_accepts_it():
    fallback = InMemoryEventBroker()
    received = []
    fallback.subscribe("weather.collected", received.append)
    broker = RabbitEventBroker("amqp://guest:guest@broker:5672/%2F", fallback=fallback)

    with patch("messaging.pika.BlockingConnection") as connect:
        channel = connect.return_value.channel.return_value
        channel.basic_publish.side_effect = pika.exceptions.UnroutableError([])
        broker.publish("weather.collected", {"city": "Denver", "summary_date": "2026-11-18"})

    channel.confirm_delivery.assert_called_once()
    assert channel.basic_publish.call_args.kwargs["mandatory"] is True
    assert received == [{"city": "Denver", "summary_date": "2026-11-18"}]
