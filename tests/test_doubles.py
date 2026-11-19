import os

os.environ.setdefault("SKIP_DEFAULT_APP_INIT", "true")

from unittest.mock import MagicMock, patch
import pytest
from app import create_app
from collector import collect_city_weather, fetch_open_meteo_reading
from messaging import InMemoryEventBroker, RabbitEventBroker
from models import DailyWeatherSummary, WeatherObservation, db
from monitoring import MetricsRegistry


class SpyEventBroker:
    """Test Spy recording all published routing keys and payloads while delegating."""

    def __init__(self, delegate):
        self._delegate = delegate
        self.calls = []

    def subscribe(self, routing_key, callback):
        return self._delegate.subscribe(routing_key, callback)

    def publish(self, routing_key, payload):
        self.calls.append((routing_key, dict(payload)))
        return self._delegate.publish(routing_key, payload)


@pytest.fixture()
def app_ctx(tmp_path):
    db_file = tmp_path / "doubles_test.sqlite3"
    metrics = MetricsRegistry()
    fake_broker = InMemoryEventBroker(metrics=metrics)
    spy_broker = SpyEventBroker(fake_broker)
    flask_app = create_app(
        config_overrides={
            "TESTING": True,
            "SQLALCHEMY_DATABASE_URI": f"sqlite:///{db_file}",
        },
        broker=spy_broker,
        metrics=metrics,
        seed_on_boot=False,
    )
    with flask_app.app_context():
        yield flask_app, fake_broker, spy_broker, metrics
        db.session.remove()
        db.drop_all()


def test_mock_open_meteo_http_client():
    """Demonstrates a Mock object replacing external HTTP network I/O."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "current": {
            "time": "2026-11-18T16:00:00Z",
            "temperature_2m": 11.3,
            "relative_humidity_2m": 38.0,
            "wind_speed_10m": 19.2,
        }
    }
    mock_http_get = MagicMock(return_value=mock_response)

    reading = fetch_open_meteo_reading(40.0150, -105.2705, http_get=mock_http_get)

    mock_http_get.assert_called_once()
    called_args, called_kwargs = mock_http_get.call_args
    assert "api.open-meteo.com" in called_args[0]
    assert called_kwargs["params"]["latitude"] == 40.0150
    assert reading["temperature_c"] == 11.3
    assert reading["humidity_pct"] == 38.0
    assert reading["wind_speed_kmh"] == 19.2


def test_fake_broker_and_spy_verify_end_to_end_event_collaboration(app_ctx):
    """Demonstrates both a Fake broker (InMemoryEventBroker) and a Spy (SpyEventBroker)."""
    _app, fake_broker, spy_broker, metrics = app_ctx

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "current": {
            "time": "2026-11-18T17:00:00Z",
            "temperature_2m": -4.2,
            "relative_humidity_2m": 71.0,
            "wind_speed_10m": 25.0,
        }
    }
    mock_get = MagicMock(return_value=mock_response)

    saved = collect_city_weather(
        "Fort Collins",
        broker=spy_broker,
        metrics=metrics,
        http_get=mock_get,
    )

    assert saved.id is not None
    assert WeatherObservation.query.filter_by(city="Fort Collins").count() == 1

    # Verify Spy recorded exact message publication
    assert len(spy_broker.calls) == 1
    routing_key, payload = spy_broker.calls[0]
    assert routing_key == "weather.collected"
    assert payload["city"] == "Fort Collins"
    assert payload["observation_id"] == saved.id

    # Verify Fake broker dispatched event to analyzer subscriber and persisted summary
    assert len(fake_broker.published_messages) == 1
    summary = DailyWeatherSummary.query.filter_by(city="Fort Collins").first()
    assert summary is not None
    assert summary.min_temp_c == -4.2
    assert summary.alert_status == "FREEZE_WARNING"


def test_rabbit_event_broker_falls_back_gracefully_when_pika_mock_raises():
    """Uses unittest.mock.patch to simulate a RabbitMQ broker outage and verify fallback."""
    metrics = MetricsRegistry()
    fallback_fake = InMemoryEventBroker(metrics=metrics)
    received = []
    fallback_fake.subscribe("weather.collected", lambda msg: received.append(msg))

    rabbit_broker = RabbitEventBroker(
        "amqp://guest:guest@unreachable-broker:5672/%2F",
        metrics=metrics,
        fallback_broker=fallback_fake,
    )

    with patch("messaging.pika.BlockingConnection", side_effect=OSError("Connection refused")):
        rabbit_broker.publish("weather.collected", {"city": "Boulder", "temperature_c": 3.5})

    assert len(received) == 1
    assert received[0]["city"] == "Boulder"
    assert metrics.snapshot()["events_published_total"] == 1
    assert metrics.snapshot()["events_consumed_total"] == 1
