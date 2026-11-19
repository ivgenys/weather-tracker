"""Tests against a real RabbitMQ broker. They run when TEST_RABBIT_URL is set (CI does)."""
import os
import threading
import time
import uuid

import pika
import pytest

from analyzer import make_event_handler
from messaging import ROUTING_KEY, InMemoryEventBroker, RabbitEventBroker
from monitoring import MetricsRegistry

RABBIT_URL = os.environ.get("TEST_RABBIT_URL")
pytestmark = pytest.mark.skipif(not RABBIT_URL, reason="TEST_RABBIT_URL is not set")


@pytest.fixture()
def rabbit():
    """A broker on its own queue, removed again after the test.

    `fallback` records events that did not go through RabbitMQ, so a test can
    check that the queue was really used.
    """
    metrics = MetricsRegistry()
    fallback = InMemoryEventBroker()
    broker = RabbitEventBroker(
        RABBIT_URL,
        metrics=metrics,
        fallback=fallback,
        queue_name=f"test.weather.{uuid.uuid4().hex}",
    )
    stop = threading.Event()
    threads = []

    def start_consumer(handler):
        threads.append(broker.start_consumer_thread(handler, stop))
        assert broker.consumer_ready.wait(timeout=15), "consumer did not connect"

    yield broker, metrics, fallback, start_consumer

    stop.set()
    for thread in threads:
        thread.join(timeout=5)
    connection = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
    connection.channel().queue_delete(broker.queue_name)
    connection.close()


def test_published_event_is_delivered_to_the_consumer(rabbit):
    broker, metrics, fallback, start_consumer = rabbit
    received = []
    delivered = threading.Event()

    def handler(payload):
        received.append(payload)
        delivered.set()

    start_consumer(handler)
    broker.publish(ROUTING_KEY, {"city": "Boulder", "summary_date": "2026-11-18"})

    assert delivered.wait(timeout=10), "consumer did not receive the event"
    assert received == [{"city": "Boulder", "summary_date": "2026-11-18"}]
    assert fallback.published_messages == []
    assert metrics.snapshot()["events_published_total"] == 1


def test_reading_posted_to_the_api_is_summarized_by_the_queue_consumer(rabbit, make_app):
    broker, metrics, fallback, start_consumer = rabbit
    app = make_app(broker, metrics)
    start_consumer(make_event_handler(app, metrics))
    client = app.test_client()

    response = client.post(
        "/api/observations",
        json={
            "city": "Seattle",
            "temperature_c": 33.5,
            "humidity_pct": 30.0,
            "wind_speed_kmh": 9.0,
            "observed_at": "2026-07-04T21:00:00Z",
        },
    )
    assert response.status_code == 201

    summaries = []
    deadline = time.monotonic() + 10
    while not summaries and time.monotonic() < deadline:
        time.sleep(0.2)
        summaries = client.get("/api/summaries?city=Seattle").get_json()

    assert len(summaries) == 1, "analyzer did not write a summary within 10 seconds"
    assert summaries[0]["alert_status"] == "HEAT_ADVISORY"
    assert summaries[0]["sample_count"] == 1
    assert fallback.published_messages == [], "the event bypassed RabbitMQ"
    assert client.get("/health").get_json()["event_broker"] == "rabbitmq"
