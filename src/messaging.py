import json
import logging
import os
import threading
import time

import pika

logger = logging.getLogger(__name__)

EXCHANGE_NAME = "weather.events"
QUEUE_NAME = "weather.observations"
ROUTING_KEY = "weather.collected"


class InMemoryEventBroker:
    """Delivers events to subscribers inside the same process.

    Used when no RabbitMQ URL is configured, and as the fake broker in tests.
    """

    def __init__(self, metrics=None):
        self.published_messages = []
        self._subscribers = {}
        self._metrics = metrics

    def subscribe(self, routing_key, callback):
        self._subscribers.setdefault(routing_key, []).append(callback)

    def publish(self, routing_key, payload):
        self.published_messages.append(
            {"routing_key": routing_key, "payload": dict(payload)}
        )
        if self._metrics is not None:
            self._metrics.record_event_published()
        for callback in self._subscribers.get(routing_key, []):
            callback(dict(payload))
            if self._metrics is not None:
                self._metrics.record_event_consumed()


class RabbitEventBroker:
    """Publishes to and consumes from a durable RabbitMQ queue."""

    def __init__(self, rabbit_url, metrics=None, fallback=None, queue_name=QUEUE_NAME):
        self.rabbit_url = rabbit_url
        self.queue_name = queue_name
        self._metrics = metrics
        self._fallback = fallback or InMemoryEventBroker(metrics=metrics)

    def subscribe(self, routing_key, callback):
        # Only used if RabbitMQ is unreachable at publish time.
        self._fallback.subscribe(routing_key, callback)

    def _open_channel(self):
        params = pika.URLParameters(self.rabbit_url)
        params.socket_timeout = 5
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.exchange_declare(
            exchange=EXCHANGE_NAME, exchange_type="topic", durable=True
        )
        channel.queue_declare(queue=self.queue_name, durable=True)
        channel.queue_bind(
            exchange=EXCHANGE_NAME, queue=self.queue_name, routing_key=ROUTING_KEY
        )
        return connection, channel

    def publish(self, routing_key, payload):
        try:
            connection, channel = self._open_channel()
        except Exception as exc:
            logger.warning(
                "RabbitMQ unreachable (%r); handling the event in this process", exc
            )
            self._fallback.publish(routing_key, payload)
            return
        try:
            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=routing_key,
                body=json.dumps(payload).encode("utf-8"),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,  # persist the message to disk
                ),
            )
            if self._metrics is not None:
                self._metrics.record_event_published()
        finally:
            connection.close()

    def consume(self, handler, stop_event=None):
        """Handles messages one at a time until stop_event is set.

        A message is acknowledged only after the handler returns, so a crash
        mid-way leaves it on the queue for redelivery.
        """
        connection, channel = self._open_channel()
        channel.basic_qos(prefetch_count=1)
        try:
            for method, _properties, body in channel.consume(
                self.queue_name, inactivity_timeout=1
            ):
                if stop_event is not None and stop_event.is_set():
                    break
                if method is None:
                    continue
                try:
                    handler(json.loads(body))
                except Exception:
                    logger.exception("Dropping event that failed to process: %r", body)
                    channel.basic_nack(method.delivery_tag, requeue=False)
                    continue
                channel.basic_ack(method.delivery_tag)
                if self._metrics is not None:
                    self._metrics.record_event_consumed()
        finally:
            if connection.is_open:
                connection.close()

    def start_consumer_thread(self, handler, stop_event=None):
        """Runs consume() in a background thread, reconnecting after failures."""

        def run():
            while stop_event is None or not stop_event.is_set():
                try:
                    self.consume(handler, stop_event)
                except Exception as exc:
                    logger.warning("RabbitMQ consumer stopped (%r); retrying in 5s", exc)
                    time.sleep(5)

        thread = threading.Thread(target=run, name="weather-analyzer", daemon=True)
        thread.start()
        return thread


def build_event_broker(rabbit_url=None, metrics=None):
    url = rabbit_url or os.environ.get("RABBIT_URL") or os.environ.get("CLOUDAMQP_URL")
    if url:
        return RabbitEventBroker(url, metrics=metrics)
    return InMemoryEventBroker(metrics=metrics)
