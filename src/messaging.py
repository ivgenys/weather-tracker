import json
import logging
import os
import pika

logger = logging.getLogger(__name__)

EXCHANGE_NAME = "weather.events"
QUEUE_NAME = "weather.observations"
ROUTING_KEY = "weather.collected"


class InMemoryEventBroker:
    """In-process event bus used as single-node fallback and test Fake."""

    def __init__(self, metrics=None):
        self.published_messages = []
        self._subscribers = {}
        self._metrics = metrics

    def subscribe(self, routing_key, callback):
        self._subscribers.setdefault(routing_key, []).append(callback)

    def publish(self, routing_key, payload):
        envelope = {"routing_key": routing_key, "payload": dict(payload)}
        self.published_messages.append(envelope)
        if self._metrics is not None:
            self._metrics.record_event_published()
        for callback in self._subscribers.get(routing_key, []):
            if self._metrics is not None:
                self._metrics.record_event_consumed()
            callback(dict(payload))


class RabbitEventBroker:
    """RabbitMQ AMQP event broker supporting durable queues and manual acks."""

    def __init__(self, rabbit_url, metrics=None, fallback_broker=None):
        self.rabbit_url = rabbit_url
        self._metrics = metrics
        self._fallback = fallback_broker or InMemoryEventBroker(metrics=metrics)

    def subscribe(self, routing_key, callback):
        self._fallback.subscribe(routing_key, callback)

    def _open_channel(self):
        params = pika.URLParameters(self.rabbit_url)
        params.socket_timeout = 3
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.exchange_declare(
            exchange=EXCHANGE_NAME, exchange_type="topic", durable=True
        )
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.queue_bind(
            exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY
        )
        return connection, channel

    def publish(self, routing_key, payload):
        try:
            connection, channel = self._open_channel()
            try:
                body = json.dumps(payload).encode("utf-8")
                channel.basic_publish(
                    exchange=EXCHANGE_NAME,
                    routing_key=routing_key,
                    body=body,
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        delivery_mode=2,
                    ),
                )
                if self._metrics is not None:
                    self._metrics.record_event_published()
            finally:
                connection.close()
        except Exception as exc:
            logger.warning(
                "RabbitMQ unavailable (%s); falling back to in-process event dispatch.",
                exc,
            )
            self._fallback.publish(routing_key, payload)

    def consume_forever(self, handler):
        connection, channel = self._open_channel()
        channel.basic_qos(prefetch_count=1)

        def _on_message(ch, method, _properties, body):
            try:
                data = json.loads(body.decode("utf-8"))
                handler(data)
                if self._metrics is not None:
                    self._metrics.record_event_consumed()
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as exc:
                logger.error("Failed processing event %s: %s", body, exc)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        channel.basic_consume(
            queue=QUEUE_NAME, on_message_callback=_on_message, auto_ack=False
        )
        try:
            channel.start_consuming()
        finally:
            if connection.is_open:
                connection.close()


def build_event_broker(rabbit_url=None, metrics=None):
    resolved_url = (
        rabbit_url
        or os.environ.get("RABBIT_URL")
        or os.environ.get("CLOUDAMQP_URL")
    )
    in_memory = InMemoryEventBroker(metrics=metrics)
    if resolved_url:
        return RabbitEventBroker(
            resolved_url, metrics=metrics, fallback_broker=in_memory
        )
    return in_memory
