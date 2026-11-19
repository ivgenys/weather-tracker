import logging
import os
import threading
import time
from datetime import datetime, timezone

from flask import Flask, Response, g, jsonify, redirect, render_template, request, url_for
from sqlalchemy import text

from analyzer import make_event_handler
from collector import (
    CITY_COORDINATES,
    UnknownCityError,
    canonical_city_name,
    collect_city_weather,
    parse_observed_timestamp,
    store_observation_and_publish,
)
from messaging import ROUTING_KEY, build_event_broker
from models import DailyWeatherSummary, WeatherObservation, db, normalize_database_uri
from monitoring import metrics_registry

logger = logging.getLogger(__name__)

SEED_CITIES = ("Boulder", "Denver", "Colorado Springs")

# Sanity bounds for hand-entered readings: (field, lowest, highest).
READING_BOUNDS = (
    ("temperature_c", -90.0, 60.0),
    ("humidity_pct", 0.0, 100.0),
    ("wind_speed_kmh", 0.0, 450.0),
)


def parse_reading(fields):
    """Validates a submitted reading. Returns (values, error_message)."""
    city = canonical_city_name(fields.get("city"))
    if not city:
        return None, "city is required"
    values = {"city": city}
    for name, lowest, highest in READING_BOUNDS:
        raw = fields.get(name)
        if raw is None or raw == "":
            return None, f"{name} is required"
        try:
            number = float(raw)
        except (TypeError, ValueError):
            return None, f"{name} must be a number"
        if not lowest <= number <= highest:
            return None, f"{name} must be between {lowest:g} and {highest:g}"
        values[name] = number
    return values, None


def _seed_if_empty(app, broker, metrics):
    """Collects a first reading for a few cities so a fresh deploy is not blank."""
    with app.app_context():
        if WeatherObservation.query.first() is not None:
            return
        for city in SEED_CITIES:
            try:
                collect_city_weather(city, broker=broker, metrics=metrics)
            except Exception as exc:
                logger.warning("Could not seed %s from Open-Meteo: %s", city, exc)


def create_app(config_overrides=None, broker=None, metrics=None, seed_on_boot=None,
               embedded_analyzer=None):
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = normalize_database_uri()
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config.update(config_overrides or {})
    db.init_app(app)

    metrics = metrics or metrics_registry
    broker = broker or build_event_broker(metrics=metrics)
    analyze = make_event_handler(app, metrics=metrics)
    broker.subscribe(ROUTING_KEY, analyze)
    app.extensions["event_broker"] = broker
    app.extensions["metrics"] = metrics

    with app.app_context():
        db.create_all()

    # With RabbitMQ and no separate analyzer process (the single free Render
    # service), consume the queue from a background thread in this process.
    if embedded_analyzer is None:
        embedded_analyzer = (
            os.environ.get("EMBEDDED_ANALYZER", "true").lower() == "true"
            and not app.config.get("TESTING")
        )
    if embedded_analyzer and hasattr(broker, "start_consumer_thread"):
        broker.start_consumer_thread(analyze)

    if seed_on_boot is None:
        seed_on_boot = (
            os.environ.get("SEED_ON_BOOT", "true").lower() == "true"
            and not app.config.get("TESTING")
        )
    if seed_on_boot:
        # In the background, so a slow Open-Meteo cannot delay startup.
        threading.Thread(
            target=_seed_if_empty, args=(app, broker, metrics), name="seed", daemon=True
        ).start()

    @app.before_request
    def start_timer():
        g.started_at = time.perf_counter()

    @app.after_request
    def record_request(response):
        if request.path not in ("/metrics", "/api/metrics"):
            elapsed_ms = (time.perf_counter() - g.started_at) * 1000.0
            metrics.record_http_request(
                f"{request.method} {request.path}", response.status_code, elapsed_ms
            )
        return response

    @app.route("/")
    def dashboard():
        city = canonical_city_name(request.args.get("city"))
        observations = WeatherObservation.query
        summaries = DailyWeatherSummary.query
        if city:
            observations = observations.filter_by(city=city)
            summaries = summaries.filter_by(city=city)
        return render_template(
            "dashboard.html",
            observations=observations.order_by(
                WeatherObservation.observed_at.desc(), WeatherObservation.id.desc()
            ).limit(25).all(),
            summaries=summaries.order_by(
                DailyWeatherSummary.summary_date.desc(), DailyWeatherSummary.city
            ).all(),
            cities=list(CITY_COORDINATES),
            selected_city=city,
            message=request.args.get("msg", ""),
            error=request.args.get("error", ""),
        )

    @app.route("/collect", methods=["POST"])
    def collect_form():
        city = canonical_city_name(request.form.get("city"))
        try:
            observation, created = collect_city_weather(city, broker=broker, metrics=metrics)
        except UnknownCityError as exc:
            return redirect(url_for("dashboard", error=str(exc)))
        except Exception as exc:
            logger.warning("Open-Meteo collection failed for %s: %s", city, exc)
            return redirect(
                url_for("dashboard", city=city, error="Could not reach Open-Meteo. Try again shortly.")
            )
        if created:
            msg = f"Collected {observation.city}: {observation.temperature_c:.1f} C"
        else:
            msg = (
                f"{observation.city} already has the latest Open-Meteo reading "
                f"({observation.observed_at:%H:%M} UTC); it updates every 15 minutes."
            )
        return redirect(url_for("dashboard", city=observation.city, msg=msg))

    @app.route("/observations", methods=["POST"])
    def observation_form():
        values, error = parse_reading(request.form)
        if error:
            return redirect(url_for("dashboard", error=error))
        store_observation_and_publish(source="web-form", broker=broker, **values)
        return redirect(
            url_for("dashboard", city=values["city"], msg=f"Saved reading for {values['city']}")
        )

    @app.route("/api/observations", methods=["GET"])
    def list_observations():
        try:
            limit = max(1, min(int(request.args.get("limit", 50)), 200))
        except ValueError:
            return jsonify({"error": "limit must be an integer"}), 400
        query = WeatherObservation.query
        city = canonical_city_name(request.args.get("city"))
        if city:
            query = query.filter_by(city=city)
        rows = query.order_by(
            WeatherObservation.observed_at.desc(), WeatherObservation.id.desc()
        ).limit(limit).all()
        return jsonify([row.to_dict() for row in rows])

    @app.route("/api/observations", methods=["POST"])
    def create_observation():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify({"error": "request body must be a JSON object"}), 400
        values, error = parse_reading(payload)
        if error:
            return jsonify({"error": error}), 400
        observed_at = None
        if payload.get("observed_at"):
            try:
                observed_at = parse_observed_timestamp(str(payload["observed_at"]))
            except ValueError:
                return jsonify({"error": "observed_at must be an ISO 8601 timestamp"}), 400
        record = store_observation_and_publish(
            observed_at=observed_at, source="rest-api", broker=broker, **values
        )
        return jsonify(record.to_dict()), 201

    @app.route("/api/summaries", methods=["GET"])
    def list_summaries():
        query = DailyWeatherSummary.query
        city = canonical_city_name(request.args.get("city"))
        if city:
            query = query.filter_by(city=city)
        rows = query.order_by(
            DailyWeatherSummary.summary_date.desc(), DailyWeatherSummary.city
        ).all()
        return jsonify([row.to_dict() for row in rows])

    @app.route("/api/collect", methods=["POST"])
    def collect_api():
        payload = request.get_json(silent=True) or {}
        try:
            observation, created = collect_city_weather(
                payload.get("city"), broker=broker, metrics=metrics
            )
        except UnknownCityError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            logger.warning("Open-Meteo collection failed: %s", exc)
            return jsonify({"error": "Open-Meteo request failed"}), 502
        return jsonify(observation.to_dict()), 201 if created else 200

    @app.route("/health", methods=["GET"])
    def health():
        body = {
            "version": os.environ.get("RENDER_GIT_COMMIT", "dev"),
            "event_broker": "rabbitmq" if hasattr(broker, "consume") else "in-process",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        try:
            db.session.execute(text("SELECT 1"))
        except Exception as exc:
            logger.error("Health check could not reach the database: %s", exc)
            return jsonify({"status": "DOWN", "database": "unreachable", **body}), 503
        return jsonify({"status": "UP", "database": "connected", **body})

    @app.route("/metrics", methods=["GET"])
    def prometheus_metrics():
        return Response(
            metrics.render_prometheus(),
            mimetype="text/plain; version=0.0.4; charset=utf-8",
        )

    @app.route("/api/metrics", methods=["GET"])
    def json_metrics():
        return jsonify(metrics.snapshot())

    return app
