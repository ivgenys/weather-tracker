#!/usr/bin/env python3
import os
import time
from datetime import datetime, timezone
from flask import Flask, Response, g, jsonify, redirect, render_template_string, request, url_for
from sqlalchemy import text

from analyzer import make_event_handler, recompute_all_summaries, recompute_city_summary
from collector import (
    CITY_COORDINATES,
    collect_city_weather,
    resolve_city_coordinates,
    store_observation_and_publish,
)
from messaging import ROUTING_KEY, build_event_broker
from models import DailyWeatherSummary, WeatherObservation, db, normalize_database_uri
from monitoring import MetricsRegistry, metrics_registry

DASHBOARD_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Weather Tracker - Distributed Telemetry &amp; Daily Analytics</title>
    <style>
        :root {
            --bg: #f8fafc;
            --surface: #ffffff;
            --ink: #0f172a;
            --muted: #475569;
            --border: #e2e8f0;
            --accent: #0284c7;
            --accent-hover: #0369a1;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: var(--bg);
            color: var(--ink);
            margin: 0;
            padding: 28px 20px 48px;
            line-height: 1.5;
        }
        .container { max-width: 1080px; margin: 0 auto; }
        header {
            display: flex;
            flex-wrap: wrap;
            justify-content: space-between;
            align-items: baseline;
            gap: 12px;
            margin-bottom: 20px;
            border-bottom: 1px solid var(--border);
            padding-bottom: 14px;
        }
        h1 { margin: 0; font-size: 1.65rem; }
        .subtitle { color: var(--muted); font-size: 0.95rem; margin: 4px 0 0; }
        .nav-links a {
            color: var(--accent);
            text-decoration: none;
            font-size: 0.9rem;
            margin-left: 14px;
            font-weight: 500;
        }
        .nav-links a:hover { text-decoration: underline; }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }
        .card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 18px 20px;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }
        .card h2 { margin: 0 0 10px; font-size: 1.1rem; }
        .card p { margin: 0 0 12px; color: var(--muted); font-size: 0.9rem; }
        form.inline-form { display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end; }
        label { display: block; font-size: 0.8rem; font-weight: 600; color: var(--muted); margin-bottom: 4px; }
        input, select {
            padding: 8px 10px;
            border: 1px solid #cbd5e1;
            border-radius: 6px;
            font-size: 0.92rem;
            background: #fff;
        }
        button {
            background: var(--accent);
            color: #fff;
            border: none;
            border-radius: 6px;
            padding: 9px 15px;
            font-size: 0.92rem;
            font-weight: 600;
            cursor: pointer;
        }
        button:hover { background: var(--accent-hover); }
        .filters { margin-bottom: 16px; display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
        .chip {
            padding: 5px 12px;
            border-radius: 999px;
            border: 1px solid var(--border);
            background: var(--surface);
            color: var(--ink);
            text-decoration: none;
            font-size: 0.85rem;
        }
        .chip.active { background: var(--accent); color: #fff; border-color: var(--accent); }
        table {
            width: 100%;
            border-collapse: collapse;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 10px;
            overflow: hidden;
            margin-bottom: 26px;
        }
        th, td { padding: 11px 14px; text-align: left; border-bottom: 1px solid var(--border); font-size: 0.92rem; }
        th { background: #f1f5f9; font-weight: 600; color: #334155; }
        .badge {
            display: inline-block;
            padding: 2px 9px;
            border-radius: 999px;
            font-size: 0.78rem;
            font-weight: 600;
        }
        .badge-NORMAL { background: #dcfce7; color: #166534; }
        .badge-FREEZE_WARNING { background: #dbeafe; color: #1e40af; }
        .badge-HIGH_WIND_ADVISORY { background: #fef3c7; color: #92400e; }
        .badge-HEAT_ADVISORY { background: #fee2e2; color: #991b1b; }
        .banner {
            background: #e0f2fe;
            border: 1px solid #bae6fd;
            color: #0c4a6e;
            padding: 10px 14px;
            border-radius: 8px;
            margin-bottom: 18px;
            font-size: 0.92rem;
        }
    </style>
</head>
<body>
<div class="container">
    <header>
        <div>
            <h1>Weather Tracker</h1>
            <p class="subtitle">Event-driven weather collection, daily statistical analysis, and operational telemetry</p>
        </div>
        <nav class="nav-links">
            <a href="/api/summaries">REST: /api/summaries</a>
            <a href="/api/observations">REST: /api/observations</a>
            <a href="/health">Health: /health</a>
            <a href="/metrics">Metrics: /metrics</a>
        </nav>
    </header>

    {% if message %}
    <div class="banner">{{ message }}</div>
    {% endif %}

    <div class="grid">
        <div class="card">
            <h2>Collect Live Weather (Open-Meteo API)</h2>
            <p>Fetches real-time reading, stores it in SQL, and publishes a <code>weather.collected</code> event to trigger the analyzer.</p>
            <form class="inline-form" action="/collect" method="POST">
                <div>
                    <label for="collect-city">City</label>
                    <select id="collect-city" name="city">
                        {% for c in cities %}
                        <option value="{{ c }}" {% if selected_city == c %}selected{% endif %}>{{ c }}</option>
                        {% endfor %}
                    </select>
                </div>
                <button type="submit">Fetch Live Reading</button>
            </form>
        </div>

        <div class="card">
            <h2>Record Custom Observation</h2>
            <p>Submit a weather sensor observation directly via form post to update daily analyzed statistics.</p>
            <form class="inline-form" action="/observations" method="POST">
                <div>
                    <label for="obs-city">City</label>
                    <input id="obs-city" name="city" value="{{ selected_city or 'Boulder' }}" size="11" required>
                </div>
                <div>
                    <label for="obs-temp">Temp (&deg;C)</label>
                    <input id="obs-temp" name="temperature_c" type="number" step="0.1" value="12.5" style="width:74px" required>
                </div>
                <div>
                    <label for="obs-hum">Humidity (%)</label>
                    <input id="obs-hum" name="humidity_pct" type="number" step="0.1" value="45" style="width:74px" required>
                </div>
                <div>
                    <label for="obs-wind">Wind (km/h)</label>
                    <input id="obs-wind" name="wind_speed_kmh" type="number" step="0.1" value="14.0" style="width:74px" required>
                </div>
                <button type="submit">Save Observation</button>
            </form>
        </div>
    </div>

    <div class="filters">
        <strong>Filter by city:</strong>
        <a class="chip {% if not selected_city %}active{% endif %}" href="/">All Cities</a>
        {% for c in cities %}
        <a class="chip {% if selected_city == c %}active{% endif %}" href="/?city={{ c }}">{{ c }}</a>
        {% endfor %}
    </div>

    <h2>Daily Analyzed Weather Summaries</h2>
    <table>
        <thead>
            <tr>
                <th>City</th>
                <th>Date (UTC)</th>
                <th>Avg Temp (&deg;C)</th>
                <th>Min / Max (&deg;C)</th>
                <th>Avg Humidity</th>
                <th>Peak Wind</th>
                <th>Samples</th>
                <th>Alert Status</th>
            </tr>
        </thead>
        <tbody>
            {% for s in summaries %}
            <tr>
                <td><strong>{{ s.city }}</strong></td>
                <td>{{ s.summary_date }}</td>
                <td>{{ "%.1f"|format(s.avg_temp_c) }}&deg;C</td>
                <td>{{ "%.1f"|format(s.min_temp_c) }}&deg;C / {{ "%.1f"|format(s.max_temp_c) }}&deg;C</td>
                <td>{{ "%.1f"|format(s.avg_humidity_pct) }}%</td>
                <td>{{ "%.1f"|format(s.max_wind_kmh) }} km/h</td>
                <td>{{ s.sample_count }}</td>
                <td><span class="badge badge-{{ s.alert_status }}">{{ s.alert_status }}</span></td>
            </tr>
            {% else %}
            <tr><td colspan="8">No daily summaries computed yet.</td></tr>
            {% endfor %}
        </tbody>
    </table>

    <h2>Recent Raw Weather Observations</h2>
    <table>
        <thead>
            <tr>
                <th>ID</th>
                <th>City</th>
                <th>Observed At (UTC)</th>
                <th>Temperature (&deg;C)</th>
                <th>Humidity (%)</th>
                <th>Wind Speed (km/h)</th>
                <th>Source</th>
            </tr>
        </thead>
        <tbody>
            {% for r in observations %}
            <tr>
                <td>{{ r.id }}</td>
                <td><strong>{{ r.city }}</strong></td>
                <td>{{ r.observed_at.strftime("%Y-%m-%d %H:%M:%S") }}</td>
                <td>{{ "%.1f"|format(r.temperature_c) }}&deg;C</td>
                <td>{{ "%.1f"|format(r.humidity_pct) }}%</td>
                <td>{{ "%.1f"|format(r.wind_speed_kmh) }} km/h</td>
                <td>{{ r.source }}</td>
            </tr>
            {% else %}
            <tr><td colspan="7">No observations stored yet.</td></tr>
            {% endfor %}
        </tbody>
    </table>
</div>
</body>
</html>
"""

DEFAULT_SEED_READINGS = [
    ("Boulder", -1.8, 54.0, 19.5),
    ("Boulder", 6.4, 41.0, 22.0),
    ("Denver", 8.2, 36.0, 16.4),
    ("Colorado Springs", 5.1, 44.0, 42.5),
]


def _seed_initial_data(app, broker, metrics):
    with app.app_context():
        if WeatherObservation.query.count() > 0:
            return
        seeded_any = False
        for city in ("Boulder", "Denver", "Colorado Springs"):
            try:
                collect_city_weather(city, broker=broker, metrics=metrics)
                seeded_any = True
            except Exception:
                continue
        if not seeded_any:
            for city, temp_c, hum_pct, wind_kmh in DEFAULT_SEED_READINGS:
                store_observation_and_publish(
                    city=city,
                    temperature_c=temp_c,
                    humidity_pct=hum_pct,
                    wind_speed_kmh=wind_kmh,
                    source="seed-fallback",
                    broker=broker,
                )
        recompute_all_summaries(metrics=metrics)


def create_app(config_overrides=None, broker=None, metrics=None, seed_on_boot=None):
    flask_app = Flask(__name__)
    flask_app.config["SQLALCHEMY_DATABASE_URI"] = normalize_database_uri(
        (config_overrides or {}).get("SQLALCHEMY_DATABASE_URI")
    )
    flask_app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    if config_overrides:
        flask_app.config.update(config_overrides)

    db.init_app(flask_app)

    active_metrics = metrics or metrics_registry
    active_broker = broker or build_event_broker(metrics=active_metrics)
    active_broker.subscribe(
        ROUTING_KEY, make_event_handler(flask_app, metrics=active_metrics)
    )

    flask_app.extensions["event_broker"] = active_broker
    flask_app.extensions["metrics"] = active_metrics

    with flask_app.app_context():
        db.create_all()

    should_seed = (
        seed_on_boot
        if seed_on_boot is not None
        else os.environ.get("SEED_ON_BOOT", "true").lower() == "true"
        and not flask_app.config.get("TESTING", False)
    )
    if should_seed:
        _seed_initial_data(flask_app, active_broker, active_metrics)

    @flask_app.before_request
    def _start_timer():
        g.request_started_at = time.perf_counter()

    @flask_app.after_request
    def _record_request_telemetry(response):
        started = getattr(g, "request_started_at", None)
        if started is not None and request.path not in ("/metrics", "/api/metrics"):
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            route_key = f"{request.method} {request.path}"
            active_metrics.record_http_request(
                route_key, response.status_code, elapsed_ms
            )
        return response

    @flask_app.route("/")
    def index():
        selected_city = request.args.get("city", "").strip()
        message = request.args.get("msg", "").strip()

        obs_query = WeatherObservation.query
        sum_query = DailyWeatherSummary.query
        if selected_city:
            obs_query = obs_query.filter_by(city=selected_city)
            sum_query = sum_query.filter_by(city=selected_city)

        observations = obs_query.order_by(
            WeatherObservation.observed_at.desc(), WeatherObservation.id.desc()
        ).limit(25).all()
        summaries = sum_query.order_by(
            DailyWeatherSummary.summary_date.desc(), DailyWeatherSummary.city.asc()
        ).all()

        return render_template_string(
            DASHBOARD_TEMPLATE,
            observations=observations,
            summaries=summaries,
            cities=list(CITY_COORDINATES.keys()),
            selected_city=selected_city,
            message=message,
        )

    @flask_app.route("/collect", methods=["POST"])
    def trigger_collect_form():
        city = request.form.get("city", "Boulder")
        canonical_city, _ = resolve_city_coordinates(city)
        try:
            obs = collect_city_weather(
                canonical_city, broker=active_broker, metrics=active_metrics
            )
            recompute_city_summary(
                obs.city, obs.observed_at.date(), metrics=active_metrics
            )
            msg = f"Collected live reading for {obs.city}: {obs.temperature_c:.1f} C"
        except Exception:
            obs = store_observation_and_publish(
                city=canonical_city,
                temperature_c=10.5,
                humidity_pct=42.0,
                wind_speed_kmh=15.0,
                source="fallback",
                broker=active_broker,
            )
            recompute_city_summary(
                obs.city, obs.observed_at.date(), metrics=active_metrics
            )
            msg = f"Recorded fallback reading for {obs.city}"
        return redirect(url_for("index", city=canonical_city, msg=msg))

    @flask_app.route("/observations", methods=["POST"])
    def submit_observation_form():
        city, _ = resolve_city_coordinates(request.form.get("city", "Boulder"))
        temperature_c = float(request.form.get("temperature_c", 0.0))
        humidity_pct = float(request.form.get("humidity_pct", 50.0))
        wind_speed_kmh = float(request.form.get("wind_speed_kmh", 10.0))

        obs = store_observation_and_publish(
            city=city,
            temperature_c=temperature_c,
            humidity_pct=humidity_pct,
            wind_speed_kmh=wind_speed_kmh,
            source="web-form",
            broker=active_broker,
        )
        recompute_city_summary(
            obs.city, obs.observed_at.date(), metrics=active_metrics
        )
        return redirect(
            url_for("index", city=city, msg=f"Saved observation for {city}")
        )

    @flask_app.route("/echo_user_input", methods=["POST"])
    def echo_input():
        input_text = request.form.get("user_input", "")
        return "You entered: " + input_text

    @flask_app.route("/api/observations", methods=["GET", "POST"])
    def api_observations():
        if request.method == "POST":
            payload = request.get_json(silent=True) or {}
            if "city" not in payload or "temperature_c" not in payload:
                return (
                    jsonify({"error": "Fields 'city' and 'temperature_c' are required"}),
                    400,
                )
            city, _ = resolve_city_coordinates(payload["city"])
            observed_at = None
            if payload.get("observed_at"):
                observed_at = datetime.fromisoformat(
                    payload["observed_at"].replace("Z", "+00:00")
                ).replace(tzinfo=None)
            record = store_observation_and_publish(
                city=city,
                temperature_c=float(payload["temperature_c"]),
                humidity_pct=float(payload.get("humidity_pct", 50.0)),
                wind_speed_kmh=float(payload.get("wind_speed_kmh", 10.0)),
                observed_at=observed_at,
                source=payload.get("source", "rest-api"),
                broker=active_broker,
            )
            summary = recompute_city_summary(
                record.city, record.observed_at.date(), metrics=active_metrics
            )
            return (
                jsonify(
                    {
                        "observation": record.to_dict(),
                        "daily_summary": summary.to_dict() if summary else None,
                    }
                ),
                201,
            )

        city = request.args.get("city", "").strip()
        limit = min(int(request.args.get("limit", 50)), 200)
        query = WeatherObservation.query
        if city:
            query = query.filter_by(city=city)
        rows = (
            query.order_by(
                WeatherObservation.observed_at.desc(), WeatherObservation.id.desc()
            )
            .limit(limit)
            .all()
        )
        return jsonify([r.to_dict() for r in rows]), 200

    @flask_app.route("/api/summaries", methods=["GET"])
    def api_summaries():
        city = request.args.get("city", "").strip()
        query = DailyWeatherSummary.query
        if city:
            query = query.filter_by(city=city)
        rows = query.order_by(
            DailyWeatherSummary.summary_date.desc(), DailyWeatherSummary.city.asc()
        ).all()
        return jsonify([s.to_dict() for s in rows]), 200

    @flask_app.route("/api/collect", methods=["POST"])
    def api_collect():
        payload = request.get_json(silent=True) or {}
        city = payload.get("city", "Boulder")
        obs = collect_city_weather(
            city, broker=active_broker, metrics=active_metrics
        )
        summary = recompute_city_summary(
            obs.city, obs.observed_at.date(), metrics=active_metrics
        )
        return (
            jsonify(
                {
                    "observation": obs.to_dict(),
                    "daily_summary": summary.to_dict() if summary else None,
                }
            ),
            201,
        )

    @flask_app.route("/health", methods=["GET"])
    def health():
        try:
            db.session.execute(text("SELECT 1"))
            return (
                jsonify(
                    {
                        "status": "UP",
                        "database": "connected",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                ),
                200,
            )
        except Exception as exc:
            return jsonify({"status": "DOWN", "error": str(exc)}), 503

    @flask_app.route("/metrics", methods=["GET"])
    def prometheus_metrics():
        if request.args.get("format") == "json":
            return jsonify(active_metrics.snapshot()), 200
        return Response(
            active_metrics.render_prometheus(),
            status=200,
            mimetype="text/plain; version=0.0.4; charset=utf-8",
        )

    @flask_app.route("/api/metrics", methods=["GET"])
    def json_metrics():
        return jsonify(active_metrics.snapshot()), 200

    return flask_app


if os.environ.get("SKIP_DEFAULT_APP_INIT", "").lower() != "true":
    app = create_app(
        metrics=MetricsRegistry() if os.environ.get("ISOLATED_METRICS") else None
    )
else:
    app = None

if __name__ == "__main__":
    live_app = app or create_app()
    port = int(os.environ.get("PORT", 8000))
    live_app.run(host="0.0.0.0", port=port)
