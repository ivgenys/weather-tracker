# Weather Tracker

**Live Production Deployment:** https://weather-tracker-gxaw.onrender.com

Weather Tracker is an event-driven web application, data collection pipeline, and statistical weather analyzer built for *CSCA 5028: Applications of Software Architecture for Big Data*. It ingests real-time meteorological observations from the public [Open-Meteo REST API](https://open-meteo.com/), persists raw readings and daily pre-aggregated summaries in a relational SQL database (PostgreSQL in distributed deployments, SQLite for zero-config local runs), coordinates asynchronous analysis over RabbitMQ (`weather.events` topic exchange), and exposes both a browser reporting dashboard and REST/Prometheus operational endpoints.

---

## Architecture & A-Level Rubric Coverage

| Rubric Requirement | Implementation in Repository |
| :--- | :--- |
| **1. Web application (basic form, reporting)** | `src/app.py` (`GET /` dashboard with city filter, live Open-Meteo fetch form `POST /collect`, custom observation form `POST /observations`, and daily summary + raw observation tables) |
| **2. Data collection** | `src/collector.py` (`fetch_open_meteo_reading`, `collect_city_weather`, standalone CLI worker) |
| **3. Data analyzer** | `src/analyzer.py` (`compute_daily_metrics`, `recompute_city_summary`, freeze/wind/heat alert classification, standalone `--worker` consumer) |
| **4. Unit tests** | `tests/test_unit.py` (7 isolated unit tests covering aggregation math, alert thresholds, URI normalization, timestamp parsing, and metrics registry) |
| **5. Data persistence** | `src/models.py` (`WeatherObservation`, `DailyWeatherSummary` via SQLAlchemy) + `migrations/001_initial_schema.sql` |
| **6. REST collaboration / API endpoints** | `GET /api/observations`, `POST /api/observations`, `GET /api/summaries`, `POST /api/collect` |
| **7. Production environment** | Hosted on Render (`https://weather-tracker-gxaw.onrender.com`) via Gunicorn (`Procfile`, `render.yaml`, `Dockerfile`, `docker-compose.yml`) |
| **8. Integration tests** | `tests/test_integration.py` (5 end-to-end HTTP + SQL database + event bus integration tests) |
| **9. Mock objects, fakes, and spies** | `tests/test_doubles.py` (`MagicMock` for Open-Meteo HTTP client, `InMemoryEventBroker` Fake, `SpyEventBroker` Spy, and `patch` simulating RabbitMQ failure) |
| **10. Continuous integration** | `.github/workflows/ci.yml` (GitHub Actions workflow with PostgreSQL + RabbitMQ services running `pytest -v` on every push/PR) |
| **11. Production monitoring / instrumenting** | `src/monitoring.py` + `GET /health` (DB liveness check), `GET /metrics` (Prometheus exposition), and `GET /api/metrics` (JSON telemetry) |
| **12. Event collaboration / messaging** | `src/messaging.py` (`RabbitEventBroker` using `pika` durable queues and manual `basic_ack`, with automatic `InMemoryEventBroker` fallback) |
| **13. Continuous delivery** | `render.yaml` (`autoDeploy: true` on `main`) + `.github/workflows/cd.yml` post-CI deploy hook & live health smoke verification |

---

## Local Quick Start

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run automated unit, integration, and mock/fake/spy test suites
pytest -v

# Run standalone data collector (optional)
python src/collector.py

# Start web server locally on http://localhost:8000
gunicorn --chdir src --bind 0.0.0.0:8000 app:app
```

To run the full multi-container stack (PostgreSQL 15 + RabbitMQ 3.12 + Web + Collector + Analyzer worker):

```bash
docker compose up --build
```
