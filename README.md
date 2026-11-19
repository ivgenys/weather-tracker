# Weather Tracker

Live app: https://weather-tracker-gxaw.onrender.com

Weather Tracker collects current weather readings for a few US cities from the
[Open-Meteo API](https://open-meteo.com/), stores them, and keeps a daily summary
per city (average, minimum and maximum temperature, average humidity, peak wind)
with a freeze, heat or high-wind alert. You can view the data in a web dashboard
or through a small REST API.

Course project for CSCA 5028, Applications of Software Architecture for Big Data.

## How it works

1. The **collector** (`src/collector.py`) asks Open-Meteo for the current reading
   of a city and saves it as a row in `weather_observations`.
2. It then publishes a `weather.collected` event.
3. The **analyzer** (`src/analyzer.py`) receives the event, recomputes that city's
   summary for that UTC day from the raw rows, and saves it in
   `daily_weather_summaries`.
4. The **web app** (`src/webapp.py`) serves the dashboard, the REST API, and the
   `/health` and `/metrics` endpoints.

Events go through RabbitMQ when `RABBIT_URL` is set. Without it they are handed
to the analyzer inside the same process, through the same publish/subscribe
interface (`src/messaging.py`).

## Where each rubric item is

| Rubric item | Where to look |
| :--- | :--- |
| Web application: form and reporting | `src/webapp.py`, `src/templates/dashboard.html` |
| Data collection | `src/collector.py` |
| Data analyzer | `src/analyzer.py` |
| Unit tests | `tests/test_unit.py` |
| Data persistence | `src/models.py`, `migrations/001_initial_schema.sql` |
| REST collaboration or API endpoint | `/api/observations`, `/api/summaries`, `/api/collect` in `src/webapp.py`; the collector calls the Open-Meteo REST API |
| Product environment | Render web service at the URL above, `render.yaml` |
| Integration tests | `tests/test_integration.py`, `tests/test_rabbitmq.py` |
| Mock objects or test doubles | `tests/test_doubles.py` (mock HTTP client, fake broker, spy broker) |
| Continuous integration | `.github/workflows/ci.yml` |
| Production monitoring | `src/monitoring.py`, `/health`, `/metrics`, `/api/metrics` |
| Event collaboration messaging | `src/messaging.py` |
| Continuous delivery | Render deploys `main`; `.github/workflows/cd.yml` checks the deployed commit |

## Run the tests

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
pytest -v
```

The tests use a temporary SQLite database by default. The two RabbitMQ tests are
skipped unless a broker is available. To run everything the way CI does, point
the tests at PostgreSQL and RabbitMQ:

```bash
TEST_DATABASE_URL=postgresql://weather:weather_password@localhost:5432/weather_tracker_test \
TEST_RABBIT_URL=amqp://guest:guest@localhost:5672/%2F \
pytest -v
```

## Run the app

Single process, SQLite, no broker needed:

```bash
gunicorn --chdir src --bind 0.0.0.0:8000 app:app
```

Gunicorn does not run on Windows. There, use `python src/app.py` instead.

Full stack with PostgreSQL, RabbitMQ, and the collector and analyzer as separate
processes:

```bash
docker compose up --build
```

Then open http://localhost:8000.

## API

| Method and path | What it does |
| :--- | :--- |
| `GET /api/observations?city=&limit=` | Latest raw readings |
| `POST /api/observations` | Store a reading: `city`, `temperature_c`, `humidity_pct`, `wind_speed_kmh`, optional `observed_at` |
| `GET /api/summaries?city=` | Daily summaries |
| `POST /api/collect` | Collect the current Open-Meteo reading for `city` |
| `GET /health` | Database check, deployed commit, which event broker is in use |
| `GET /metrics`, `GET /api/metrics` | Request and pipeline counters (Prometheus text, JSON) |

## What runs in production

The Render deployment is one free web service. It has no `DATABASE_URL` or
`RABBIT_URL` configured, so it uses SQLite on the instance disk and handles
events in-process. Render clears that disk on every restart or deploy, and the
app collects a first reading for three cities when it starts with an empty
database. There is no background worker on the free plan, so new readings are
collected when someone presses Collect or calls `POST /api/collect`.

Setting `DATABASE_URL` (PostgreSQL) and `RABBIT_URL` (RabbitMQ) on the service
switches both without code changes; `/health` shows which broker is active.
