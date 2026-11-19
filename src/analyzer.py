import argparse
import logging
import sys
from datetime import date, datetime, time, timezone

from sqlalchemy.exc import IntegrityError

from messaging import build_event_broker
from models import DailyWeatherSummary, WeatherObservation, db
from monitoring import metrics_registry

logger = logging.getLogger(__name__)

FREEZE_AT_OR_BELOW_C = 0.0
HEAT_AT_OR_ABOVE_C = 32.0
HIGH_WIND_AT_OR_ABOVE_KMH = 40.0


def classify_weather_alert(min_temp_c, max_temp_c, max_wind_kmh):
    """Returns one alert per day. Freeze wins over heat, heat wins over wind."""
    if min_temp_c <= FREEZE_AT_OR_BELOW_C:
        return "FREEZE_WARNING"
    if max_temp_c >= HEAT_AT_OR_ABOVE_C:
        return "HEAT_ADVISORY"
    if max_wind_kmh >= HIGH_WIND_AT_OR_ABOVE_KMH:
        return "HIGH_WIND_ADVISORY"
    return "NORMAL"


def compute_daily_metrics(observations):
    """Aggregates one city's readings for one day. No database or network access."""
    if not observations:
        return None

    temps = [float(o["temperature_c"]) for o in observations]
    humidities = [float(o["humidity_pct"]) for o in observations]
    winds = [float(o["wind_speed_kmh"]) for o in observations]

    min_temp = round(min(temps), 1)
    max_temp = round(max(temps), 1)
    max_wind = round(max(winds), 1)
    return {
        "avg_temp_c": round(sum(temps) / len(temps), 1),
        "min_temp_c": min_temp,
        "max_temp_c": max_temp,
        "avg_humidity_pct": round(sum(humidities) / len(humidities), 1),
        "max_wind_kmh": max_wind,
        "sample_count": len(observations),
        "alert_status": classify_weather_alert(min_temp, max_temp, max_wind),
    }


def _upsert_summary(city, summary_day, metrics_for_day):
    summary = DailyWeatherSummary.query.filter_by(
        city=city, summary_date=summary_day
    ).first()
    if summary is None:
        summary = DailyWeatherSummary(city=city, summary_date=summary_day)
        db.session.add(summary)
    for field, value in metrics_for_day.items():
        setattr(summary, field, value)
    summary.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    db.session.commit()
    return summary


def recompute_city_summary(city, target_date=None, metrics=None):
    """Rebuilds the summary row for (city, UTC day) from the raw observations.

    It always recomputes from scratch, so handling the same event twice gives
    the same row.
    """
    summary_day = target_date or datetime.now(timezone.utc).date()
    if isinstance(summary_day, str):
        summary_day = date.fromisoformat(summary_day)

    rows = WeatherObservation.query.filter(
        WeatherObservation.city == city,
        WeatherObservation.observed_at >= datetime.combine(summary_day, time.min),
        WeatherObservation.observed_at <= datetime.combine(summary_day, time.max),
    ).all()
    if not rows:
        return None

    metrics_for_day = compute_daily_metrics([row.to_dict() for row in rows])
    try:
        summary = _upsert_summary(city, summary_day, metrics_for_day)
    except IntegrityError:
        # Another worker inserted the same (city, day) first; update its row.
        db.session.rollback()
        summary = _upsert_summary(city, summary_day, metrics_for_day)

    (metrics or metrics_registry).record_analysis()
    return summary


def recompute_all_summaries(metrics=None):
    pairs = db.session.query(
        WeatherObservation.city, WeatherObservation.observed_at
    ).all()
    targets = sorted({(city, observed_at.date()) for city, observed_at in pairs})
    return [recompute_city_summary(city, day, metrics=metrics) for city, day in targets]


def make_event_handler(flask_app, metrics=None):
    """Builds the callback that runs when a weather.collected event arrives."""

    def handle_event(payload):
        city = payload.get("city")
        if not city:
            return
        with flask_app.app_context():
            summary = recompute_city_summary(city, payload.get("summary_date"), metrics=metrics)
            if summary is not None:
                logger.info(
                    "Updated %s %s: %d readings, %s",
                    summary.city, summary.summary_date, summary.sample_count, summary.alert_status,
                )

    return handle_event


if __name__ == "__main__":
    from webapp import create_app

    parser = argparse.ArgumentParser(description="Weather Tracker analyzer")
    parser.add_argument(
        "--worker",
        action="store_true",
        help="consume weather.collected events from RabbitMQ until stopped",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)
    application = create_app(seed_on_boot=False, embedded_analyzer=False)
    if args.worker:
        broker = build_event_broker(metrics=metrics_registry)
        if not hasattr(broker, "start_consumer_thread"):
            sys.exit("Set RABBIT_URL to run the analyzer as a queue worker.")
        logger.info("Waiting for weather.collected events")
        worker = broker.start_consumer_thread(
            make_event_handler(application, metrics_registry)
        )
        worker.join()
    else:
        with application.app_context():
            print(f"Recomputed {len(recompute_all_summaries())} daily summaries.")
