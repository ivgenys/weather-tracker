import argparse
from datetime import date, datetime, timezone
from models import DailyWeatherSummary, WeatherObservation, db
from messaging import build_event_broker
from monitoring import metrics_registry


def classify_weather_alert(min_temp_c, max_temp_c, max_wind_kmh):
    if min_temp_c <= 0.0:
        return "FREEZE_WARNING"
    if max_temp_c >= 32.0:
        return "HEAT_ADVISORY"
    if max_wind_kmh >= 40.0:
        return "HIGH_WIND_ADVISORY"
    return "NORMAL"


def compute_daily_metrics(observations):
    """Pure aggregation function computing daily statistics and weather alerts."""
    if not observations:
        return None

    temps = [float(item["temperature_c"]) for item in observations]
    humidities = [float(item["humidity_pct"]) for item in observations]
    winds = [float(item["wind_speed_kmh"]) for item in observations]

    avg_temp = round(sum(temps) / len(temps), 1)
    min_temp = round(min(temps), 1)
    max_temp = round(max(temps), 1)
    avg_humidity = round(sum(humidities) / len(humidities), 1)
    max_wind = round(max(winds), 1)

    return {
        "avg_temp_c": avg_temp,
        "min_temp_c": min_temp,
        "max_temp_c": max_temp,
        "avg_humidity_pct": avg_humidity,
        "max_wind_kmh": max_wind,
        "sample_count": len(observations),
        "alert_status": classify_weather_alert(min_temp, max_temp, max_wind),
    }


def recompute_city_summary(city, target_date=None, metrics=None):
    summary_day = target_date or date.today()
    if isinstance(summary_day, str):
        summary_day = date.fromisoformat(summary_day)

    day_start = datetime(summary_day.year, summary_day.month, summary_day.day, 0, 0, 0)
    day_end = datetime(summary_day.year, summary_day.month, summary_day.day, 23, 59, 59)

    rows = (
        WeatherObservation.query.filter(
            WeatherObservation.city == city,
            WeatherObservation.observed_at >= day_start,
            WeatherObservation.observed_at <= day_end,
        )
        .order_by(WeatherObservation.observed_at.asc())
        .all()
    )
    if not rows:
        return None

    computed = compute_daily_metrics([row.to_dict() for row in rows])
    summary = DailyWeatherSummary.query.filter_by(
        city=city, summary_date=summary_day
    ).first()
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)

    if summary is None:
        summary = DailyWeatherSummary(
            city=city,
            summary_date=summary_day,
            **computed,
            updated_at=now_utc,
        )
        db.session.add(summary)
    else:
        for field, value in computed.items():
            setattr(summary, field, value)
        summary.updated_at = now_utc

    db.session.commit()
    active_metrics = metrics or metrics_registry
    active_metrics.record_analysis()
    return summary


def recompute_all_summaries(metrics=None):
    pairs = (
        db.session.query(WeatherObservation.city, WeatherObservation.observed_at)
        .all()
    )
    distinct_targets = {(city, ts.date()) for city, ts in pairs if ts is not None}
    results = []
    for city, summary_day in sorted(distinct_targets):
        updated = recompute_city_summary(city, summary_day, metrics=metrics)
        if updated is not None:
            results.append(updated)
    return results


def make_event_handler(flask_app, metrics=None):
    def _handle_event(payload):
        city = payload.get("city")
        observed_date = payload.get("summary_date")
        if not city:
            return
        with flask_app.app_context():
            recompute_city_summary(city, observed_date, metrics=metrics)

    return _handle_event


if __name__ == "__main__":
    from app import create_app

    parser = argparse.ArgumentParser(description="Weather Tracker Data Analyzer")
    parser.add_argument(
        "--worker",
        action="store_true",
        help="Run continuous RabbitMQ consumer worker loop.",
    )
    args = parser.parse_args()

    application = create_app(seed_on_boot=False)
    if args.worker:
        broker = build_event_broker(metrics=metrics_registry)
        if hasattr(broker, "consume_forever"):
            broker.consume_forever(make_event_handler(application, metrics_registry))
    else:
        with application.app_context():
            summaries = recompute_all_summaries(metrics=metrics_registry)
            print(f"Recomputed {len(summaries)} daily weather summaries.")
