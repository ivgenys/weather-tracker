import os
from datetime import date, datetime, timezone
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


def normalize_database_uri(raw_uri=None):
    uri = raw_uri or os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_URI")
    if not uri:
        return "sqlite:///weather_tracker.db"
    if uri.startswith("postgres://"):
        return uri.replace("postgres://", "postgresql://", 1)
    return uri


class WeatherObservation(db.Model):
    __tablename__ = "weather_observations"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    city = db.Column(db.String(100), nullable=False, index=True)
    observed_at = db.Column(
        db.DateTime,
        nullable=False,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        index=True,
    )
    temperature_c = db.Column(db.Float, nullable=False)
    humidity_pct = db.Column(db.Float, nullable=False)
    wind_speed_kmh = db.Column(db.Float, nullable=False)
    source = db.Column(db.String(64), nullable=False, default="open-meteo")

    def to_dict(self):
        return {
            "id": self.id,
            "city": self.city,
            "observed_at": self.observed_at.isoformat() if self.observed_at else None,
            "temperature_c": round(self.temperature_c, 1),
            "humidity_pct": round(self.humidity_pct, 1),
            "wind_speed_kmh": round(self.wind_speed_kmh, 1),
            "source": self.source,
        }


class DailyWeatherSummary(db.Model):
    __tablename__ = "daily_weather_summaries"
    __table_args__ = (
        db.UniqueConstraint("city", "summary_date", name="uq_city_summary_date"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    city = db.Column(db.String(100), nullable=False, index=True)
    summary_date = db.Column(db.Date, nullable=False, default=date.today, index=True)
    avg_temp_c = db.Column(db.Float, nullable=False)
    min_temp_c = db.Column(db.Float, nullable=False)
    max_temp_c = db.Column(db.Float, nullable=False)
    avg_humidity_pct = db.Column(db.Float, nullable=False)
    max_wind_kmh = db.Column(db.Float, nullable=False)
    sample_count = db.Column(db.Integer, nullable=False)
    alert_status = db.Column(db.String(64), nullable=False, default="NORMAL")
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "city": self.city,
            "summary_date": self.summary_date.isoformat() if self.summary_date else None,
            "avg_temp_c": round(self.avg_temp_c, 1),
            "min_temp_c": round(self.min_temp_c, 1),
            "max_temp_c": round(self.max_temp_c, 1),
            "avg_humidity_pct": round(self.avg_humidity_pct, 1),
            "max_wind_kmh": round(self.max_wind_kmh, 1),
            "sample_count": self.sample_count,
            "alert_status": self.alert_status,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
