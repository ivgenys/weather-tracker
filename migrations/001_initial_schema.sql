-- Migration 001: Initial schema for Weather Tracker raw observations and daily analyzed summaries

CREATE TABLE IF NOT EXISTS weather_observations (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    temperature_c DOUBLE PRECISION NOT NULL,
    humidity_pct DOUBLE PRECISION NOT NULL,
    wind_speed_kmh DOUBLE PRECISION NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'open-meteo'
);

CREATE INDEX IF NOT EXISTS idx_observations_city_time
    ON weather_observations (city, observed_at DESC);

CREATE TABLE IF NOT EXISTS daily_weather_summaries (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    summary_date DATE NOT NULL,
    avg_temp_c DOUBLE PRECISION NOT NULL,
    min_temp_c DOUBLE PRECISION NOT NULL,
    max_temp_c DOUBLE PRECISION NOT NULL,
    avg_humidity_pct DOUBLE PRECISION NOT NULL,
    max_wind_kmh DOUBLE PRECISION NOT NULL,
    sample_count INTEGER NOT NULL,
    alert_status VARCHAR(64) NOT NULL DEFAULT 'NORMAL',
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT uq_city_summary_date UNIQUE (city, summary_date)
);

CREATE INDEX IF NOT EXISTS idx_summaries_date_city
    ON daily_weather_summaries (summary_date DESC, city);
