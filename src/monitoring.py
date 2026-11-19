import threading
import time


class MetricsRegistry:
    """Request and pipeline counters behind /metrics. Safe to update from several threads."""

    def __init__(self):
        self._lock = threading.Lock()
        self._started_at = time.monotonic()
        self.http_requests_total = 0
        self.http_errors_total = 0
        self.route_counts = {}
        self._latency_sum_ms = 0.0
        self._latency_max_ms = 0.0
        self.collector_runs_total = 0
        self.collector_errors_total = 0
        self.analyzer_runs_total = 0
        self.events_published_total = 0
        self.events_consumed_total = 0

    def record_http_request(self, route_key, status_code, duration_ms):
        with self._lock:
            self.http_requests_total += 1
            self.route_counts[route_key] = self.route_counts.get(route_key, 0) + 1
            if status_code >= 400:
                self.http_errors_total += 1
            self._latency_sum_ms += float(duration_ms)
            if duration_ms > self._latency_max_ms:
                self._latency_max_ms = float(duration_ms)

    def record_collection(self, success=True):
        with self._lock:
            self.collector_runs_total += 1
            if not success:
                self.collector_errors_total += 1

    def record_analysis(self):
        with self._lock:
            self.analyzer_runs_total += 1

    def record_event_published(self):
        with self._lock:
            self.events_published_total += 1

    def record_event_consumed(self):
        with self._lock:
            self.events_consumed_total += 1

    def snapshot(self):
        with self._lock:
            avg_ms = (
                self._latency_sum_ms / self.http_requests_total
                if self.http_requests_total > 0
                else 0.0
            )
            return {
                "uptime_seconds": round(time.monotonic() - self._started_at, 2),
                "http_requests_total": self.http_requests_total,
                "http_errors_total": self.http_errors_total,
                "http_request_duration_ms_avg": round(avg_ms, 2),
                "http_request_duration_ms_max": round(self._latency_max_ms, 2),
                "collector_runs_total": self.collector_runs_total,
                "collector_errors_total": self.collector_errors_total,
                "analyzer_runs_total": self.analyzer_runs_total,
                "events_published_total": self.events_published_total,
                "events_consumed_total": self.events_consumed_total,
                "routes": dict(self.route_counts),
            }

    def render_prometheus(self):
        snap = self.snapshot()
        lines = [
            "# HELP weather_tracker_uptime_seconds Process uptime in seconds.",
            "# TYPE weather_tracker_uptime_seconds gauge",
            f"weather_tracker_uptime_seconds {snap['uptime_seconds']}",
            "# HELP weather_tracker_http_requests_total Total HTTP requests handled.",
            "# TYPE weather_tracker_http_requests_total counter",
            f"weather_tracker_http_requests_total {snap['http_requests_total']}",
            "# HELP weather_tracker_http_errors_total Total HTTP error responses (4xx/5xx).",
            "# TYPE weather_tracker_http_errors_total counter",
            f"weather_tracker_http_errors_total {snap['http_errors_total']}",
            "# HELP weather_tracker_http_latency_avg_ms Mean HTTP request latency in milliseconds.",
            "# TYPE weather_tracker_http_latency_avg_ms gauge",
            f"weather_tracker_http_latency_avg_ms {snap['http_request_duration_ms_avg']}",
            "# HELP weather_tracker_collector_runs_total Total Open-Meteo collection runs.",
            "# TYPE weather_tracker_collector_runs_total counter",
            f"weather_tracker_collector_runs_total {snap['collector_runs_total']}",
            "# HELP weather_tracker_analyzer_runs_total Total daily summary analyzer executions.",
            "# TYPE weather_tracker_analyzer_runs_total counter",
            f"weather_tracker_analyzer_runs_total {snap['analyzer_runs_total']}",
            "# HELP weather_tracker_events_published_total Total domain events published.",
            "# TYPE weather_tracker_events_published_total counter",
            f"weather_tracker_events_published_total {snap['events_published_total']}",
            "# HELP weather_tracker_events_consumed_total Total domain events consumed.",
            "# TYPE weather_tracker_events_consumed_total counter",
            f"weather_tracker_events_consumed_total {snap['events_consumed_total']}",
        ]
        return "\n".join(lines) + "\n"


metrics_registry = MetricsRegistry()
