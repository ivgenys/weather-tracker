import os
from types import SimpleNamespace

import pytest

from messaging import InMemoryEventBroker
from models import db
from monitoring import MetricsRegistry
from webapp import create_app


@pytest.fixture()
def database_uri(tmp_path):
    """PostgreSQL when TEST_DATABASE_URL is set (CI), otherwise a temporary SQLite file."""
    return os.environ.get("TEST_DATABASE_URL") or f"sqlite:///{tmp_path / 'test.sqlite3'}"


@pytest.fixture()
def make_app(database_uri):
    """Builds an app on an empty database and cleans the database up afterwards."""
    apps = []

    def build(broker, metrics):
        app = create_app(
            config_overrides={"TESTING": True, "SQLALCHEMY_DATABASE_URI": database_uri},
            broker=broker,
            metrics=metrics,
        )
        with app.app_context():
            db.drop_all()
            db.create_all()
        apps.append(app)
        return app

    yield build

    for app in apps:
        with app.app_context():
            db.session.remove()
            db.drop_all()
            db.engine.dispose()


@pytest.fixture()
def env(make_app):
    """The app wired to an in-memory broker, plus a test client."""
    metrics = MetricsRegistry()
    broker = InMemoryEventBroker(metrics=metrics)
    app = make_app(broker, metrics)
    return SimpleNamespace(app=app, client=app.test_client(), broker=broker, metrics=metrics)
