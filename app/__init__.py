from pathlib import Path

from flask import Flask
from sqlalchemy import inspect

from .models import db


def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    default_db = Path(app.instance_path) / 'igreel.db'
    app.config.from_mapping(
        SECRET_KEY='dev',
        SQLALCHEMY_DATABASE_URI=f"sqlite:///{default_db}",
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
    )

    if test_config:
        app.config.update(test_config)

    Path(app.instance_path).mkdir(parents=True, exist_ok=True)
    db.init_app(app)

    with app.app_context():
        from . import routes  # noqa: F401
        db.create_all()
        _ensure_schema_updates()

    from .routes import bp
    app.register_blueprint(bp)
    app.jinja_env.filters["compact_number"] = compact_number
    return app


def compact_number(value):
    if value in (None, ""):
        return "n/a"
    num = float(value)
    for suffix, threshold in (("B", 1_000_000_000), ("M", 1_000_000), ("K", 1_000)):
        if abs(num) >= threshold:
            trimmed = num / threshold
            return f"{trimmed:.1f}".rstrip("0").rstrip(".") + suffix
    return str(int(num))


def _ensure_schema_updates():
    inspector = inspect(db.engine)
    table_names = inspector.get_table_names()
    if "reel" not in table_names:
        return
    columns = {column["name"] for column in inspector.get_columns("reel")}
    if "thumbnail_url" not in columns:
        with db.engine.begin() as connection:
            connection.exec_driver_sql("ALTER TABLE reel ADD COLUMN thumbnail_url TEXT")
    if "video_url" not in columns:
        with db.engine.begin() as connection:
            connection.exec_driver_sql("ALTER TABLE reel ADD COLUMN video_url TEXT")
    if "hashtag_search_state" not in table_names:
        with db.engine.begin() as connection:
            connection.exec_driver_sql(
                """
                CREATE TABLE hashtag_search_state (
                    id INTEGER PRIMARY KEY,
                    hashtag VARCHAR(120) NOT NULL UNIQUE,
                    page INTEGER NOT NULL DEFAULT 1,
                    next_page INTEGER,
                    more_available BOOLEAN NOT NULL DEFAULT 0,
                    updated_at DATETIME NOT NULL
                )
                """
            )
