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
    if "media_type" not in columns:
        with db.engine.begin() as connection:
            connection.exec_driver_sql("ALTER TABLE reel ADD COLUMN media_type VARCHAR(20) DEFAULT 'video'")
    if "carousel_json" not in columns:
        with db.engine.begin() as connection:
            connection.exec_driver_sql("ALTER TABLE reel ADD COLUMN carousel_json TEXT")
    if "local_thumb_path" not in columns:
        with db.engine.begin() as connection:
            connection.exec_driver_sql("ALTER TABLE reel ADD COLUMN local_thumb_path VARCHAR(500)")
    if "local_video_path" not in columns:
        with db.engine.begin() as connection:
            connection.exec_driver_sql("ALTER TABLE reel ADD COLUMN local_video_path VARCHAR(500)")
    if "discovery_page" not in columns:
        with db.engine.begin() as connection:
            connection.exec_driver_sql("ALTER TABLE reel ADD COLUMN discovery_page INTEGER")
    if "hashtag_search_state" not in table_names:
        with db.engine.begin() as connection:
            connection.exec_driver_sql(
                """
                CREATE TABLE hashtag_search_state (
                    id INTEGER PRIMARY KEY,
                    hashtag VARCHAR(120) NOT NULL UNIQUE,
                    page INTEGER NOT NULL DEFAULT 1,
                    next_page INTEGER,
                    next_max_id TEXT,
                    more_available BOOLEAN NOT NULL DEFAULT 0,
                    updated_at DATETIME NOT NULL
                )
                """
            )
    else:
        columns = {column["name"] for column in inspector.get_columns("hashtag_search_state")}
        if "next_max_id" not in columns:
            with db.engine.begin() as connection:
                connection.exec_driver_sql("ALTER TABLE hashtag_search_state ADD COLUMN next_max_id TEXT")

    if "task_notification" not in table_names:
        with db.engine.begin() as connection:
            connection.exec_driver_sql(
                """
                CREATE TABLE task_notification (
                    id INTEGER PRIMARY KEY,
                    message VARCHAR(255) NOT NULL,
                    action_url VARCHAR(500),
                    is_read BOOLEAN NOT NULL DEFAULT 0,
                    created_at DATETIME NOT NULL
                )
                """
            )

    if "creator_stats" not in table_names:
        with db.engine.begin() as connection:
            connection.exec_driver_sql(
                """
                CREATE TABLE creator_stats (
                    id INTEGER PRIMARY KEY,
                    username VARCHAR(120) NOT NULL UNIQUE,
                    full_name VARCHAR(255),
                    profile_pic_url TEXT,
                    biography TEXT,
                    external_url VARCHAR(500),
                    followers_count INTEGER,
                    following_count INTEGER,
                    posts_count INTEGER,
                    is_verified BOOLEAN DEFAULT 0,
                    updated_at DATETIME NOT NULL
                )
                """
            )

    # Update hashtag_search_state table
    columns = {column["name"] for column in inspector.get_columns("hashtag_search_state")}
    if "status" not in columns:
        with db.engine.begin() as connection:
            connection.exec_driver_sql("ALTER TABLE hashtag_search_state ADD COLUMN status VARCHAR(20) DEFAULT 'ready'")
    if "last_error" not in columns:
        with db.engine.begin() as connection:
            connection.exec_driver_sql("ALTER TABLE hashtag_search_state ADD COLUMN last_error TEXT")

