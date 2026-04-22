import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "fallback-secret-change-me")
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

PREVENT_UNSAFE_DB_CONNECTIONS = False
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}