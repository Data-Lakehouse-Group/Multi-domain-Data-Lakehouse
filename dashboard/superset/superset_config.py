import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "fallback-secret-change-me")
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

PREVENT_UNSAFE_DB_CONNECTIONS = False

# ---------------------------------------------------------------------------
# Timeout configs
# ---------------------------------------------------------------------------
SQLLAB_TIMEOUT                      = 300
SUPERSET_WEBSERVER_TIMEOUT          = 300
SQLLAB_ASYNC_TIME_LIMIT_SEC         = 300
SQL_MAX_ROW                         = 100_000   # max rows returned to Superset
ROW_LIMIT                           = 10_000    # default row limit for charts

# ---------------------------------------------------------------------------
# Feature flags
# ---------------------------------------------------------------------------
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING"    : True,     # allows {{ }} Jinja in SQL
    "DRILL_TO_DETAIL"               : True,     # click chart → see raw rows
    "DASHBOARD_CROSS_FILTERS"       : True,     # filters across charts
    "ENABLE_EXPLORE_DRAG_AND_DROP"  : True,
}

# ---------------------------------------------------------------------------
# Trino connection defaults
# ---------------------------------------------------------------------------
TRINO_HOSTNAME           = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT               = int(os.environ.get("TRINO_PORT", 8080))

# ---------------------------------------------------------------------------
# Cache — without this every chart re-queries Trino on every page load
# ---------------------------------------------------------------------------
CACHE_CONFIG = {
    "CACHE_TYPE"                : "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT"     : 60, #Change to 86400(24 hours) in prod          
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE"                : "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT"     : 60, #Change to 86400(24 hours) in prod 
}