import argparse
import calendar
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

INPUT_DIR       = Path("data/raw/taxi")
DELTA_TABLE_URI = "s3://bronze/taxi/yellow_tripdata"

# MinIO connection — must match your docker-compose.yml
STORAGE_OPTIONS = {
    "endpoint_url"       : "http://localhost:9000",
    "aws_access_key_id"  : "minioadmin",
    "aws_secret_access_key": "minioadmin",
    "region_name"        : "us-east-1",
    "allow_http"         : "true",          # Required for non-HTTPS MinIO
    "aws_s3_allow_unsafe_rename": "true",   # Required for Delta Lake on MinIO
}

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------