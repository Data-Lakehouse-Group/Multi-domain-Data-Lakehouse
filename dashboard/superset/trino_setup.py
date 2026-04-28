"""
Trino Lakehouse Setup
=====================
Replaces the DuckDB setup script.
Registers all Delta tables across bronze, silver and gold
so Superset can query them via Trino.

Run once on startup after Trino is healthy.
"""

from trino.dbapi import connect
import time
import sys

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

GOLD_TABLES = [
        # Weather
        ("weather_monthly_climate",  "s3://gold/weather/monthly_climate/"),
        ("weather_station_metrics",  "s3://gold/weather/station_metrics/"),
        ("weather_daily_climate",    "s3://gold/weather/daily_climate/"),

        # Taxi
        ("taxi_daily_summary",       "s3://gold/taxi/daily_summary/"),
        ("taxi_zone_performance",    "s3://gold/taxi/zone_performance/"),
        ("taxi_hourly_summary",      "s3://gold/taxi/hourly_summary/"),
        ("taxi_payment_summary",     "s3://gold/taxi/payment_summary/"),
        ("taxi_individual_day_summary",     "s3://gold/taxi/individual_day_summary/"),
    ]

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def get_trino_connection():
    return connect(
        host    = "trino",
        port    = 8080,
        user    = "admin",
        catalog = "lakehouse",
    )


def wait_for_trino(retries: int = 10, delay: int = 5):
    print("Waiting for Trino to be ready...")
    for attempt in range(retries):
        try:
            con = get_trino_connection()
            cur = con.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            print("✅ Trino is ready")
            return
        except Exception:
            print(f"  Attempt {attempt + 1}/{retries} — retrying in {delay}s")
            time.sleep(delay)
    print("❌ Trino did not become ready in time")
    sys.exit(1)


def setup_gold_tables(cur):
    for table_name, location in GOLD_TABLES:
        try:
            # Unregister first if it already exists
            cur.execute(f"""
                CALL lakehouse.system.unregister_table(
                    schema_name => 'gold',
                    table_name  => '{table_name}'
                )
            """)
        except Exception:
            pass  # table didn't exist yet, that's fine

        try:
            cur.execute(f"""
                CALL lakehouse.system.register_table(
                    schema_name    => 'gold',
                    table_name     => '{table_name}',
                    table_location => '{location}'
                )
            """)
            print(f"  ✅ {table_name}")
        except Exception as e:
            print(f"  ❌ {table_name} — {e}")
        


def main():
    #Wating For Trino To Be Up
    print("\nWaiting For Trino...")
    wait_for_trino()

    #Setting Up Trino Connection
    print("\nSetting Up Connection To Trino...")
    con = get_trino_connection()
    cur = con.cursor()

    #Loading the gold schemas
    print("\nLoading Schemas...")
    cur.execute(f"""
            CREATE SCHEMA IF NOT EXISTS lakehouse.gold
            WITH (location = 's3://gold/')
    """)
    print("Gold Schemas Created")

    #Get each gold table
    print("\nRegistering Gold tables...")
    setup_gold_tables(cur)
    print("Gold Tables Registered")

    print(f"\n{'=' * 55}")
    print(f"Trino Lakehouse Setup Complete")
    print(f"  Catalog  : lakehouse")
    print(f"  Schema   : gold")
    print(f"  Tables   : {len(GOLD_TABLES)}")
    print(f"  Superset : trino://admin@trino:8080/lakehouse")
    print(f"{'=' * 55}")


if __name__ == "__main__":
    main()