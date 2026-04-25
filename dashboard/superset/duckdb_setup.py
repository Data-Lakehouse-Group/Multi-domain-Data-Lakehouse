import duckdb

con = duckdb.connect("/app/superset_home/lakehouse.duckdb")

# Check if httpfs is already installed
is_installed = con.execute("""
    SELECT installed 
    FROM duckdb_extensions() 
    WHERE extension_name = 'httpfs'
""").fetchone()[0]

# Only run install if it isn't there
if not is_installed:
    con.execute("INSTALL httpfs;")
    
con.execute("LOAD httpfs;")

# Create persistent secret — stored inside lakehouse.duckdb permanently
con.execute("""
    CREATE PERSISTENT SECRET IF NOT EXISTS minio_secret (
        TYPE S3,
        KEY_ID 'minioadmin',
        SECRET 'minioadmin',
        ENDPOINT 'minio:9000',
        URL_STYLE 'path',
        USE_SSL false,
        REGION 'us-east-1'
    );
""")

# ------------------------------------------------------------
# Gold views for Superset dashboards
# ------------------------------------------------------------

# ----- Taxi -----
con.execute("""
    CREATE OR REPLACE VIEW taxi_daily_summary AS
    SELECT * FROM read_parquet('s3://gold/taxi/yellow_tripdata/daily_summary/**/*.parquet');
""")

con.execute("""
    CREATE OR REPLACE VIEW taxi_individual_day_summary AS
    SELECT * FROM read_parquet('s3://gold/taxi/yellow_tripdata/individual_day_summary/**/*.parquet');
""")

con.execute("""
    CREATE OR REPLACE VIEW taxi_zone_performance AS
    SELECT * FROM read_parquet('s3://gold/taxi/yellow_tripdata/zone_performance/**/*.parquet');
""")

con.execute("""
    CREATE OR REPLACE VIEW taxi_hourly_summary AS
    SELECT * FROM read_parquet('s3://gold/taxi/yellow_tripdata/hourly_summary/**/*.parquet');
""")

con.execute("""
    CREATE OR REPLACE VIEW taxi_payment_summary AS
    SELECT * FROM read_parquet('s3://gold/taxi/yellow_tripdata/payment_summary/**/*.parquet');
""")

con.execute("""
    CREATE OR REPLACE VIEW taxi_borough_summary AS
    SELECT * FROM read_parquet('s3://gold/taxi/yellow_tripdata/borough_summary/**/*.parquet');
""")

# ----- GitHub Archive -----
con.execute("""
    CREATE OR REPLACE VIEW github_event_summary_hour AS
    SELECT * FROM read_parquet('s3://gold/github_archive/event_summary_hour/**/*.parquet');
""")

con.execute("""
    CREATE OR REPLACE VIEW github_top_repos AS
    SELECT * FROM read_parquet('s3://gold/github_archive/top_repos/**/*.parquet');
""")

con.execute("""
    CREATE OR REPLACE VIEW github_daily_activity AS
    SELECT * FROM read_parquet('s3://gold/github_archive/daily_activity/**/*.parquet');
""")

# ----- Instacart Retail -----
con.execute("""
    CREATE OR REPLACE VIEW retail_department_performance AS
    SELECT * FROM read_parquet('s3://gold/retail/department_performance/**/*.parquet');
""")

con.execute("""
    CREATE OR REPLACE VIEW retail_aisle_popularity AS
    SELECT * FROM read_parquet('s3://gold/retail/aisle_popularity/**/*.parquet');
""")

con.execute("""
    CREATE OR REPLACE VIEW retail_product_reorder_analysis AS
    SELECT * FROM read_parquet('s3://gold/retail/product_reorder_analysis/**/*.parquet');
""")

# ----- NOAA Weather -----
con.execute("""
    CREATE OR REPLACE VIEW weather_monthly_climate AS
    SELECT * FROM read_parquet('s3://gold/weather/monthly_climate/**/*.parquet');
""")

con.execute("""
    CREATE OR REPLACE VIEW weather_station_metrics AS
    SELECT * FROM read_parquet('s3://gold/weather/station_metrics/**/*.parquet');
""")

con.execute("""
    CREATE OR REPLACE VIEW weather_daily_extremes AS
    SELECT * FROM read_parquet('s3://gold/weather/daily_extremes/**/*.parquet');
""")

con.close()
print("DuckDB setup complete — secret and all views created.")