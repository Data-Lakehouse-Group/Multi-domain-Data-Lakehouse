import duckdb

con = duckdb.connect("/app/superset_home/lakehouse.duckdb")

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

# Create views
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

con.close()
print("DuckDB setup complete — secret and views created.")