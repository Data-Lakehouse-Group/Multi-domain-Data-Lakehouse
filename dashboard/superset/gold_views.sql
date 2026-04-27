-- ============================================================
-- Trino Lakehouse Setup
-- Run in Superset SQL Lab against the Lakehouse (Trino) database
-- ============================================================

-- Step 1: Create gold schema
CREATE SCHEMA IF NOT EXISTS lakehouse.gold
WITH (location = 's3://gold/');

-- Step 2: Unregister existing tables (ignore errors if they don't exist)
CALL lakehouse.system.unregister_table(schema_name => 'gold', table_name => 'weather_monthly_climate');
CALL lakehouse.system.unregister_table(schema_name => 'gold', table_name => 'weather_station_metrics');
CALL lakehouse.system.unregister_table(schema_name => 'gold', table_name => 'weather_daily_climate');
CALL lakehouse.system.unregister_table(schema_name => 'gold', table_name => 'taxi_daily_summary');
CALL lakehouse.system.unregister_table(schema_name => 'gold', table_name => 'taxi_zone_performance');
CALL lakehouse.system.unregister_table(schema_name => 'gold', table_name => 'taxi_hourly_summary');
CALL lakehouse.system.unregister_table(schema_name => 'gold', table_name => 'taxi_payment_summary');
CALL lakehouse.system.unregister_table(schema_name => 'gold', table_name => 'taxi_borough_summary');
CALL lakehouse.system.unregister_table(schema_name => 'gold', table_name => 'taxi_individual_day_summary');

-- Step 3: Register all gold tables
CALL lakehouse.system.register_table(schema_name => 'gold', table_name => 'weather_monthly_climate',     table_location => 's3://gold/weather/monthly_climate/');
CALL lakehouse.system.register_table(schema_name => 'gold', table_name => 'weather_station_metrics',     table_location => 's3://gold/weather/station_metrics/');
CALL lakehouse.system.register_table(schema_name => 'gold', table_name => 'weather_daily_climate',       table_location => 's3://gold/weather/daily_climate/');
CALL lakehouse.system.register_table(schema_name => 'gold', table_name => 'taxi_daily_summary',          table_location => 's3://gold/taxi/daily_summary/');
CALL lakehouse.system.register_table(schema_name => 'gold', table_name => 'taxi_zone_performance',       table_location => 's3://gold/taxi/zone_performance/');
CALL lakehouse.system.register_table(schema_name => 'gold', table_name => 'taxi_hourly_summary',         table_location => 's3://gold/taxi/hourly_summary/');
CALL lakehouse.system.register_table(schema_name => 'gold', table_name => 'taxi_payment_summary',        table_location => 's3://gold/taxi/payment_summary/');
CALL lakehouse.system.register_table(schema_name => 'gold', table_name => 'taxi_borough_summary',        table_location => 's3://gold/taxi/borough_summary/');
CALL lakehouse.system.register_table(schema_name => 'gold', table_name => 'taxi_individual_day_summary', table_location => 's3://gold/taxi/individual_day_summary/');

-- Step 4: Verify all tables are registered
SELECT table_name, table_schema
FROM lakehouse.information_schema.tables
WHERE table_schema = 'gold'
ORDER BY table_name;