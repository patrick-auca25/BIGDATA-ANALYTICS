CREATE MATERIALIZED VIEW energy_readings_15min
WITH (timescaledb.continuous) AS
SELECT 
    meter_id,
    time_bucket('15 minutes', timestamp) AS bucket,
    AVG(power) as avg_power,
    MAX(power) as max_power,
    MIN(power) as min_power,
    SUM(energy) as total_energy,
    COUNT(*) as reading_count
FROM energy_readings
GROUP BY meter_id, bucket;







CREATE MATERIALIZED VIEW energy_readings_hourly
WITH (timescaledb.continuous) AS
SELECT 
    meter_id,
    time_bucket('1 hour', timestamp) AS bucket,
    AVG(power) as avg_power,
    MAX(power) as max_power,
    MIN(power) as min_power,
    SUM(energy) as total_energy,
    COUNT(*) as reading_count
FROM energy_readings
GROUP BY meter_id, bucket;




CREATE MATERIALIZED VIEW energy_readings_daily
WITH (timescaledb.continuous) AS
SELECT 
    meter_id,
    time_bucket('1 day', timestamp) AS bucket,
    AVG(power) as avg_power,
    MAX(power) as max_power,
    MIN(power) as min_power,
    SUM(energy) as total_energy,
    COUNT(*) as reading_count
FROM energy_readings
GROUP BY meter_id, bucket;


-----Add refresh policies-----
SELECT add_continuous_aggregate_policy('energy_readings_15min', 
  start_offset => INTERVAL '3 days', 
  end_offset => INTERVAL '1 hour', 
  schedule_interval => INTERVAL '15 minutes'); 