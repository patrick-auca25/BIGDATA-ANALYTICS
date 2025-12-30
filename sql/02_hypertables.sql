-- Convert main table to hypertable with 1-day chunks
SELECT create_hypertable(
    'energy_readings', 
    'timestamp', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE,
    migrate_data => TRUE  
);

-- Create hypertables with different chunk intervals
SELECT create_hypertable(
    'energy_readings_3h', 
    'timestamp', 
    chunk_time_interval => INTERVAL '3 hours',
    if_not_exists => TRUE
);

SELECT create_hypertable(
    'energy_readings_week', 
    'timestamp', 
    chunk_time_interval => INTERVAL '1 week',
    if_not_exists => TRUE
);
