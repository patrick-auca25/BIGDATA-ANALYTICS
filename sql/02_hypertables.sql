SELECT create_hypertable('energy_readings', 'timestamp', 
chunk_time_interval => INTERVAL '1 day',migrate_data => true);