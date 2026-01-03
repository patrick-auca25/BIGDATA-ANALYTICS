CREATE TABLE energy_readings_3h (LIKE energy_readings INCLUDING ALL);

CREATE TABLE energy_readings_week (LIKE energy_readings INCLUDING ALL);

SELECT create_hypertable(
    'energy_readings_3h', 
    'timestamp',
    chunk_time_interval => INTERVAL '3 hours'
);


SELECT create_hypertable(
    'energy_readings_week', 
    'timestamp',
    chunk_time_interval => INTERVAL '1 week'
);



INSERT INTO energy_readings_3h 
SELECT * FROM energy_readings;


INSERT INTO energy_readings_week 
SELECT * FROM energy_readings;
