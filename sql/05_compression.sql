ALTER TABLE energy_readings SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp DESC'
);
SELECT add_compression_policy('energy_readings', INTERVAL '24 hours');

SELECT compress_chunk(i) FROM show_chunks('energy_readings') i;




ALTER TABLE energy_readings_3h SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy('energy_readings_3h', INTERVAL '24 hours');
SELECT compress_chunk(i) FROM show_chunks('energy_readings_3h') i;




ALTER TABLE energy_readings_week SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy('energy_readings_week', INTERVAL '24 hours');
SELECT compress_chunk(i) FROM show_chunks('energy_readings_week') i;