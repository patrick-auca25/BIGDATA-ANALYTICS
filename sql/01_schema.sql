CREATE TABLE energy_readings (
    meter_id BIGINT,
    timestamp TIMESTAMPTZ NOT NULL,
    power DOUBLE PRECISION,
    voltage DOUBLE PRECISION,
    current DOUBLE PRECISION,
    frequency DOUBLE PRECISION,
    energy DOUBLE PRECISION
);