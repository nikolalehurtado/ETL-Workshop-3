-- Workshop-3: modelo analítico para las predicciones de felicidad
-- Se puede ejecutar varias veces sin error (IF NOT EXISTS).

-- Tabla raw / staging: el mensaje de Kafka exactamente como llegó.
-- No es una dimensión; existe para trazabilidad, auditoría y reprocesamiento.
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id      SERIAL PRIMARY KEY,
    event_id          TEXT,
    kafka_topic       TEXT        NOT NULL,
    kafka_partition   INTEGER     NOT NULL,
    kafka_offset      BIGINT      NOT NULL,
    kafka_key         TEXT,
    raw_payload       TEXT        NOT NULL,
    processing_status VARCHAR(20) NOT NULL DEFAULT 'RECEIVED',
    error_detail      TEXT,
    received_at       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_processing_status CHECK (
        processing_status IN ('RECEIVED', 'VALID', 'INVALID_SCHEMA', 'INVALID_VALUES', 'PREDICTION_ERROR')
    ),
    CONSTRAINT uq_kafka_position UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

-- Dimensión país
CREATE TABLE IF NOT EXISTS dim_country (
    country_id   SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE
);

-- Dimensión fecha: el año de la observación histórica
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INTEGER PRIMARY KEY,
    year    INTEGER NOT NULL UNIQUE
);

-- Tabla de hechos: una fila por cada evento válido que recibió predicción
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id        SERIAL PRIMARY KEY,
    raw_event_id         INTEGER        NOT NULL UNIQUE REFERENCES raw_happiness_events (raw_event_id),
    country_id           INTEGER        NOT NULL REFERENCES dim_country (country_id),
    date_id              INTEGER        NOT NULL REFERENCES dim_date (date_id),
    actual_score         NUMERIC(10, 5) NOT NULL,
    predicted_score      NUMERIC(10, 5) NOT NULL,
    prediction_error     NUMERIC(10, 5) NOT NULL,
    event_time           TIMESTAMP      NOT NULL,
    prediction_timestamp TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP
);