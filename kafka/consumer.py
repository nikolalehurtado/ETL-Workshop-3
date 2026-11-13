"""
Workshop-3 - Consumidor de Kafka con inferencia en tiempo real.

Por cada evento del topico 'happiness-events':
    1. Guarda el mensaje crudo en raw_happiness_events (antes de cualquier otra cosa).
    2. Valida el evento contra el contrato.
    3. Prepara las caracteristicas en el mismo orden del entrenamiento.
    4. Predice con el modelo serializado (models/model.pkl).
    5. Guarda la prediccion en fact_predictions y marca el evento crudo.

Los eventos invalidos se guardan en la tabla raw con su estado, se omiten de la
prediccion y NO detienen el consumidor.

Uso:
    python kafka\\consumer.py        (se detiene con Ctrl + C)
"""
import json
import math
import os
from datetime import datetime
from pathlib import Path

import joblib
import pandas as pd
import psycopg2
from confluent_kafka import Consumer

BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_PATH = BASE_DIR / "models" / "model.pkl"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "happiness-events"
GROUP_ID = "happiness-ml-team"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "happiness_dw"),
    "user": os.getenv("DB_USER", "happiness"),
    "password": os.getenv("DB_PASSWORD", "happiness"),
}

# ---------------------------------------------------------------------------
# Contrato del evento
# ---------------------------------------------------------------------------
TEXT_FIELDS = ["event_id", "country", "event_time"]
FEATURE_FIELDS = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
NUMERIC_FIELDS = FEATURE_FIELDS + ["actual_happiness_score"]
REQUIRED_FIELDS = ["event_id", "country", "year"] + NUMERIC_FIELDS + ["event_time"]


def is_number(value):
    """True para enteros y decimales. En Python True/False cuentan como enteros, por eso se excluyen."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_event(payload, model_features):
    """
    Revisa el mensaje contra el contrato.
    Devuelve (evento, estado, detalle). El estado es 'VALID', 'INVALID_SCHEMA' o 'INVALID_VALUES'.

    INVALID_SCHEMA: problema de estructura (no es JSON, faltan campos, tipo de dato equivocado).
    INVALID_VALUES: la estructura esta bien, pero un valor no es aceptable.
    """
    # --- Estructura ---
    try:
        event = json.loads(payload)
    except json.JSONDecodeError:
        return None, "INVALID_SCHEMA", "El mensaje no es un JSON valido"

    if not isinstance(event, dict):
        return None, "INVALID_SCHEMA", "El mensaje no es un objeto JSON"

    missing = [field for field in REQUIRED_FIELDS if field not in event]
    if missing:
        return event, "INVALID_SCHEMA", f"Faltan campos: {', '.join(missing)}"

    missing_features = [field for field in model_features if field not in event]
    if missing_features:
        return event, "INVALID_SCHEMA", f"Faltan caracteristicas del modelo: {', '.join(missing_features)}"

    for field in TEXT_FIELDS:
        if not isinstance(event[field], str):
            return event, "INVALID_SCHEMA", f"'{field}' debe ser texto"

    if not isinstance(event["year"], int) or isinstance(event["year"], bool):
        return event, "INVALID_SCHEMA", "'year' debe ser un numero entero"

    for field in NUMERIC_FIELDS:
        if not is_number(event[field]):
            return event, "INVALID_SCHEMA", f"'{field}' debe ser numerico"

    # --- Valores ---
    if event["event_id"].strip() == "":
        return event, "INVALID_VALUES", "'event_id' esta vacio"

    if event["country"].strip() == "":
        return event, "INVALID_VALUES", "'country' esta vacio"

    if not 2000 <= event["year"] <= datetime.now().year:
        return event, "INVALID_VALUES", f"'year' fuera de rango: {event['year']}"

    for field in NUMERIC_FIELDS:
        if not math.isfinite(event[field]):
            return event, "INVALID_VALUES", f"'{field}' no es un numero finito"

    for field in FEATURE_FIELDS:
        if event[field] < 0:
            return event, "INVALID_VALUES", f"'{field}' no puede ser negativo: {event[field]}"

    if not 0 <= event["actual_happiness_score"] <= 10:
        return event, "INVALID_VALUES", f"'actual_happiness_score' fuera de la escala 0-10: {event['actual_happiness_score']}"

    try:
        datetime.fromisoformat(event["event_time"])
    except ValueError:
        return event, "INVALID_VALUES", f"'event_time' no es una fecha ISO-8601 valida: {event['event_time']}"

    return event, "VALID", None


# ---------------------------------------------------------------------------
# Base de datos
# ---------------------------------------------------------------------------
def store_raw_event(connection, message, payload):
    """
    Guarda el mensaje original tal como llego. Devuelve (raw_event_id, estado).
    Si esa posicion de Kafka ya estaba guardada (relectura), devuelve la fila existente.
    """
    key = message.key().decode("utf-8", errors="replace") if message.key() is not None else None
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO raw_happiness_events (kafka_topic, kafka_partition, kafka_offset, kafka_key, raw_payload)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (kafka_topic, kafka_partition, kafka_offset)
            DO UPDATE SET kafka_key = EXCLUDED.kafka_key
            RETURNING raw_event_id, processing_status
            """,
            (message.topic(), message.partition(), message.offset(), key, payload),
        )
        raw_event_id, status = cursor.fetchone()
    connection.commit()
    return raw_event_id, status


def mark_raw_event(connection, raw_event_id, status, detail=None, event_id=None):
    """Actualiza el estado de procesamiento del evento crudo."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE raw_happiness_events
            SET processing_status = %s, error_detail = %s, event_id = COALESCE(%s, event_id)
            WHERE raw_event_id = %s
            """,
            (status, detail, event_id, raw_event_id),
        )


def get_country_id(cursor, country_name):
    """Devuelve el id del pais en dim_country; si no existe, lo crea."""
    cursor.execute(
        "INSERT INTO dim_country (country_name) VALUES (%s) ON CONFLICT (country_name) DO NOTHING",
        (country_name,),
    )
    cursor.execute("SELECT country_id FROM dim_country WHERE country_name = %s", (country_name,))
    return cursor.fetchone()[0]


def get_date_id(cursor, year):
    """Devuelve el id del anio en dim_date; si no existe, lo crea."""
    cursor.execute(
        "INSERT INTO dim_date (date_id, year) VALUES (%s, %s) ON CONFLICT (date_id) DO NOTHING",
        (year, year),
    )
    return year


def store_prediction(connection, raw_event_id, event, predicted_score):
    """Guarda la prediccion en la tabla de hechos y marca el evento crudo como VALID."""
    actual_score = float(event["actual_happiness_score"])
    prediction_error = actual_score - predicted_score

    with connection.cursor() as cursor:
        country_id = get_country_id(cursor, event["country"].strip())
        date_id = get_date_id(cursor, event["year"])
        cursor.execute(
            """
            INSERT INTO fact_predictions
                (raw_event_id, country_id, date_id, actual_score, predicted_score,
                 prediction_error, event_time, prediction_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (raw_event_id, country_id, date_id, actual_score, predicted_score,
             prediction_error, datetime.fromisoformat(event["event_time"]), datetime.now()),
        )
    mark_raw_event(connection, raw_event_id, "VALID", event_id=event["event_id"])
    connection.commit()
    return prediction_error


# ---------------------------------------------------------------------------
# Procesamiento de un mensaje
# ---------------------------------------------------------------------------
def process_message(connection, model, model_features, message):
    """Procesa un mensaje de principio a fin. Devuelve el estado final."""
    payload = message.value().decode("utf-8", errors="replace") if message.value() is not None else ""

    # 1. Guardar el evento crudo ANTES de validar o transformar
    raw_event_id, status = store_raw_event(connection, message, payload)
    if status != "RECEIVED":
        print(f"[raw {raw_event_id}] ya procesado antes ({status}); se omite")
        return "DUPLICATE"

    # 2. Validar contra el contrato
    event, status, detail = validate_event(payload, model_features)
    event_id = event.get("event_id") if isinstance(event, dict) else None
    event_id = event_id if isinstance(event_id, str) else None

    if status != "VALID":
        mark_raw_event(connection, raw_event_id, status, detail, event_id)
        connection.commit()
        print(f"[raw {raw_event_id}] {status}: {detail}")
        return status

    # 3 y 4. Preparar las caracteristicas en el orden del entrenamiento y predecir
    try:
        features = pd.DataFrame([event])[model_features]   # actual_happiness_score NO entra
        predicted_score = float(model.predict(features)[0])
        if not math.isfinite(predicted_score):
            raise ValueError("La prediccion no es un numero finito")

        # 5. Guardar la prediccion
        prediction_error = store_prediction(connection, raw_event_id, event, predicted_score)
    except Exception as error:
        connection.rollback()
        mark_raw_event(connection, raw_event_id, "PREDICTION_ERROR", str(error), event_id)
        connection.commit()
        print(f"[raw {raw_event_id}] PREDICTION_ERROR: {error}")
        return "PREDICTION_ERROR"

    print(f"[raw {raw_event_id}] VALID {event['event_id']} {event['country']} ({event['year']}): "
          f"real={event['actual_happiness_score']:.3f} predicho={predicted_score:.3f} "
          f"error={prediction_error:+.3f}")
    return "VALID"


def main():
    model = joblib.load(MODEL_PATH)
    model_features = list(model.feature_names_in_)
    print("Modelo cargado. Orden de caracteristicas:", model_features)

    connection = psycopg2.connect(**DB_CONFIG)
    print("Conectado a PostgreSQL:", DB_CONFIG["dbname"])

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    print(f"Escuchando el topico '{TOPIC}' con el grupo '{GROUP_ID}'. Ctrl + C para detener.\n")

    counters = {}
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print("Error de Kafka:", message.error())
                continue

            try:
                status = process_message(connection, model, model_features, message)
            except Exception as error:
                # Ultima barrera: ningun evento debe detener el consumidor
                connection.rollback()
                status = "UNEXPECTED_ERROR"
                print(f"Error inesperado en particion {message.partition()} offset {message.offset()}: {error}")

            counters[status] = counters.get(status, 0) + 1

            # Se confirma el avance en Kafka solo despues de guardar en la base de datos
            if status != "UNEXPECTED_ERROR":
                consumer.commit(message=message, asynchronous=False)
    except KeyboardInterrupt:
        print("\nDetenido por el usuario.")
    finally:
        consumer.close()
        connection.close()
        print("Resumen:", counters if counters else "no se proceso ningun evento")


if __name__ == "__main__":
    main()