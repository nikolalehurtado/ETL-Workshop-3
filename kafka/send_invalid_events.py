"""
Workshop-3 - Envia eventos danados a proposito para probar la validacion del consumidor.

Ninguno de estos eventos debe detener al consumidor: cada uno debe quedar en
raw_happiness_events con estado INVALID_SCHEMA o INVALID_VALUES y sin prediccion.

Uso:
    python kafka\\send_invalid_events.py
"""
import json
from datetime import datetime

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "happiness-events"


def good_event(event_id):
    """Un evento correcto, que cada caso de prueba dana de una forma distinta."""
    return {
        "event_id": event_id,
        "country": "Colombia",
        "year": 2019,
        "gdp": 1.2,
        "family": 0.8,
        "health": 0.9,
        "freedom": 0.6,
        "generosity": 0.3,
        "corruption": 0.1,
        "actual_happiness_score": 6.2,
        "event_time": datetime.now().isoformat(timespec="seconds"),
    }


def build_cases():
    """Devuelve una lista de (descripcion, estado esperado, mensaje como texto)."""
    cases = []

    cases.append(("Texto que no es JSON", "INVALID_SCHEMA", "esto no es un JSON"))

    event = good_event("BAD-02")
    del event["gdp"]
    cases.append(("Falta el campo gdp", "INVALID_SCHEMA", json.dumps(event)))

    event = good_event("BAD-03")
    event["year"] = "2019"
    cases.append(("year llega como texto", "INVALID_SCHEMA", json.dumps(event)))

    event = good_event("BAD-04")
    event["health"] = "alta"
    cases.append(("health llega como texto", "INVALID_SCHEMA", json.dumps(event)))

    event = good_event("BAD-05")
    event["gdp"] = -1.5
    cases.append(("gdp negativo", "INVALID_VALUES", json.dumps(event)))

    event = good_event("BAD-06")
    event["actual_happiness_score"] = 62
    cases.append(("Puntaje fuera de la escala 0-10", "INVALID_VALUES", json.dumps(event)))

    event = good_event("BAD-07")
    event["event_time"] = "ayer por la tarde"
    cases.append(("event_time no es una fecha ISO-8601", "INVALID_VALUES", json.dumps(event)))

    event = good_event("")
    cases.append(("event_id vacio", "INVALID_VALUES", json.dumps(event)))

    return cases


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    cases = build_cases()

    for description, expected_status, payload in cases:
        producer.produce(TOPIC, key="Colombia".encode("utf-8"), value=payload.encode("utf-8"))
        print(f"enviado: {description}  (esperado: {expected_status})")

    producer.flush()
    print(f"\nListo: {len(cases)} eventos invalidos enviados al topico '{TOPIC}'.")


if __name__ == "__main__":
    main()