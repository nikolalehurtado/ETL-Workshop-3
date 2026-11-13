"""
Workshop-3 - Productor de Kafka.

Lee el dataset unificado registro por registro y publica cada fila como un
evento JSON en el topico 'happiness-events', usando el pais como clave.

Uso:
    python kafka\\producer.py                 (todos los registros, 0.5 s entre eventos)
    python kafka\\producer.py --delay 0.1     (mas rapido)
    python kafka\\producer.py --limit 10      (solo los primeros 10, para probar)
"""
import argparse
import csv
import json
import time
from datetime import datetime
from pathlib import Path

from confluent_kafka import Producer

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data" / "processed" / "happiness_unified.csv"
STREAM_LOG_PATH = BASE_DIR / "data" / "streaming" / "sent_events.jsonl"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "happiness-events"


def build_event(number, row):
    """Convierte una fila del CSV en un evento con el formato JSON del taller."""
    return {
        "event_id": f"E{number:03d}",
        "country": row["country"],
        "year": int(row["year"]),
        "gdp": float(row["gdp"]),
        "family": float(row["family"]),
        "health": float(row["health"]),
        "freedom": float(row["freedom"]),
        "generosity": float(row["generosity"]),
        "corruption": float(row["corruption"]),
        "actual_happiness_score": float(row["happiness_score"]),
        "event_time": datetime.now().isoformat(timespec="seconds"),
    }


def delivery_report(error, message):
    """Kafka llama a esta funcion para confirmar (o no) la entrega de cada evento."""
    if error is not None:
        print(f"  ERROR de entrega: {error}")
    else:
        print(f"  entregado -> clave={message.key().decode('utf-8')} "
              f"particion={message.partition()} offset={message.offset()}")


def main():
    parser = argparse.ArgumentParser(description="Productor de eventos de felicidad")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="segundos de espera entre eventos (por defecto 0.5)")
    parser.add_argument("--limit", type=int, default=None,
                        help="cantidad maxima de eventos a enviar (por defecto todos)")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    sent = 0

    with open(DATA_PATH, newline="", encoding="utf-8") as data_file, \
         open(STREAM_LOG_PATH, "w", encoding="utf-8") as stream_log:

        for number, row in enumerate(csv.DictReader(data_file), start=1):
            if args.limit is not None and sent >= args.limit:
                break

            event = build_event(number, row)
            payload = json.dumps(event, ensure_ascii=False)

            producer.produce(
                TOPIC,
                key=event["country"].encode("utf-8"),
                value=payload.encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)              # atiende las confirmaciones pendientes

            stream_log.write(payload + "\n")
            sent += 1
            print(f"enviado {event['event_id']}: {event['country']} ({event['year']})")

            time.sleep(args.delay)        # simula que los eventos llegan con el tiempo

    producer.flush()                      # espera a que Kafka confirme todo antes de terminar
    print(f"\nListo: {sent} eventos enviados al topico '{TOPIC}'.")


if __name__ == "__main__":
    main()