import csv
import json
import random
import sys
import time
from datetime import datetime
from kafka import KafkaProducer

csv.field_size_limit(sys.maxsize)

PATH = "/home/vboxuser/cord19/data/metadata.csv"
TOPIC = "cord19_stream"

# 1. Cargar una muestra aleatoria (~1 %) de artículos reales
print("Cargando muestra de metadata.csv ...")
registros = []
with open(PATH, newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        if random.random() > 0.01:
            continue
        year = (row.get("publish_time") or "")[:4]
        abstract = row.get("abstract") or ""
        registros.append({
            "cord_uid": row.get("cord_uid"),
            "source": (row.get("source_x") or "desconocido")[:40],
            "journal": (row.get("journal") or "desconocido").lower()[:60],
            "year": int(year) if year.isdigit() else None,
            "abstract_words": len(abstract.split()),
        })
print(f"Registros en memoria: {len(registros)}")

# 2. Productor Kafka
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"))

# 3. Enviar eventos simulando llegada continua de publicaciones
while True:
    evento = dict(random.choice(registros))
    evento["event_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    producer.send(TOPIC, value=evento)
    print(f"Sent: {evento}")
    time.sleep(random.uniform(0.2, 0.6))
