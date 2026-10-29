# Análisis Big Data del dataset CORD-19 con Apache Spark, Kafka y Spark Streaming

Curso Big Data (202016911), UNAD, Ingeniería de Sistemas.

## 1. Problema
La pandemia de COVID-19 generó un crecimiento explosivo de la literatura científica.
Este proyecto usa Apache Spark para caracterizar esa producción (evolución temporal,
fuentes, revistas, autores, resúmenes y términos frecuentes) y simula la llegada
continua de nuevos artículos con Kafka y Spark Streaming.

## 2. Dataset
CORD-19 Research Challenge (Allen Institute for AI), archivo `metadata.csv`
(529.431 registros). Fuente: https://www.kaggle.com/datasets/allen-institute-for-ai/CORD-19-research-challenge

El archivo NO se incluye en el repositorio. Para descargarlo:
```bash
pip install kaggle
kaggle datasets download -d allen-institute-for-ai/CORD-19-research-challenge -f metadata.csv
unzip metadata.csv.zip
```
Requiere un token de la API de Kaggle en `~/.kaggle/kaggle.json`.

## 3. Entorno
Ubuntu 22.04 (VirtualBox), Hadoop, Apache Spark 3.5.3 (Scala 2.12.18),
Apache Kafka con ZooKeeper, Python 3.10, matplotlib, pandas, kafka-python.

## 4. Procesamiento batch
Script: `batch/cord19_batch.py`. Realiza carga, diagnóstico de nulos, limpieza
(duplicados, títulos vacíos, fechas válidas), EDA con DataFrames, conteo de palabras
con RDDs y visualizaciones en PNG.

Ejecución:
```bash
spark-submit --driver-memory 1g batch/cord19_batch.py
```
Antes, ajustar la variable `DATA` con la ruta del CSV. Los resultados se guardan en `output/`.

## 5. Procesamiento en tiempo real
1. Iniciar ZooKeeper y Kafka:
```bash
nohup /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties > ~/zookeeper.log 2>&1 &
nohup /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties > ~/kafka.log 2>&1 &
```
2. Crear el topic:
```bash
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 1 --topic cord19_stream
```
3. Ejecutar el productor (terminal 1):
```bash
python3 streaming/cord19_producer.py
```
4. Ejecutar el consumidor (terminal 2):
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  streaming/cord19_streaming_consumer.py
```
El consumidor calcula artículos por fuente y por ventana de 1 minuto y genera
`output/stream_dashboard.png`, que se sirve con `python3 -m http.server 8000`.

## 6. Resultados principales
- 529.431 registros originales, 503.567 tras la limpieza.
- Pico de publicaciones en 2021 (213.297); WHO es la fuente principal.
- Resumen promedio de 212 palabras; 21,7 % de los artículos sin resumen.
- Términos más frecuentes: covid, patients, pandemic, health, sars.

## 7. Limitaciones
El dataset deja de actualizarse hacia mediados de 2022; hay autores anónimos y
nombres corruptos; el streaming es una simulación a partir de una muestra real.

