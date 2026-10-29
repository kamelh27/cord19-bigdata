import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

OUT = "/home/vboxuser/cord19/output"
os.makedirs(OUT, exist_ok=True)

spark = (SparkSession.builder
         .appName("CORD19_Streaming")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("cord_uid", StringType()),
    StructField("source", StringType()),
    StructField("journal", StringType()),
    StructField("year", IntegerType()),
    StructField("abstract_words", IntegerType()),
    StructField("event_time", StringType()),
])

# Leer desde Kafka
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "cord19_stream")
       .option("startingOffsets", "latest")
       .load())

parsed = (raw.select(from_json(col("value").cast("string"), schema).alias("d"))
          .select("d.*")
          .withColumn("event_time", to_timestamp("event_time")))

# Conteo y promedio de palabras por ventana de 1 minuto y fuente
stats = (parsed
         .groupBy(window("event_time", "1 minute"), "source")
         .agg(count("*").alias("n_articulos"),
              avg("abstract_words").alias("prom_palabras")))

def procesar(batch_df, batch_id):
    pdf = (batch_df
           .select(col("window.start").alias("ventana"), "source",
                   "n_articulos", "prom_palabras")
           .toPandas())
    if pdf.empty:
        return
    total = (pdf.groupby("source")["n_articulos"].sum()
             .sort_values(ascending=False).head(10))
    por_min = pdf.groupby("ventana")["n_articulos"].sum().sort_index()

    print(f"\n=== Lote {batch_id} | total eventos: {int(pdf['n_articulos'].sum())} ===")
    print(total.to_string())

    fig, ax = plt.subplots(1, 2, figsize=(14, 5))
    total[::-1].plot(kind="barh", ax=ax[0], color="#1f77b4")
    ax[0].set_title("Artículos recibidos por fuente (acumulado)")
    ax[0].set_xlabel("Artículos")
    por_min.plot(kind="line", marker="o", ax=ax[1], color="#d62728")
    ax[1].set_title("Artículos por ventana de 1 minuto")
    ax[1].set_xlabel("Ventana"); ax[1].set_ylabel("Artículos")
    plt.tight_layout()
    tmp = f"{OUT}/tmp_stream.png"
    plt.savefig(tmp, dpi=110)
    plt.close(fig)
    os.replace(tmp, f"{OUT}/stream_dashboard.png")   # sobrescribe la imagen "en vivo"

query = (stats.writeStream
         .outputMode("complete")
         .foreachBatch(procesar)
         .option("checkpointLocation", "/tmp/chk_cord19")
         .trigger(processingTime="10 seconds")
         .start())

query.awaitTermination()
