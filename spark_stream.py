"""
spark_stream.py — Osoba 5

Spark Structured Streaming:
- czyta temat `train_arrivals` z Kafki
- parsuje JSON do tabeli z polami rekordu
- okno czasowe 5 min, przesunięcie 1 min, watermark 10 min
- agregacja per stacja: liczba pociągów + średnie opóźnienie
- wynik wypisywany do konsoli (sink: console)

Wzorzec połączenia Spark <-> Kafka: Lab 4
Wzorzec okna czasowego + watermark: Lab 3
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    count,
    avg,
    round as spark_round,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# 1. Sesja Spark
# ---------------------------------------------------------------------------
# Pakiet spark-sql-kafka jest potrzebny, żeby Spark umiał czytać z Kafki.
# Wersja musi pasować do wersji Sparka w obrazie JupyterLab z docker-compose.
spark = (
    SparkSession.builder
    .appName("TrainDelayMonitor")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    )
    .getOrCreate()
)

# Mniej szumu w konsoli — widać tylko nasze tabele, a nie logi INFO Sparka.
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 2. Schemat wiadomości z Kafki
# ---------------------------------------------------------------------------
# Producent (Osoba 2) wysyła JSON-a z tymi polami.
# `timestamp` jest stringiem ISO 8601 — Spark sam rzutuje go na TimestampType.
schema = StructType([
    StructField("train_id", StringType()),
    StructField("line", StringType()),
    StructField("station", StringType()),
    StructField("scheduled_arrival", StringType()),
    StructField("actual_arrival", StringType()),
    StructField("delay_minutes", IntegerType()),
    StructField("timestamp", TimestampType()),
])

# ---------------------------------------------------------------------------
# 3. Czytanie z Kafki (źródło)
# ---------------------------------------------------------------------------
kafka_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "train_arrivals")
    .option("startingOffsets", "latest")  # tylko nowe wiadomości
    .load()
)

# Kafka zwraca surowe bajty w kolumnie `value`. Rzutujemy na string i parsujemy JSON.
trains = (
    kafka_raw
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

# ---------------------------------------------------------------------------
# 4. Okno czasowe + watermark + agregacja
# ---------------------------------------------------------------------------
# - okno: 5 minut, przesunięcie 1 minuta -> co minutę nowy zestaw wyników
# - watermark: 10 minut -> Spark może odrzucać dane starsze niż 10 min od
#   maksymalnego widzianego event-time (inaczej czekałby w nieskończoność)
agg = (
    trains
    .withWatermark("timestamp", "10 minutes")
    .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("station"),
    )
    .agg(
        count("*").alias("trains_in_window"),
        spark_round(avg("delay_minutes"), 2).alias("avg_delay_min"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("station"),
        col("trains_in_window"),
        col("avg_delay_min"),
    )
    .orderBy("window_start", "station")
)

# ---------------------------------------------------------------------------
# 5. Zapis do konsoli (sink)
# ---------------------------------------------------------------------------
# outputMode="complete" pozwala użyć orderBy i pokazuje pełną tabelę po każdym trigerze.
# trigger co 1 minutę -> co minutę widać świeży zestaw okien.
query = (
    agg.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .option("numRows", 50)
    .trigger(processingTime="1 minute")
    .start()
)

query.awaitTermination()
