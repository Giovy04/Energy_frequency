import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, abs as sql_abs, to_date
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_IN = os.getenv("TOPIC_IN", "frequency.telemetry")
PARQUET_PATH = os.getenv("PARQUET_PATH", "/data/telemetry_parquet/frequency")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/data/chk/frequency_to_parquet")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")

schema = StructType([
    StructField("@timestamp", StringType(), True),
    StructField("ingest_ts", StringType(), True),
    StructField("source_ts_raw", StringType(), True),
    StructField("stream", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("grid", StringType(), True),
    StructField("source", StringType(), True),
    StructField("n", StringType(), True),
    StructField("d", StringType(), True),
    StructField("phase_deg", DoubleType(), True),
    StructField("dt", DoubleType(), True),
    StructField("frequency_hz", DoubleType(), True),
    StructField("deviation_hz", DoubleType(), True),
    StructField("abs_deviation_mhz", DoubleType(), True),
])

spark = (
    SparkSession.builder
    .appName("frequency_to_parquet")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC_IN)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .load()
)

events = (
    kafka_df.select(from_json(col("value").cast("string"), schema).alias("j"))
    .select("j.*")
    .filter(col("stream") == "telemetry")
    .filter(col("deviation_hz").isNotNull())
    .withColumn("event_ts", to_timestamp(col("`@timestamp`")))
    .withColumn("event_date", to_date(col("event_ts")))
    .withColumn("dev_abs_hz", sql_abs(col("deviation_hz")))
    .select(
        "event_ts", "event_date",
        "grid", "source",
        col("n").alias("station"),
        "d",
        "frequency_hz", "deviation_hz", "dev_abs_hz", "abs_deviation_mhz",
        "phase_deg", "dt",
        "source_ts_raw", "ingest_ts"
    )
)

query = (
    events.writeStream
    .format("parquet")
    .option("path", PARQUET_PATH)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .partitionBy("event_date")
    .outputMode("append")
    .start()
)

query.awaitTermination()
