import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, abs as sql_abs, lit, when, concat,
    format_number, current_timestamp, struct, to_json
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_IN = os.getenv("TOPIC_IN", "frequency.telemetry")
TOPIC_OUT = os.getenv("TOPIC_OUT", "frequency.spark-alerts")

WARN_HZ = float(os.getenv("WARN_HZ", "0.02"))
CRIT_HZ = float(os.getenv("CRIT_HZ", "0.05"))

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/data/chk/frequency_spark_alerts")

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
    .appName("frequency_spark_alerts")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC_IN)
    .option("startingOffsets", os.getenv("STARTING_OFFSETS", "latest"))
    .option("failOnDataLoss", "false")
    .load()
)

events = (
    kafka_df.select(from_json(col("value").cast("string"), schema).alias("j"))
    .select("j.*")
    .withColumn("event_ts", to_timestamp(col("`@timestamp`")))
    .withColumn("dev_abs_hz", sql_abs(col("deviation_hz")))
    .filter(col("stream") == "telemetry")
    .filter(col("deviation_hz").isNotNull())
)

alerts = (
    events.filter(col("dev_abs_hz") >= lit(WARN_HZ))
    .withColumn("alert_type",
        when(col("dev_abs_hz") >= lit(CRIT_HZ), lit("FREQ_DEVIATION_CRIT"))
        .otherwise(lit("FREQ_DEVIATION_WARN"))
    )
    .withColumn("severity",
        when(col("dev_abs_hz") >= lit(CRIT_HZ), lit(3))
        .otherwise(lit(2))
    )
    .withColumn("reason",
        concat(lit("Deviazione alta: "), format_number(col("deviation_hz"), 6), lit(" Hz"))
    )
    .select(
        col("event_ts").alias("@timestamp"),
        current_timestamp().alias("alert_generated_at"),
        lit("grid_alert").alias("event_type"),
        lit("spark_rules").alias("detector"),
        col("alert_type"),
        col("reason"),
        col("severity"),
        col("grid"),
        col("n").alias("station"),
        col("source"),
        struct(
            col("frequency_hz"),
            col("deviation_hz"),
            col("abs_deviation_mhz"),
            col("phase_deg"),
            col("dt"),
            col("source_ts_raw"),
            col("ingest_ts"),
            col("d"),
        ).alias("raw")
    )
)

out = alerts.select(
    lit(None).cast("string").alias("key"),
    to_json(struct(*[col(c) for c in alerts.columns])).alias("value")
)

query = (
    out.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", TOPIC_OUT)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .outputMode("append")
    .start()
)

query.awaitTermination()
