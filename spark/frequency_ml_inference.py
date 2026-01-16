import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, abs as sql_abs, hour, lit, when, concat,
    format_number, current_timestamp, struct, to_json
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_IN = os.getenv("TOPIC_IN", "frequency.telemetry")
TOPIC_OUT = os.getenv("TOPIC_OUT", "frequency.ml-alerts")
MODEL_PATH = os.getenv("MODEL_PATH", "/data/models/frequency_anomaly_clf")

PROB_WARN = float(os.getenv("PROB_WARN", "0.60"))
PROB_CRIT = float(os.getenv("PROB_CRIT", "0.85"))

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/data/chk/frequency_ml_inference")
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
    .appName("frequency_ml_inference")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

model = PipelineModel.load(MODEL_PATH)

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
    .withColumn("station", col("n"))
    .withColumn("hour", hour(col("event_ts")))
    .withColumn("dev_abs_hz", sql_abs(col("deviation_hz")))
    .withColumn("dt_abs", sql_abs(col("dt")))
)

scored = (
    model.transform(events)
    .withColumn("p_anom", vector_to_array(col("probability"))[1])
)

ml_alerts = (
    scored
    .filter(col("prediction") == lit(1.0))
    .withColumn("alert_type", lit("FREQ_ANOMALY_ML"))
    .withColumn(
        "severity",
        when(col("p_anom") >= lit(PROB_CRIT), lit(3)).otherwise(lit(2))
    )
    .withColumn("reason", concat(lit("ML anomaly p="), format_number(col("p_anom"), 3)))
    .select(
        col("event_ts").alias("@timestamp"),
        current_timestamp().alias("alert_generated_at"),
        lit("grid_alert").alias("event_type"),
        lit("spark_ml").alias("detector"),
        col("alert_type"),
        col("reason"),
        col("severity"),
        col("grid"),
        col("station"),
        col("source"),
        struct(
            col("frequency_hz"),
            col("deviation_hz"),
            col("dev_abs_hz"),
            col("phase_deg"),
            col("dt"),
            col("p_anom"),
            col("source_ts_raw"),
            col("ingest_ts"),
            col("d"),
        ).alias("raw")
    )
)

out = ml_alerts.select(
    lit(None).cast("string").alias("key"),
    to_json(struct(*[col(c) for c in ml_alerts.columns])).alias("value")
)

q = (
    out.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", TOPIC_OUT)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .outputMode("append")
    .start()
)

q.awaitTermination()
