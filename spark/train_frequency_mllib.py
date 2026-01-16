import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, hour, abs as sql_abs
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

PARQUET_PATH = os.getenv("PARQUET_PATH", "/data/telemetry_parquet/frequency")
MODEL_OUT = os.getenv("MODEL_OUT", "/data/models/frequency_anomaly_clf")

WARN_HZ = float(os.getenv("WARN_HZ", "0.02"))     # weak label
DT_ABS_STALE = float(os.getenv("DT_ABS_STALE", "120"))  # opzionale: “stale” se |dt| alto
TEST_FRACTION = float(os.getenv("TEST_FRACTION", "0.2"))
SEED = int(os.getenv("SEED", "42"))

def main():
    spark = (
        SparkSession.builder
        .appName("frequency-mllib-train")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(PARQUET_PATH)

    # feature engineering minimo
    df = (
        df.dropna(subset=["event_ts","station","frequency_hz","deviation_hz","phase_deg","dt"])
          .withColumn("hour", hour(col("event_ts")))
          .withColumn("dev_abs_hz", sql_abs(col("deviation_hz")))
          .withColumn("dt_abs", sql_abs(col("dt")))
    )

    # weak labels (regole)
    is_anom = (col("dev_abs_hz") >= lit(WARN_HZ)) | (col("dt_abs") >= lit(DT_ABS_STALE))
    df = df.withColumn("label", when(is_anom, lit(1.0)).otherwise(lit(0.0)))

    print("Label distribution:")
    df.groupBy("label").count().orderBy("label").show(truncate=False)

    if df.select("label").distinct().count() < 2:
        print("ERRORE: hai una sola classe. Fai accumulare più storico o abbassa WARN_HZ/DT_ABS_STALE.")
        raise SystemExit(2)

    train, test = df.randomSplit([1.0 - TEST_FRACTION, TEST_FRACTION], seed=SEED)

    station_indexer = StringIndexer(inputCol="station", outputCol="station_idx", handleInvalid="keep")
    assembler = VectorAssembler(
        inputCols=["frequency_hz","deviation_hz","phase_deg","dt","dev_abs_hz","dt_abs","hour","station_idx"],
        outputCol="features"
    )

    clf = RandomForestClassifier(
        labelCol="label", featuresCol="features",
        numTrees=100, seed=SEED
    )

    pipeline = Pipeline(stages=[station_indexer, assembler, clf])
    model = pipeline.fit(train)

    preds = model.transform(test)

    auc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    ).evaluate(preds)

    print(f"AUC: {auc:.4f}")
    preds.select("label","prediction","probability").show(20, truncate=False)

    model.write().overwrite().save(MODEL_OUT)
    print(f"Model saved to: {MODEL_OUT}")

    spark.stop()

if __name__ == "__main__":
    main()
