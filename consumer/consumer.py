from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    when,
    lit,
    to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType
import logging

KAFKA_BOOTSTRAP = "kafka:29092"
INPUT_TOPIC = "firewall-logs"
CLEAN_TOPIC = "firewall-clean"
QUARANTINE_TOPIC = "firewall-quarantine"

ALLOWED_LEVELS = ["INFO", "WARN", "ERROR"]
ALLOWED_SOURCES = ["producer", "servers"]

logging.basicConfig(
    level=logging.INFO,
    format="FIREWALL | %(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("firewall")

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("level", StringType(), True),
    StructField("message", StringType(), True),
    StructField("timestamp", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("StreamingFirewall")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = raw_df.select(
    from_json(col("value").cast("string"), event_schema).alias("event")
).select("event.*")

#firewall logic
validated_df = (
    parsed_df
    .withColumn(
        "parsed_timestamp",
        to_timestamp(col("timestamp"))
    )
    .withColumn(
        "is_valid",
        when(col("event_id").isNull(), False)
        .when(~col("level").isin(ALLOWED_LEVELS), False)
        .when(col("parsed_timestamp").isNull(), False)
        .when(~col("source").isin(ALLOWED_SOURCES), False)
        .otherwise(True)
    )
    .withColumn(
        "reject_reason",
        when(col("event_id").isNull(), lit("event_id_missing"))
        .when(~col("level").isin(ALLOWED_LEVELS), lit("invalid_level"))
        .when(col("parsed_timestamp").isNull(), lit("invalid_timestamp"))
        .when(~col("source").isin(ALLOWED_SOURCES), lit("unknown_source"))
        .otherwise(lit(None))
    )
    .drop("parsed_timestamp")
)

def log_batch(df, batch_id):
    valid = df.filter(col("is_valid") == True).count()
    invalid = df.filter(col("is_valid") == False).count()
    logger.info(
        f"Batch {batch_id} | clean = {valid} | quarantine = {invalid}"
    )

log_query = validated_df.writeStream.foreachBatch(log_batch).start()

clean_df = validated_df.filter(col("is_valid") == True)
quarantine_df = validated_df.filter(col("is_valid") == False)

#writing clean events
clean_query = (
    clean_df
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", CLEAN_TOPIC)
    .option("checkpointLocation", "/tmp/firewall-clean-checkpoint")
    .outputMode("append")
    .start()
)

#writing quarantine events
quarantine_query = (
    quarantine_df
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", QUARANTINE_TOPIC)
    .option("checkpointLocation", "/tmp/firewall-quarantine-checkpoint")
    .outputMode("append")
    .start()
)

logger.info("Firewall routing active")

spark.streams.awaitAnyTermination()
