import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, sum as spark_sum, count as spark_count, desc
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

from kafka_client import DEFAULT_BOOTSTRAP_SERVERS, TOPICS

EVENT_SCHEMA = StructType([
    StructField("event_type", StringType(), nullable=False),
    StructField("device_ip", StringType(), nullable=True),
    StructField("device_mac", StringType(), nullable=True),
    StructField("event_action", StringType(), nullable=True),
    StructField("session_id", StringType(), nullable=True),
    StructField("bytes_sent", LongType(), nullable=True),
    StructField("bytes_received", LongType(), nullable=True),
    StructField("packets_sent", LongType(), nullable=True),
    StructField("packets_received", LongType(), nullable=True),
    StructField("interface", StringType(), nullable=True),
    StructField("domain", StringType(), nullable=True),
    StructField("query_type", StringType(), nullable=True),
    StructField("alert_type", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("timestamp", StringType(), nullable=False)
])


def get_spark(app_name="HotspotStreamingPipeline"):
    return SparkSession.builder.appName(app_name).getOrCreate()


def write_bronze_stream(spark, output_dir, checkpoint_dir, kafka_topic_list):
    kafka_options = {
        "kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
        "subscribe": ",".join(kafka_topic_list),
        "startingOffsets": "latest"
    }

    raw_df = (
        spark.readStream.format("kafka")
        .options(**kafka_options)
        .load()
        .selectExpr("CAST(value AS STRING) as json")
        .select(from_json(col("json"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
    )

    return (
        raw_df.writeStream
        .format("parquet")
        .option("path", os.path.join(output_dir, "bronze"))
        .option("checkpointLocation", os.path.join(checkpoint_dir, "bronze_checkpoint"))
        .outputMode("append")
        .start()
    )


def write_silver_stream(spark, bronze_dir, output_dir, checkpoint_dir):
    bronze_stream = (
        spark.readStream
        .format("parquet")
        .schema(EVENT_SCHEMA.add(StructField("event_timestamp", TimestampType(), nullable=True)))
        .load(bronze_dir)
    )

    normalized = (
        bronze_stream
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .dropDuplicates(["event_type", "device_ip", "domain", "session_id", "timestamp"])
        .filter(col("event_timestamp").isNotNull())
    )

    device_sessions = normalized.filter(col("event_type").isin(["DEVICE_CONNECTED", "DEVICE_DISCONNECTED"]))
    bandwidth_usage = normalized.filter(col("event_type") == "BANDWIDTH_USAGE")
    dns_activity = normalized.filter(col("event_type") == "DNS_QUERY")

    device_sessions.writeStream.format("parquet").option("path", os.path.join(output_dir, "silver_device_sessions")).option("checkpointLocation", os.path.join(checkpoint_dir, "silver_sessions_checkpoint")).outputMode("append").start()
    bandwidth_usage.writeStream.format("parquet").option("path", os.path.join(output_dir, "silver_bandwidth_usage")).option("checkpointLocation", os.path.join(checkpoint_dir, "silver_bandwidth_checkpoint")).outputMode("append").start()
    dns_activity.writeStream.format("parquet").option("path", os.path.join(output_dir, "silver_dns_activity")).option("checkpointLocation", os.path.join(checkpoint_dir, "silver_dns_checkpoint")).outputMode("append").start()

    return True


def write_gold_tables(spark, silver_dir, output_dir):
    device_sessions = spark.read.format("parquet").load(os.path.join(silver_dir, "silver_device_sessions"))
    bandwidth_usage = spark.read.format("parquet").load(os.path.join(silver_dir, "silver_bandwidth_usage"))
    dns_activity = spark.read.format("parquet").load(os.path.join(silver_dir, "silver_dns_activity"))

    gold_device_usage_summary = (
        bandwidth_usage.groupBy("device_ip")
        .agg(
            spark_sum("bytes_sent").alias("total_bytes_sent"),
            spark_sum("bytes_received").alias("total_bytes_received"),
            spark_sum("packets_sent").alias("total_packets_sent"),
            spark_sum("packets_received").alias("total_packets_received"),
            spark_count("device_ip").alias("observations")
        )
    )

    gold_top_domains = (
        dns_activity.groupBy("domain")
        .agg(spark_count("domain").alias("domain_count"))
        .orderBy(desc("domain_count"))
        .limit(50)
    )

    gold_hourly_bandwidth = (
        bandwidth_usage
        .withColumn("hour", window(col("event_timestamp"), "1 hour"))
        .groupBy("hour", "device_ip")
        .agg(
            spark_sum("bytes_sent").alias("hourly_bytes_sent"),
            spark_sum("bytes_received").alias("hourly_bytes_received")
        )
    )

    gold_device_usage_summary.write.mode("overwrite").parquet(os.path.join(output_dir, "gold_device_usage_summary"))
    gold_top_domains.write.mode("overwrite").parquet(os.path.join(output_dir, "gold_top_domains"))
    gold_hourly_bandwidth.write.mode("overwrite").parquet(os.path.join(output_dir, "gold_hourly_bandwidth"))

    return True


def parse_args():
    parser = argparse.ArgumentParser(description="Databricks Structured Streaming hotspot pipeline")
    parser.add_argument("mode", choices=["bronze", "silver", "gold"], help="Pipeline mode to run")
    parser.add_argument("--output-dir", default="spark_output", help="Base output path for bronze/silver/gold directories")
    parser.add_argument("--checkpoint-dir", default="spark_checkpoints", help="Checkpoint directory for streaming jobs")
    parser.add_argument("--topics", default=','.join([TOPICS["connection"], TOPICS["bandwidth"], TOPICS["dns"], TOPICS["anomaly"]]), help="Kafka topics to subscribe to")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = get_spark()
    output_dir = args.output_dir
    checkpoint_dir = args.checkpoint_dir

    if args.mode == "bronze":
        query = write_bronze_stream(spark, output_dir, checkpoint_dir, args.topics.split(","))
        query.awaitTermination()
    elif args.mode == "silver":
        write_silver_stream(spark, os.path.join(output_dir, "bronze"), output_dir, checkpoint_dir)
        spark.streams.awaitAnyTermination()
    elif args.mode == "gold":
        write_gold_tables(spark, output_dir, os.path.join(output_dir, "gold"))
    else:
        raise ValueError(f"Unknown mode: {args.mode}")


if __name__ == "__main__":
    main()
