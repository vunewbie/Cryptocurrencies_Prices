from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv
import os
import json

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW")
KAFKA_TOPIC_MOVING = os.getenv("KAFKA_TOPIC_MOVING")

def create_spark_session():
    spark = SparkSession.builder \
        .appName("BTCMovingStatistics") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark

def define_windows():
    windows = [
        ("30s", "30 seconds"),
        ("1m", "1 minute"),
        ("5m", "5 minutes"),
        ("15m", "15 minutes"),
        ("30m", "30 minutes"),
        ("1h", "1 hour")]

    return windows

def define_schema():
    schema = StructType([
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("timestamp", TimestampType())
    ])
    
    return schema

def read_from_kafka(spark):
    df_raw = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC_RAW) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "100") \
        .load()
    
    return df_raw

def parse_json(df, schema):
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    parsed_df = parsed_df.withColumn(
        "timestamp", 
        to_timestamp(col("timestamp"))
    )
    
    return parsed_df

def process_window(parsed_df, window_name, window_duration):
    df_with_watermark = parsed_df.withWatermark("timestamp", "10 seconds")
    window_spec = window(col("timestamp"), window_duration)
    window_stats = df_with_watermark \
        .groupBy(
            col("symbol"),
            window_spec.alias("window")
        ) \
        .agg(
            avg("price").alias("avg_price"),
            stddev("price").alias("std_price")
        ) \
        .select(
            col("symbol"),
            col("window.end").alias("timestamp"),
            lit(window_name).alias("window"),
            col("avg_price"),
            col("std_price")
        )
    
    return window_stats

def combine_windows(spark, *window_dfs):
    if not window_dfs:
        return None
    
    union_df = window_dfs[0]
    for df in window_dfs[1:]:
        union_df = union_df.unionAll(df)
    
    return union_df

def format_output(union_df):
    formatted_df = union_df \
        .groupBy("timestamp", "symbol") \
        .agg(
            collect_list(
                struct(
                    col("window"),
                    col("avg_price"),
                    col("std_price")
                )
            ).alias("windows")
        ) \
        .select(
            col("timestamp"),
            col("symbol"),
            col("windows"),
            to_json(
                struct(
                    col("timestamp"),
                    col("symbol"),
                    col("windows")
                )
            ).alias("value")
        )
    
    return formatted_df

def main():
    spark = create_spark_session()
    schema = define_schema()
    raw_df = read_from_kafka(spark)
    parsed_df = parse_json(raw_df, schema)
    windows = define_windows()
    window_dfs = []
    
    for window_name, window_duration in windows:
        window_df = process_window(parsed_df, window_name, window_duration)
        window_dfs.append(window_df)
    
    combined_df = combine_windows(spark, *window_dfs)
    output_df = format_output(combined_df)
    query = output_df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", KAFKA_TOPIC_MOVING) \
        .option("checkpointLocation", "./checkpoints/moving") \
        .outputMode("update") \
        .start()
    query.awaitTermination()

if __name__ == "__main__":
    main()