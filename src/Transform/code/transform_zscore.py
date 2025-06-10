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
KAFKA_TOPIC_ZSCORE = os.getenv("KAFKA_TOPIC_ZSCORE")

def create_spark_session():
    spark = SparkSession.builder \
        .appName("BTCZScoreCalculation") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def define_raw_schema():
    schema = StructType([
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("timestamp", TimestampType())
    ])
    
    return schema

def define_moving_schema():
    window_schema = StructType([
        StructField("window", StringType()),
        StructField("avg_price", DoubleType()),
        StructField("std_price", DoubleType())
    ])
    
    schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("symbol", StringType()),
        StructField("windows", ArrayType(window_schema))
    ])
    
    return schema

def read_price_stream(spark):
    raw_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC_RAW) \
        .option("startingOffsets", "latest") \
        .load()
    raw_schema = define_raw_schema()
    
    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), raw_schema).alias("data")
    ).select("data.*")
    parsed_df = parsed_df.withWatermark("timestamp", "10 seconds")
    
    return parsed_df

def read_moving_stats_stream(spark):
    moving_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC_MOVING) \
        .option("startingOffsets", "latest") \
        .load()
    
    moving_schema = define_moving_schema()

    parsed_df = moving_df.select(
        from_json(col("value").cast("string"), moving_schema).alias("data")
    ).select("data.*")
    parsed_df = parsed_df.withWatermark("timestamp", "10 seconds")
    
    exploded_df = parsed_df.select(
        col("timestamp"),
        col("symbol"),
        explode(col("windows")).alias("window_data")
    ).select(
        col("timestamp"),
        col("symbol"),
        col("window_data.window").alias("window"),
        col("window_data.avg_price").alias("avg_price"),
        col("window_data.std_price").alias("std_price")
    )
    
    return exploded_df

def calculate_zscores(price_df, stats_df):
    price_df = price_df.select(
        col("timestamp").alias("price_timestamp"),
        col("symbol").alias("price_symbol"),
        col("price")
    )
    
    stats_df = stats_df.select(
        col("timestamp").alias("stats_timestamp"),
        col("symbol").alias("stats_symbol"),
        col("window"),
        col("avg_price"),
        col("std_price")
    )
    
    joined_df = price_df.join(
        stats_df,
        (price_df.price_timestamp == stats_df.stats_timestamp) & 
        (price_df.price_symbol == stats_df.stats_symbol),
        "inner"
    )
    
    zscore_df = joined_df.withColumn(
        "zscore_price",
        when(
            (col("std_price") > 0.0001), 
            (col("price") - col("avg_price")) / col("std_price")
        ).otherwise(lit(0.0))
    ).select(
        col("price_timestamp").alias("timestamp"),
        col("price_symbol").alias("symbol"),
        col("window"),
        col("zscore_price")
    )
    
    return zscore_df

def format_zscore_output(zscore_df):
    deduplicated_df = zscore_df \
        .groupBy("timestamp", "symbol", "window") \
        .agg(
            max("zscore_price").alias("zscore_price")
        )
    
    formatted_df = deduplicated_df \
        .groupBy("timestamp", "symbol") \
        .agg(
            collect_list(
                struct(
                    col("window"),
                    col("zscore_price")
                )
            ).alias("zscores")
        )
    
    output_df = formatted_df.select(
        col("timestamp"),
        col("symbol"),
        col("zscores"),
        to_json(
            struct(
                col("timestamp"),
                col("symbol"),
                col("zscores")
            )
        ).alias("value")
    )
    
    return output_df

def write_to_kafka(df, checkpoint_path, topic):
    return df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .start()

def write_to_console(df):
    return df.writeStream \
        .format("console") \
        .option("truncate", False) \
        .outputMode("append") \
        .start()

def main():
    spark = create_spark_session()
    price_df = read_price_stream(spark)
    stats_df = read_moving_stats_stream(spark)
    zscore_df = calculate_zscores(price_df, stats_df)
    output_df = format_zscore_output(zscore_df)

    kafka_query = write_to_kafka(
        output_df,
        checkpoint_path="../checkpoints/zscore",
        topic=KAFKA_TOPIC_ZSCORE
    )
    console_query = write_to_console(output_df)

    kafka_query.awaitTermination()
    console_query.awaitTermination()

if __name__ == "__main__":
    main()
