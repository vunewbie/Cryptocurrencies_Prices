from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv
import os

load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_BTC_PRICE=os.getenv("KAFKA_TOPIC_BTC_PRICE")
KAFKA_TOPIC_BTC_PRICE_MOVING=os.getenv("KAFKA_TOPIC_BTC_PRICE_MOVING")
KAFKA_TOPIC_BTC_PRICE_ZSCORE=os.getenv("KAFKA_TOPIC_BTC_PRICE_ZSCORE")
KAFKA_CLIENT_ID_TRANSFORM_ZSCORE=os.getenv("KAFKA_CLIENT_ID_TRANSFORM_ZSCORE")
KAFKA_CONNECTOR=os.getenv("KAFKA_CONNECTOR")

# Spark Configuration
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL")
SPARK_ZSCORE_APP_NAME = os.getenv("SPARK_ZSCORE_APP_NAME")
SPARK_LOG_LEVEL = os.getenv("SPARK_LOG_LEVEL")
SPARK_TIMEZONE = os.getenv("SPARK_TIMEZONE")
SPARK_HADOOP_FS_DEFAULT = os.getenv("SPARK_HADOOP_FS_DEFAULT")
SPARK_STATEFUL_OPERATOR_CHECK = os.getenv("SPARK_STATEFUL_OPERATOR_CHECK")
SPARK_WATERMARK_DELAY = os.getenv("SPARK_WATERMARK_DELAY")
SPARK_MAX_OFFSETS_PER_TRIGGER = os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "100")

def create_spark_session():
    """Create Spark session"""
    try:
        spark = SparkSession.builder \
            .appName(SPARK_ZSCORE_APP_NAME) \
            .master(SPARK_MASTER_URL) \
            .config("spark.sql.session.timeZone", SPARK_TIMEZONE) \
            .config("spark.hadoop.fs.defaultFS", SPARK_HADOOP_FS_DEFAULT) \
            .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", SPARK_STATEFUL_OPERATOR_CHECK) \
            .config("spark.jars.packages", KAFKA_CONNECTOR) \
            .getOrCreate()
        spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)

        return spark
        
    except Exception as e:
        print(f"Failed to create Spark session: {e}")
        raise

def define_raw_schema():
    """Define expected schema for BTC price data from Kafka"""
    schema = StructType([
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("timestamp", TimestampType())
    ])
    
    return schema

def define_moving_schema():
    """Define expected schema for moving statistics data from Kafka."""
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
    """Read streaming price data"""
    raw_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_BTC_PRICE) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", SPARK_MAX_OFFSETS_PER_TRIGGER) \
        .load()
    raw_schema = define_raw_schema()
    
    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), raw_schema).alias("data")
    ).select("data.*")
    parsed_df = parsed_df.withWatermark("timestamp", SPARK_WATERMARK_DELAY)
    
    return parsed_df

def read_moving_stats_stream(spark):
    """Read streaming moving statistics"""
    moving_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_BTC_PRICE_MOVING) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", SPARK_MAX_OFFSETS_PER_TRIGGER) \
        .load()
    
    moving_schema = define_moving_schema()

    parsed_df = moving_df.select(
        from_json(col("value").cast("string"), moving_schema).alias("data")
    ).select("data.*")
    parsed_df = parsed_df.withWatermark("timestamp", SPARK_WATERMARK_DELAY)
    
    # Explode windows array for easier processing
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
    """
    Calculate Z-scores using stream-stream inner join with watermarking.
    
    Performs inner join between price and moving statistics streams based on
    timestamp and symbol matching. Implements edge case handling for zero
    standard deviation scenarios.
    
    Args:
        price_df: DataFrame with real-time price data from btc-price topic
        stats_df: DataFrame with moving statistics from btc-price-moving topic
        
    Returns:
        DataFrame: Z-scores for each window with columns [timestamp, symbol, window, zscore_price]
    """
    # Rename columns to avoid conflicts in join operation
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
    
    # Stream-stream inner join with timestamp and symbol matching
    joined_df = price_df.join(
        stats_df,
        (price_df.price_timestamp == stats_df.stats_timestamp) & 
        (price_df.price_symbol == stats_df.stats_symbol),
        "inner"
    )
    
    # Calculate Z-score with edge case handling for zero standard deviation
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
    """
    Format Z-score results according to required JSON structure.
    
    Groups Z-scores by timestamp and symbol, then formats as JSON for Kafka output
    according to lab specifications. Handles potential duplicates through aggregation.
    
    Args:
        zscore_df: DataFrame with Z-score calculations for each window
        
    Returns:
        DataFrame: Formatted output with JSON structure
    """
    # Remove potential duplicates by taking max Z-score per window
    deduplicated_df = zscore_df \
        .groupBy("timestamp", "symbol", "window") \
        .agg(
            max("zscore_price").alias("zscore_price")
        )
    
    # Group all windows' Z-scores by timestamp and symbol
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
    
    # Create final JSON structure for Kafka output
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
    """Write processed Z-score data to Kafka topic for downstream processing."""
    return df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .start()

def write_to_console(df):
    """Write processed Z-score data to console for monitoring and debugging."""
    return df.writeStream \
        .format("console") \
        .option("truncate", False) \
        .outputMode("append") \
        .start()

def main():
    spark = create_spark_session()
    
    # Read streaming data from both topics
    price_df = read_price_stream(spark)
    stats_df = read_moving_stats_stream(spark)
    
    # Calculate Z-scores with edge case handling
    zscore_df = calculate_zscores(price_df, stats_df)
    output_df = format_zscore_output(zscore_df)

    # Get absolute path to project root and create checkpoint path
    current_file = os.path.abspath(__file__)
    transform_code_dir = os.path.dirname(current_file)  # src/Transform/code
    transform_dir = os.path.dirname(transform_code_dir)  # src/Transform  
    src_dir = os.path.dirname(transform_dir)  # src
    project_root = os.path.dirname(src_dir)  # project root
    checkpoint_path = os.path.join(project_root, "checkpoints", "zscore")

    # Start streaming queries
    kafka_query = write_to_kafka(
        output_df,
        checkpoint_path=checkpoint_path,
        topic=KAFKA_TOPIC_BTC_PRICE_ZSCORE
    )
    console_query = write_to_console(output_df)

    # Keep the streaming application running
    kafka_query.awaitTermination()
    console_query.awaitTermination()

if __name__ == "__main__":
    main()
