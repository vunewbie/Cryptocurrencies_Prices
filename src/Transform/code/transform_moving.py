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
KAFKA_CLIENT_ID_TRANSFORM_MOVING=os.getenv("KAFKA_CLIENT_ID_TRANSFORM_MOVING")
KAFKA_CONNECTOR=os.getenv("KAFKA_CONNECTOR")

# Spark Configuration
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL")
SPARK_MOVING_APP_NAME = os.getenv("SPARK_MOVING_APP_NAME")
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
            .appName(SPARK_MOVING_APP_NAME) \
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

def define_windows():
    """Define sliding window configurations for moving average calculations."""
    windows = [
        ("30s", "30 seconds"),
        ("1m", "1 minute"),
        ("5m", "5 minutes"),
        ("15m", "15 minutes"),
        ("30m", "30 minutes"),
        ("1h", "1 hour")]

    return windows

def define_schema():
    """Define expected schema for BTC price data from Kafka."""
    schema = StructType([
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("timestamp", TimestampType())
    ])
    
    return schema

def read_from_kafka(spark):
    """Read streaming data from Kafka topic"""
    df_raw = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_BTC_PRICE) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", SPARK_MAX_OFFSETS_PER_TRIGGER) \
        .load()
    
    return df_raw

def parse_json(df, schema):
    """Parse JSON data from Kafka and convert timestamp to proper format."""
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    parsed_df = parsed_df.withColumn(
        "timestamp", 
        to_timestamp(col("timestamp"))
    )
    
    return parsed_df

def process_window(parsed_df, window_name, window_duration):
    """
    Process windowed aggregations with configurable watermark delay.
    
    Creates sliding windows to calculate moving averages and standard deviations
    for each time window. Uses SPARK_WATERMARK_DELAY for late data handling.
    
    Args:
        parsed_df: DataFrame with parsed BTC price data
        window_name: Short name for the window (e.g., "30s", "1m")
        window_duration: Full duration string (e.g., "30 seconds", "1 minute")
        
    Returns:
        DataFrame: Windowed statistics with avg_price and std_price
    """
    df_with_watermark = parsed_df.withWatermark("timestamp", SPARK_WATERMARK_DELAY)
    
    # Define slide intervals for each window type
    slide_intervals = {
        "30 seconds": "10 seconds",
        "1 minute": "20 seconds", 
        "5 minutes": "1 minute",
        "15 minutes": "5 minutes",
        "30 minutes": "10 minutes",
        "1 hour": "20 minutes"
    }
    slide_interval = slide_intervals.get(window_duration, "10 seconds")
    
    window_spec = window(col("timestamp"), window_duration, slide_interval)
    
    # Calculate moving averages and standard deviations
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
    """Combine all window DataFrames into a single unified DataFrame."""
    if not window_dfs:
        return None
    
    union_df = window_dfs[0]
    for df in window_dfs[1:]:
        union_df = union_df.unionAll(df)
    
    return union_df

def format_output(union_df):
    """
    Format windowed statistics for Kafka output.
    
    Groups all window statistics by timestamp and symbol, then formats
    as JSON for downstream Z-score processing stage.
    """
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

def write_to_kafka(df, checkpoint_path, topic):
    """Write processed data to Kafka topic for downstream processing."""
    return df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("update") \
        .start()

def write_to_console(df):
    """Write processed data to console for monitoring and debugging."""
    return df.writeStream \
        .format("console") \
        .option("truncate", False) \
        .outputMode("update") \
        .start()

def main():
    spark = create_spark_session()
    schema = define_schema()
    
    # Read and parse streaming data
    raw_df = read_from_kafka(spark)
    parsed_df = parse_json(raw_df, schema)
    
    # Process all window configurations
    windows = define_windows()
    window_dfs = []

    for window_name, window_duration in windows:
        window_df = process_window(parsed_df, window_name, window_duration)
        window_dfs.append(window_df)

    # Combine and format output
    combined_df = combine_windows(spark, *window_dfs)
    output_df = format_output(combined_df)

    # Get absolute path to project root and create checkpoint path
    current_file = os.path.abspath(__file__)
    transform_code_dir = os.path.dirname(current_file)  # src/Transform/code
    transform_dir = os.path.dirname(transform_code_dir)  # src/Transform  
    src_dir = os.path.dirname(transform_dir)  # src
    project_root = os.path.dirname(src_dir)  # project root
    checkpoint_path = os.path.join(project_root, "checkpoints", "moving")

    # Start streaming queries
    kafka_query = write_to_kafka(
        output_df,
        checkpoint_path=checkpoint_path,
        topic=KAFKA_TOPIC_BTC_PRICE_MOVING
    )
    console_query = write_to_console(output_df)

    # Keep the streaming application running
    kafka_query.awaitTermination()
    console_query.awaitTermination()


if __name__ == "__main__":
    main()