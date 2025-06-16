from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import os
import json
import time

load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_BTC_PRICE_ZSCORE = os.getenv("KAFKA_TOPIC_BTC_PRICE_ZSCORE")
KAFKA_CLIENT_ID_LOAD = os.getenv("KAFKA_CLIENT_ID_LOAD")
KAFKA_CONNECTOR = os.getenv("KAFKA_CONNECTOR")

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION_PREFIX = os.getenv("MONGO_COLLECTION_PREFIX")
MONGO_CONNECTOR = os.getenv("MONGO_CONNECTOR")

# Spark Configuration
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL")
SPARK_LOAD_APP_NAME = os.getenv("SPARK_LOAD_APP_NAME")
SPARK_LOG_LEVEL = os.getenv("SPARK_LOG_LEVEL")
SPARK_TIMEZONE = os.getenv("SPARK_TIMEZONE")
SPARK_HADOOP_FS_DEFAULT = os.getenv("SPARK_HADOOP_FS_DEFAULT")
SPARK_STATEFUL_OPERATOR_CHECK = os.getenv("SPARK_STATEFUL_OPERATOR_CHECK")

def create_spark_session():
    spark = SparkSession.builder \
        .appName(SPARK_LOAD_APP_NAME) \
        .master(SPARK_MASTER_URL) \
        .config("spark.sql.session.timeZone", SPARK_TIMEZONE) \
        .config("spark.hadoop.fs.defaultFS", SPARK_HADOOP_FS_DEFAULT) \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", SPARK_STATEFUL_OPERATOR_CHECK) \
        .config("spark.jars.packages", f"{KAFKA_CONNECTOR},{MONGO_CONNECTOR}") \
        .getOrCreate()
    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
    return spark

def define_zscore_schema():
    zscore_item_schema = StructType([
        StructField("window", StringType()),
        StructField("zscore_price", DoubleType())
    ])
    
    schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("symbol", StringType()),
        StructField("zscores", ArrayType(zscore_item_schema))
    ])
    
    return schema

def read_zscore_stream(spark):
    zscore_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_BTC_PRICE_ZSCORE) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "100") \
        .load()
    
    schema = define_zscore_schema()
    parsed_df = zscore_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    watermarked_df = parsed_df.withWatermark("timestamp", "10 seconds")
    
    return watermarked_df

def create_mongodb_connection():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[MONGO_DB]
        print(f"Connected to MongoDB: {MONGO_URI}/{MONGO_DB}")
        return db
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        raise e

def upsert_to_mongodb(batch_df, epoch_id):
    if batch_df.count() == 0:
        print(f"Batch {epoch_id}: Empty batch, skipping...")
        return
    
    print(f"Processing batch {epoch_id} with {batch_df.count()} records")
    
    try:
        db = create_mongodb_connection()
        
        records = batch_df.collect()
        total_upserts = 0
        
        for record in records:
            timestamp = record.timestamp
            symbol = record.symbol
            zscores = record.zscores
            
            print(f"Processing record: {symbol} at {timestamp} with {len(zscores)} windows")
            
            for zscore_data in zscores:
                window = zscore_data.window
                zscore_price = zscore_data.zscore_price
                
                collection_name = f"{MONGO_COLLECTION_PREFIX}{window}"
                collection = db[collection_name]
                
                filter_criteria = {
                    "timestamp": timestamp,
                    "symbol": symbol
                }
                
                document = {
                    "timestamp": timestamp,
                    "symbol": symbol,
                    "window": window,
                    "zscore_price": zscore_price,
                    "created_at": datetime.utcnow(),
                    "batch_id": epoch_id,
                    "updated_count": 1
                }
                
                result = collection.replace_one(
                    filter_criteria,
                    document,
                    upsert=True
                )
                
                if result.upserted_id:
                    print(f"Inserted new record for {window}: {timestamp}")
                else:
                    print(f"Updated existing record for {window}: {timestamp}")
                    collection.update_one(
                        filter_criteria,
                        {
                            "$inc": {"updated_count": 1},
                            "$set": {"last_updated": datetime.utcnow()}
                        }
                    )
                
                total_upserts += 1
        
        print(f"Batch {epoch_id} completed: {total_upserts} upsert operations")
        
        print_collection_stats(db)
        
    except Exception as e:
        print(f"Error processing batch {epoch_id}: {e}")
        raise e

def print_collection_stats(db):
    windows = ["30s", "1m", "5m", "15m", "30m", "1h"]
    
    print("\nCollection Statistics:")
    print("-" * 50)
    
    for window in windows:
        collection_name = f"{MONGO_COLLECTION_PREFIX}{window}"
        try:
            collection = db[collection_name]
            count = collection.count_documents({})
            
            if count > 0:
                latest = collection.find().sort("timestamp", -1).limit(1)
                latest_record = list(latest)[0] if count > 0 else None
                latest_time = latest_record["timestamp"] if latest_record else "N/A"
                
                print(f"  {collection_name}: {count} records (latest: {latest_time})")
            else:
                print(f"  {collection_name}: 0 records")
                
        except Exception as e:
            print(f"  {collection_name}: Error - {e}")
    
    print("-" * 50)

def write_to_console(df):
    return df.writeStream \
        .format("console") \
        .option("truncate", False) \
        .outputMode("append") \
        .start()

def main():   
    spark = create_spark_session()
    zscore_df = read_zscore_stream(spark)
    
    print("Setting up MongoDB stream...")
    
    # Get absolute path to project root and create checkpoint path
    current_file = os.path.abspath(__file__)
    load_code_dir = os.path.dirname(current_file)  # src/Load/code
    load_dir = os.path.dirname(load_code_dir)  # src/Load  
    src_dir = os.path.dirname(load_dir)  # src
    project_root = os.path.dirname(src_dir)  # project root
    checkpoint_path = os.path.join(project_root, "checkpoints", "load")
    
    # Use foreachBatch with upsert logic to prevent duplicates
    # This combines MongoDB Spark Connector availability with upsert functionality
    mongodb_query = zscore_df.writeStream \
        .foreachBatch(upsert_to_mongodb) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .trigger(processingTime='5 seconds') \
        .start()
    
    print("Load stage started successfully!")
    print("Monitoring Z-score data loading to MongoDB...")
    print("Check MongoDB collections for data verification")
    print(f"MongoDB Query ID: {mongodb_query.id}")
    
    try:
        print("Waiting for streams to process data...")
        
        while mongodb_query.isActive:
            if mongodb_query.exception():
                print(f"MongoDB Query Exception: {mongodb_query.exception()}")
                break
        
        mongodb_query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nStopping Load stage...")
        mongodb_query.stop()
        print("Load stage stopped successfully!")
    except Exception as e:
        print(f"Unexpected error: {e}")
        mongodb_query.stop()
        raise e

if __name__ == "__main__":
    main()
