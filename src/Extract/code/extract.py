from confluent_kafka import Producer
from dotenv import load_dotenv
from datetime import datetime, timezone
import os, time, websocket, json, threading

load_dotenv()

# Binance Configuration
BINANCE_WS_BASE_URL=os.getenv("BINANCE_WS_BASE_URL")
BINANCE_SYMBOL=os.getenv("BINANCE_SYMBOL")
BINANCE_STREAM_TYPE=os.getenv("BINANCE_STREAM_TYPE")
WS_URL = f"{BINANCE_WS_BASE_URL}{BINANCE_SYMBOL.lower()}{BINANCE_STREAM_TYPE}"

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_BTC_PRICE=os.getenv("KAFKA_TOPIC_BTC_PRICE")
KAFKA_CLIENT_ID_EXTRACT=os.getenv("KAFKA_CLIENT_ID_EXTRACT")

# Extract Timing Configuration
EXTRACT_RESOLUTION_MS = int(os.getenv("EXTRACT_RESOLUTION_MS", 100))

# Global variables
latest_price = None        # Latest price received from WebSocket
message_count = 0          # Counter for throughput monitoring
start_minute = time.time() # Start time for throughput calculation

def create_producer():
    """Create Kafka producer for Extract stage."""
    try:
        producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": KAFKA_CLIENT_ID_EXTRACT,
        })
        
        print(f"Kafka producer created successfully for Extract stage. Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
        
        return producer
    except Exception as e:
        raise Exception(f"Failed to create Kafka producer for Extract stage: {e}")

def get_rounded_iso_time(resolution_ms=None):
    """
    Generate a rounded ISO 8601 timestamp aligned to the specified resolution.

    This function creates an event-time timestamp that is rounded down to the nearest
    multiple of the specified resolution in milliseconds. This ensures consistent
    event-time processing in the streaming pipeline.

    Args:
        resolution_ms (int, optional): The resolution in milliseconds to round down to.
                                     Must be a positive integer. If None, uses configured
                                     EXTRACT_RESOLUTION_MS from environment.

    Returns:
        str: ISO 8601 formatted timestamp string with milliseconds precision and UTC timezone.
             Format: "YYYY-MM-DDTHH:MM:SS.sssZ"

    Raises:
        ValueError: If resolution_ms is not a positive integer.
        Exception: If timestamp generation or formatting fails.

    Example:
        >>> get_rounded_iso_time()     # Uses configured resolution
        "2024-01-15T10:30:45.600Z"
        
        >>> get_rounded_iso_time(100)
        "2024-01-15T10:30:45.600Z"
        
        >>> get_rounded_iso_time(500)  
        "2024-01-15T10:30:45.500Z"
    """
    try:
        # Use configured resolution if not provided
        if resolution_ms is None:
            resolution_ms = EXTRACT_RESOLUTION_MS
            
        # Validate input
        if not isinstance(resolution_ms, int) or resolution_ms <= 0:
            raise ValueError(f"resolution_ms must be a positive integer, got: {resolution_ms}")
        
        # Get current UTC time
        now = datetime.now(timezone.utc)
        
        # Round down to nearest resolution multiple
        rounded_ts = int(now.timestamp() * 1000) // resolution_ms * resolution_ms
        
        # Convert back to datetime object
        rounded_dt = datetime.fromtimestamp(rounded_ts / 1000, tz=timezone.utc)
        
        # Format as ISO 8601 string
        iso_timestamp = rounded_dt.isoformat(timespec='milliseconds')
        
        return iso_timestamp
        
    except ValueError:
        # Re-raise validation errors
        raise
    except Exception as e:
        raise Exception(f"Failed to generate rounded ISO timestamp: {e}")

def delivery_report(err, msg):
    """Kafka producer delivery callback for monitoring message delivery status."""
    if err is not None:
        print(f'Delivery failed: {err}')
    else:
        print(f'Message delivered → {msg.value().decode()}')

# WebSocket event handlers
def on_message(ws, message):
    """
    WebSocket message handler for Binance trade stream data.
    
    Processes incoming trade data from Binance WebSocket stream and extracts
    the latest price information. Updates the global price state with the
    most recent trade price.
    
    Args:
        ws: WebSocket connection object
        message (str): JSON message string from Binance containing trade data
        
    Expected WebSocket message format:
        {
            "e": "trade",
            "s": "BTCUSDT", 
            "p": "45000.12",  # Price as string
            "q": "0.001",     # Quantity
            ...               # Other trade fields
        }
    """
    global latest_price
    try:
        # Parse JSON message from WebSocket
        data = json.loads(message)
        
        # Extract price from trade data ('p' field contains price as string)
        latest_price = float(data['p'])
        
    except json.JSONDecodeError as e:
        print(f"Failed to parse WebSocket message as JSON: {e}")
    except KeyError as e:
        print(f"Missing price field in WebSocket message: {e}")
    except ValueError as e:
        print(f"Invalid price format in WebSocket message: {e}")
    except Exception as e:
        print(f"Unexpected error processing WebSocket message: {e}")


def on_error(ws, error):
    """WebSocket error handler for connection issues."""
    print(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    """WebSocket close handler for connection termination."""
    print(f"WebSocket connection closed - Status: {close_status_code}, Message: {close_msg}")

def on_open(ws):
    """WebSocket open handler for successful connection establishment."""
    print(f"WebSocket connection established to {BINANCE_SYMBOL} trade stream")

def start_websocket():
    """Initialize and start WebSocket connection to Binance trade stream."""
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    threading.Thread(target=ws.run_forever, daemon=True).start()
    print(f"WebSocket thread started for URL: {WS_URL}")

# Sending messages to Kafka
def send_message():
    """
    Main Extract stage processing loop for cryptocurrency price streaming.
    
    This function implements the core Extract stage logic of the ETL pipeline:
    1. Establishes WebSocket connection to Binance trade stream
    2. Creates Kafka producer for message publishing
    3. Continuously sends price data to Kafka at configured intervals
    4. Monitors and reports performance metrics
    
    The function maintains precise timing according to EXTRACT_RESOLUTION_MS
    configuration and runs indefinitely until interrupted by the user.
    
    Process Flow:
    - WebSocket receives real-time trade data and updates latest_price
    - Main loop sends latest_price to Kafka every EXTRACT_RESOLUTION_MS
    - Timestamps are aligned to resolution boundaries for consistent event-time
    - Performance metrics are logged every minute
    
    Global Variables:
        message_count (int): Counter for throughput monitoring
        start_minute (float): Timestamp for throughput calculation window
        latest_price (float): Most recent price from WebSocket stream
        
    Raises:
        Exception: If Kafka producer creation fails
        KeyboardInterrupt: When user stops the process
    """
    global message_count, start_minute
    
    print("Starting Extract stage for cryptocurrency price streaming...")
    
    # Initialize WebSocket connection for real-time price data
    start_websocket()
    
    # Initialize Kafka producer for publishing price events
    producer = create_producer()
    
    # Calculate send interval from configured resolution (ms to seconds)
    interval = EXTRACT_RESOLUTION_MS / 1000.0

    print(f"Extract frequency: every {interval*1000}ms (resolution: {EXTRACT_RESOLUTION_MS}ms)")

    try:
        while True:
            # Start timing for precise interval control
            loop_start = time.perf_counter()
            now = time.time()

            # Send price data if available from WebSocket
            if latest_price is not None:
                # Create price event with aligned timestamp
                event = {
                    "symbol": BINANCE_SYMBOL,
                    "price": latest_price,
                    "timestamp": get_rounded_iso_time()
                }
                
                # Publish to Kafka with delivery confirmation
                producer.produce(
                    topic=KAFKA_TOPIC_BTC_PRICE,
                    key=BINANCE_SYMBOL,
                    value=json.dumps(event),
                    callback=delivery_report
                )
                
                # Process delivery callbacks
                producer.poll(0)
                message_count += 1

            # Report throughput metrics every minute
            if now - start_minute >= 60:
                print(f"\n\n\nMessages in the last minute: {message_count}\n\n\n")
                message_count = 0
                start_minute = now

            # Maintain precise timing interval
            elapsed = time.perf_counter() - loop_start
            sleep_time = max(0.0, interval - elapsed)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\nExtract stage stopped by user")
    except Exception as e:
        print(f"\nExtract stage failed with error: {e}")
        raise
    finally:
        # Ensure all messages are delivered before exit
        print("Flushing remaining messages...")
        producer.flush()
        print("Extract stage cleanup completed")

def main():
    send_message()

if __name__ == "__main__":
    main()