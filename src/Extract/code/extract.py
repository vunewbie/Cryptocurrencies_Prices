from confluent_kafka import Producer
from dotenv import load_dotenv
from datetime import datetime, timezone
import os, time, websocket, json, threading

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW")
KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID_EXTRACT")
SYMBOL = os.getenv("BINANCE_SYMBOL").lower()
WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@trade"

latest_price = None
message_count = 0
start_minute = time.time()

def create_producer():
    return Producer({
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': KAFKA_CLIENT_ID,
    })

def get_rounded_iso_time(resolution_ms=100):
    now = datetime.now(timezone.utc)
    rounded_ts = int(now.timestamp() * 1000) // resolution_ms * resolution_ms
    rounded_dt = datetime.fromtimestamp(rounded_ts / 1000, tz=timezone.utc)

    return rounded_dt.isoformat(timespec='milliseconds')

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')
    else:
        print(f'Message delivered → {msg.value().decode()}')

def on_message(ws, message):
    global latest_price
    try:
        data = json.loads(message)
        latest_price = float(data['p'])
    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    print(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket Closed: {close_status_code}, {close_msg}")

def on_open(ws):
    print("WebSocket connection opened.")

def start_websocket():
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    threading.Thread(target=ws.run_forever, daemon=True).start()

def send_message():
    global message_count, start_minute
    start_websocket()

    producer = create_producer()
    interval = 0.1

    while True:
        loop_start = time.perf_counter()
        now = time.time()

        if latest_price is not None:
            event = {
                "symbol": SYMBOL.upper(),
                "price": latest_price,
                "timestamp": get_rounded_iso_time()
            }
            producer.produce(
                topic=KAFKA_TOPIC_RAW,
                key=SYMBOL.upper(),
                value=json.dumps(event),
                callback=delivery_report
            )
            producer.poll(0)
            message_count += 1

        if now - start_minute >= 60:
            print(f"\n\n\nMessages in the last minute: {message_count}\n\n\n")
            message_count = 0
            start_minute = now

        elapsed = time.perf_counter() - loop_start
        time.sleep(max(0.0, interval - elapsed))

def main():
    send_message()

if __name__ == "__main__":
    main()
