echo "Deleting btc-price..."
kafka-topics.sh --bootstrap-server localhost:9092 --topic btc-price --delete 2>/dev/null
if [ $? -ne 0 ]; then
    echo "btc-price not found."
fi

echo "Deleting btc-price-moving..."
kafka-topics.sh --bootstrap-server localhost:9092 --topic btc-price-moving --delete 2>/dev/null
if [ $? -ne 0 ]; then
    echo "btc-price-moving not found."
fi

echo "Deleting btc-price-zscore..."
kafka-topics.sh --bootstrap-server localhost:9092 --topic btc-price-zscore --delete 2>/dev/null
if [ $? -ne 0 ]; then
    echo "btc-price-zscore not found."
fi

echo "Done!"
sleep 10

echo "Creating btc-price..."
kafka-topics.sh --bootstrap-server localhost:9092 --topic btc-price --create --partitions 1 --replication-factor 1

echo "Creating btc-price-moving..."
kafka-topics.sh --bootstrap-server localhost:9092 --topic btc-price-moving --create --partitions 1 --replication-factor 1

echo "Creating btc-price-zscore..."
kafka-topics.sh --bootstrap-server localhost:9092 --topic btc-price-zscore --create --partitions 1 --replication-factor 1

echo "Done!"

PROJECT_DIR=$(pwd)
CHECKPOINTS_DIR="$PROJECT_DIR/checkpoints"
echo "Removing checkpoints directory at $CHECKPOINTS_DIR"
rm -rf "$CHECKPOINTS_DIR" || true