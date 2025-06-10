if [[ "$1" == "--drop-db" ]]; then
  echo "Dropping MongoDB data volume!"
  docker volume rm 22120384_mongodb_data 2>/dev/null
else
  echo "MongoDB data will be preserved. Use --drop-db to delete it."
fi

echo "docker compose down -v"
docker compose down -v

echo "Removing Kafka/Spark logs and checkpoints"
rm -rf /tmp/kafka-logs /tmp/kraft-combined-logs src/__pycache__ || true

PROJECT_DIR=$(cd .. && pwd)
CHECKPOINTS_DIR="$PROJECT_DIR/checkpoints"
echo "Removing checkpoints directory at $CHECKPOINTS_DIR"
rm -rf "$CHECKPOINTS_DIR" || true

echo "Rebuilding Docker images..."
docker compose build

echo "Ready. Run: docker compose up -d"
docker compose up -d