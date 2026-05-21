#!/bin/bash
# Kill any existing instances
pkill -f kafka_consumer.py 2>/dev/null
pkill -f kafka_producer.py 2>/dev/null
sleep 1

cd "$(dirname "$0")"

nohup python3 src/kafka_consumer.py >> /tmp/consumer.log 2>&1 &
echo "Consumer PID: $!"

nohup python3 src/kafka_producer.py >> /tmp/producer.log 2>&1 &
echo "Producer PID: $!"
