#!/bin/bash
# Kill any existing instances
pkill -f kafka_consumer.py 2>/dev/null
pkill -f kafka_producer.py 2>/dev/null
pkill -f influx_consumer.py 2>/dev/null
sleep 1

cd "$(dirname "$0")"

# Neo4j consumer (graph state + snapshots + alerts)
nohup python3 src/neo4j/kafka_consumer.py >> /tmp/consumer.log 2>&1 &
echo "Neo4j Consumer PID: $!"

# InfluxDB consumer (time-series writes)
nohup python3 src/influxdb/influx_consumer.py >> /tmp/influx_consumer.log 2>&1 &
echo "InfluxDB Consumer PID: $!"

# Shared Kafka producer (sensor readings + approach sensors)
nohup python3 src/kafka_producer.py >> /tmp/producer.log 2>&1 &
echo "Producer PID: $!"
