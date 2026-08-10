Kafka Consumer Minimal Run Guide

Purpose
- Consume messages from Kafka topic asset-milestones
- Insert them into Postgres table asset_milestones


Minimal setup
1. Install packages
npm install

2. Start Kafka and Postgres
npm run kafka:up

3. Ensure .env has these values
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=node-kafka-app
ASSET_MILESTONES_TOPIC=asset-milestones
ASSET_MILESTONES_GROUP_ID=asset-milestones-db-writer
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres123
POSTGRES_DB=postgres

Run consumer
npm run consumer:replay

