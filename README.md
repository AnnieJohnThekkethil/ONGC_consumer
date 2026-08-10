Kafka Consumer Minimal Run Guide

Purpose
- Consume messages from Kafka topic asset-milestones
- Insert them into Postgres table asset_milestones

Which file runs
- npm run consumer uses src/consumer.ts
- npm run start uses dist/consumer.js (compiled output)

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
- Normal mode
npm run consumer

- Replay all existing topic data (new group each run)
npm run consumer:replay

Verify data in Postgres
- Row count
docker exec local-postgres psql -U postgres -d postgres -c "select count(*) from asset_milestones;"

- Latest rows
docker exec local-postgres psql -U postgres -d postgres -c "select id, asset, well_wellbore, assigned_user, current_milestone from asset_milestones order by id desc limit 10;"

Stop
- Stop consumer with Ctrl+C
- Stop containers
npm run kafka:down

Notes
- If you type consumer:reply by mistake, it fails. Use consumer:replay.
