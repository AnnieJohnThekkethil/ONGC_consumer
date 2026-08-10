import dotenv from "dotenv";
import { Kafka } from "kafkajs";
import { Pool } from "pg";

dotenv.config();

type AssetMilestonePayload = {
  asset?: string;
  well_wellbore?: string;
  wellWellbore?: string;
  assigned_user?: string;
  user?: string;
  current_milestone?: string;
  currentMilestone?: string;
  milestone_date_time?: string;
  dateTime?: string;
  approval_level?: string;
  approvalLevel?: string;
  status?: string;
  days?: number | string;
  percent_complete?: number | string;
  percentComplete?: number | string;
};

const brokers = (process.env.KAFKA_BROKERS || "localhost:9092")
  .split(",")
  .map((item) => item.trim())
  .filter(Boolean);

const clientId = process.env.KAFKA_CLIENT_ID || "node-kafka-app";
const topic = process.env.ASSET_MILESTONES_TOPIC || "asset-milestones";
const groupId = process.env.ASSET_MILESTONES_GROUP_ID || "asset-milestones-db-writer";

const pool = new Pool({
  host: process.env.POSTGRES_HOST || "127.0.0.1",
  port: Number(process.env.POSTGRES_PORT || 5432),
  user: process.env.POSTGRES_USER || "postgres",
  password: process.env.POSTGRES_PASSWORD || "postgres123",
  database: process.env.POSTGRES_DB || "postgres"
});

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function ensureTable(): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS asset_milestones (
      id SERIAL PRIMARY KEY,
      asset TEXT,
      well_wellbore TEXT,
      assigned_user TEXT,
      current_milestone TEXT,
      milestone_date_time TIMESTAMPTZ,
      approval_level TEXT,
      status TEXT,
      days INTEGER,
      percent_complete NUMERIC,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

function parsePayload(rawValue: string): AssetMilestonePayload {
  const parsed = JSON.parse(rawValue) as unknown;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Kafka message must be a JSON object");
  }
  return parsed as AssetMilestonePayload;
}

async function insertPayload(payload: AssetMilestonePayload): Promise<number> {
  const result = await pool.query(
    `
      INSERT INTO asset_milestones (
        asset,
        well_wellbore,
        assigned_user,
        current_milestone,
        milestone_date_time,
        approval_level,
        status,
        days,
        percent_complete,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())
      RETURNING id
    `,
    [
      payload.asset ?? null,
      payload.well_wellbore ?? payload.wellWellbore ?? null,
      payload.assigned_user ?? payload.user ?? null,
      payload.current_milestone ?? payload.currentMilestone ?? null,
      payload.milestone_date_time
        ? new Date(payload.milestone_date_time)
        : payload.dateTime
          ? new Date(payload.dateTime)
          : null,
      payload.approval_level ?? payload.approvalLevel ?? null,
      payload.status ?? null,
      Number.isFinite(Number(payload.days)) ? Number(payload.days) : null,
      Number.isFinite(Number(payload.percent_complete ?? payload.percentComplete))
        ? Number(payload.percent_complete ?? payload.percentComplete)
        : null
    ]
  );

  return result.rows[0].id as number;
}

async function runConsumer(): Promise<void> {
  const kafka = new Kafka({ clientId, brokers });
  const consumer = kafka.consumer({ groupId });

  const shutdown = async (signal: string): Promise<void> => {
    console.log(`${signal} received. Shutting down consumer...`);
    await consumer.disconnect();
    await pool.end();
    process.exit(0);
  };

  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });
  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });

  await ensureTable();
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  console.log(`Consuming from topic ${topic} and writing to asset_milestones`);

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      try {
        if (!message.value) {
          throw new Error("Kafka message payload is empty");
        }

        const payload = parsePayload(message.value.toString());
        const insertedId = await insertPayload(payload);

        console.log({
          topic,
          partition,
          offset: message.offset,
          insertedId
        });
      } catch (error) {
        console.error("Skipping invalid Kafka message", {
          topic,
          partition,
          offset: message.offset,
          reason: toErrorMessage(error)
        });
      }
    }
  });
}

runConsumer().catch((error: unknown) => {
  console.error("Consumer failed:", error);
  process.exit(1);
});
