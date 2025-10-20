// src/db.js
import { Pool } from "pg";
import { Connector } from "@google-cloud/cloud-sql-connector";

const {
  INSTANCE_CONNECTION_NAME,
  DB_USER,
  DB_PASS,
  DB_NAME,
} = process.env;

let pool;
export async function getDb() {
  if (pool) return pool;
  const connector = new Connector();
  const clientOpts = await connector.getOptions({
    instanceConnectionName: INSTANCE_CONNECTION_NAME,
    ipType: "PUBLIC",
  });
  pool = new Pool({
    ...clientOpts,
    user: DB_USER,
    password: DB_PASS,
    database: DB_NAME,
    max: 5,
  });
  return pool;
}
