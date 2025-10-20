// index.js
import functions from "@google-cloud/functions-framework";
import { Pool } from "pg";
import { Connector } from "@google-cloud/cloud-sql-connector";
import "dotenv/config";

const INSTANCE_CONNECTION_NAME = process.env.INSTANCE_CONNECTION_NAME;
const DB_USER = process.env.DB_USER;
const DB_PASS = process.env.DB_PASS;
const DB_NAME = process.env.DB_NAME;

let pool;
async function getPool() {
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

functions.http("getSMSHistory", async (req, res) => {
  // CORS
  if (req.method === "OPTIONS") {
    res.set("Access-Control-Allow-Origin", "*");
    res.set("Access-Control-Allow-Methods", "GET, OPTIONS");
    res.set("Access-Control-Allow-Headers", "Content-Type");
    return res.status(204).send("");
  }
  res.set("Access-Control-Allow-Origin", "*");

  const raw = String(req.query.number || "").trim();
  if (!raw) return res.status(400).send("Missing number");

  try {
    const db = await getPool();

    const sql = `
      SELECT
        COALESCE(sent_at, created_at) AS timestamp,
        from_number  AS sender,
        to_number    AS receiver,
        direction,
        "text"       AS message
      FROM hello_hearing.sms_messages
      WHERE hello_hearing.norm_phone(from_number) = hello_hearing.norm_phone($1)
         OR hello_hearing.norm_phone(to_number)   = hello_hearing.norm_phone($1)
      ORDER BY COALESCE(sent_at, created_at) ASC
      LIMIT 500
    `;

    const { rows } = await db.query(sql, [raw]);
    return res.json(rows);
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String(e.message || e) });
  }
});
