// src/routes/sms.js
import express from "express";
import { getDb } from "../db.js";

const router = express.Router();

// GET /sms/history?number=+15551234567
router.get("/history", async (req, res) => {
  const raw = String(req.query.number || "").trim();
  if (!raw) return res.status(400).send("Missing number");

  try {
    const db = await getDb();
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
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

export default router;
