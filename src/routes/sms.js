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
        c.gcid,
        c.firstname,
        c.lastname,
        s.campaign_name,
        COALESCE(s.sent_at, s.created_at) AS date,
        CASE WHEN s.direction = 'outbound'
             THEN s.from_number
             ELSE s.to_number
        END AS hhg_number,
        CASE WHEN s.direction = 'outbound'
             THEN s.to_number
             ELSE s.from_number
        END AS customer_number,
        s.direction,
        s."text"
      FROM hello_hearing.sms_messages s
      LEFT JOIN hello_hearing.contacts c
      ON s.contact_gcid = c.gcid
      WHERE hello_hearing.norm_phone(from_number) = hello_hearing.norm_phone($1)
         OR hello_hearing.norm_phone(to_number)   = hello_hearing.norm_phone($1)
      ORDER BY COALESCE(s.sent_at, s.created_at) ASC
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
