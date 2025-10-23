import express from "express";
import { getDb } from "../db.js";

const router = express.Router();

const COLS = [
  "name", "description", "deal_type",
  "utm_source", "utm_campaign", "utm_medium",
  "variation"
];

// POST /campaigns  { data: { name, utm_source, utm_campaign, utm_medium, ... } }
router.post("/", async (req, res) => {
  try {
    const raw = req.body?.data || {};
    const data = {};
    Object.keys(raw).forEach(k => { if (k) data[k.toLowerCase()] = raw[k]; });

    const name = data.name?.toString().trim();
    if (!name) return res.status(400).json({ error: "Missing name" });

    for (const k of ["utm_source", "utm_campaign", "utm_medium"]) {
      if (!data[k] || String(data[k]).trim() === "") {
        return res.status(400).json({ error: `Missing ${k}` });
      }
    }

    const db = await getDb();

    const cols = COLS.filter(c => data[c] !== undefined);
    for (const k of ["name","utm_source","utm_campaign","utm_medium"]) {
      if (!cols.includes(k)) cols.push(k);
    }

    const vals = cols.map(c => data[c] ?? null);
    const ph   = cols.map((_, i) => `$${i + 1}`).join(", ");
    const set  = cols.filter(c => c !== "name").map(c => `${c}=EXCLUDED.${c}`).join(", ");

    const sql = `
      INSERT INTO hello_hearing.campaigns (${cols.join(", ")})
      VALUES (${ph})
      ON CONFLICT (name) DO UPDATE SET ${set}
      RETURNING id, name, utm_source, utm_campaign, utm_medium, variation, description, deal_type, created_at, updated_at
    `;

    const { rows } = await db.query(sql, vals);
    res.status(201).json({ ok: true, upserted: rows[0] });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

// DELETE /campaigns/:name
router.delete("/:name", async (req, res) => {
  try {
    const name = (req.params.name || "").toString().trim();
    if (!name) return res.status(400).json({ error: "Missing name in path" });

    const db = await getDb();

    const { rowCount } = await db.query("DELETE FROM hello_hearing.campaigns WHERE name = $1", [name]);
    // 204 No Content if deleted, 404 if not found
    if (rowCount === 0) return res.status(404).json({ error: "Not found" });
    return res.status(204).send();
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

export default router;
