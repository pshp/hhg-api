// src/routes/appointments.js
import express from "express";
import { getDb } from "../db.js";

const router = express.Router();

// GET /appointments/missing
router.get("/appointments/missing", async (_req, res) => {
  try {
    const token = process.env.HUBSPOT_TOKEN;
    if (!token) return res.status(500).json({ error: "Missing HUBSPOT_TOKEN" });

    // 1) DB: id + hubspot_id
    const db = await getDb();
    const { rows } = await db.query(
      `SELECT id, hubspot_id
       FROM hello_hearing.appointments
       WHERE hubspot_id IS NOT NULL`
    );

    // 2) HubSpot: collect ALL object IDs (paginated)
    const hsIds = new Set();
    let after;
    do {
      const url = new URL("https://api.hubapi.com/crm/v3/objects/APPOINTMENT");
      url.searchParams.set("limit", "100");
      url.searchParams.set("archived", "false");
      if (after) url.searchParams.set("after", after);

      const r = await fetch(url, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!r.ok) {
        const text = await r.text().catch(() => "");
        return res
          .status(502)
          .json({ error: `HubSpot ${r.status}: ${text || r.statusText}` });
      }
      const data = await r.json();
      for (const item of data.results || []) hsIds.add(String(item.id));
      after = data?.paging?.next?.after;
    } while (after);

    // 3) Diff: in DB but not in HubSpot
    const missing = rows.filter((r) => !hsIds.has(String(r.hubspot_id)));

    res.json(missing); // [{ id, hubspot_id }, ...]
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

export default router;
