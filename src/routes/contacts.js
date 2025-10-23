// src/routes/deals.js
import express from "express";
import { getDb } from "../db.js";

const router = express.Router();

// POST /contacts/delete-missing
router.post("/delete-missing", async (_req, res) => {
  try {
    const token = process.env.HUBSPOT_TOKEN;
    if (!token) return res.status(500).json({ error: "Missing HUBSPOT_TOKEN" });

    const db = await getDb();

    // 1) DB: id + hubspot_id
    const { rows } = await db.query(`
      SELECT gcid, hubspot_id
      FROM hello_hearing.contacts
      WHERE hubspot_id IS NOT NULL
    `);

    // 2) HubSpot: collect ALL deal IDs (paginated)
    const hsIds = new Set();
    let after;
    do {
      const url = new URL("https://api.hubapi.com/crm/v3/objects/contacts");
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
      for (const it of data.results || []) hsIds.add(String(it.id));
      after = data?.paging?.next?.after;
    } while (after);

    // 3) Missing in HubSpot -> delete from DB
    const idsToDelete = rows
      .filter((r) => !hsIds.has(String(r.hubspot_id)))
      .map((r) => r.gcid);

    if (idsToDelete.length === 0) {
      return res.json({ deleted: 0, deleted_ids: [] });
    }

    await db.query(`DELETE FROM hello_hearing.contacts WHERE gcid = ANY($1)`, [
      idsToDelete,
    ]);

    res.json({ deleted: idsToDelete.length, deleted_ids: idsToDelete });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

export default router;
