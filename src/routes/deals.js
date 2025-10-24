// src/routes/deals.js
import express from "express";
import { getDb } from "../db.js";
import {
  createDeal,
  updateDeal,
  // getDealByHubspotId,   // not used here
  // deleteDealByHubspotId // not used here
} from "../queries/deals.js";
import { fetchRecentDealsFromHubSpot } from "../services/hubspot/deals.js";

const router = new express.Router();

// tiny helpers
const clean = (v) => (v == null ? null : (typeof v === "string" ? (v.trim() || null) : v));

function hsToDb(p) {
  return {
    // IDs & timestamps
    hubspot_id: String(p.id),
    hubspot_owner_id:           /* string */   clean(p.hubspot_owner_id),
    hubspot_created_at:         /* timestamptz */ clean(p.createdate),
    hubspot_updated_at:         /* timestamptz */ clean(p.hs_lastmodifieddate),
    hubspot_created_by_user_id: clean(p.hs_created_by_user_id),
    hubspot_updated_by_user_id: clean(p.hs_updated_by_user_id),

    // core deal
    dealname:        clean(p.dealname),
    dealtype:        clean(p.dealtype),
    dealstage:       clean(p.dealstage),
    closing_reason:  clean(p.closing_reason),
    closedate:       /* timestamptz */  clean(p.closedate),
    expected_revenue:/* numeric   */    clean(p.expected_revenue),
    notes:                             clean(p.notes),

    // marketing / source
    utm_campaign:    clean(p.utm_campaign),
    utm_medium:      clean(p.utm_medium),
    utm_source:      clean(p.utm_source),
    source_id:       /* text */         clean(p.creation_source_id),

    // links (contact_gcid is now nullable; we will insert even if null)
    owner_id:        /* int   */        clean(p.owner_id),
    contact_gcid:    /* uuid  */        clean(p.contact_gcid),
    fitter_id:       /* int   */        clean(p.fitter_id),

    // hearing-aid details
    type_of_future_hearing_aids:      clean(p.type_of_future_hearing_aids),
    tech_level_future_hearing_aids:   clean(p.tech_level_future_hearing_aid_fitted ?? p.tech_level_future_hearing_aids),
    brand_hearing_aid_fitted:         clean(p.brand_hearing_aid_fitted),
    tech_level_hearing_aid_fitted:    clean(p.tech_level_hearing_aid_fitted),
    style_hearing_aid_fitted:         clean(p.style_hearing_aid_fitted),
    cross_hearing_aid:                clean(p.cross_hearing_aid),
    full_name_of_hearing_aid:         clean(p.full_name_of_hearing_aid),

    // money & dates
    list_price:        /* numeric */    clean(p.list_price),
    final_price:       /* numeric */    clean(p.final_price),
    balance_paid:      /* boolean */    clean(p.balance_paid),
    financing_details:                 clean(p.financing_details),
    purchase_date:     /* date    */    clean(p.purchase_date),

    // context
    why_now:                         clean(p.why_now),
    main_motivation:                 clean(p.main_motivation),
    currently_using_hearing_aids:    clean(p.currently_using_hearing_aids),
    how_soon:                        clean(p.how_soon),
    recent_hearing_test:             clean(p.recent_hearing_tes ?? p.recent_hearing_test),
  };
}

// POST /deals/sync?dryrun=true&hours=4&overlapMinutes=10&pageSize=100&mode=incremental|force
router.post("/sync", async (req, res) => {
  try {
    const hours          = req.query.hours          !== undefined ? Number(req.query.hours)          : 4;
    const overlapMinutes = req.query.overlapMinutes !== undefined ? Number(req.query.overlapMinutes) : 10;
    const pageSize       = req.query.pageSize       !== undefined ? Number(req.query.pageSize)       : 100;
    const dryRun         = String(req.query.dryrun ?? "").toLowerCase() === "true"; // simple
    const mode           = String(req.query.mode ?? "incremental").toLowerCase() === "force" ? "force" : "incremental";

    // 1) pull from HubSpot
    const { since, pages, results = [] } = await fetchRecentDealsFromHubSpot({ hours, overlapMinutes, pageSize });
    const hsMap = new Map(results.map(r => [String(r.id), r]));
    const ids   = [...hsMap.keys()];

    if (ids.length === 0) {
      return res.json({
        ok: true, since, pages, total: 0, mode,
        partitions: { to_insert: 0, to_update: 0, unchanged: 0 },
        to_insert_ids: [], to_update_ids: [], unchanged: [],
        applied: { inserted: 0, updated: 0, skipped_inserts: 0, skipped_insert_ids: [] },
        errors: [],
      });
    }

    // 2) DB lookup (only what we need)
    const db = await getDb();
    const { rows } = await db.query(
      `SELECT hubspot_id, hubspot_updated_at
         FROM hello_hearing.deals
        WHERE hubspot_id = ANY($1)`,
      [ids]
    );
    const dbMap = new Map(rows.map(r => [String(r.hubspot_id), r.hubspot_updated_at]));

    // 3) partition into toInsert / toUpdate / unchanged
    const toInsertIds = [];
    const toUpdateIds = [];
    const unchanged   = [];

    for (const id of ids) {
      const hs    = hsMap.get(id);
      const hsTs  = hs?.hs_lastmodifieddate ? Date.parse(hs.hs_lastmodifieddate) : null;
      const dbIso = dbMap.get(id);
      const dbTs  = dbIso ? new Date(dbIso).getTime() : null;

      if (!dbMap.has(id)) {
        toInsertIds.push(id);
      } else if (mode === "force" || !dbTs || (hsTs && hsTs > dbTs)) {
        toUpdateIds.push(id);
      } else {
        unchanged.push({
          hubspot_id: id,
          hs_lastmodifieddate: hs?.hs_lastmodifieddate ?? null,
          db_lastmodifieddate: dbIso ?? null,
        });
      }
    }

    // 4) dry-run report (no writes)
    if (dryRun) {
      return res.json({
        ok: true, since, pages, total: results.length, mode,
        partitions: { to_insert: toInsertIds.length, to_update: toUpdateIds.length, unchanged: unchanged.length },
        to_insert_ids: toInsertIds,
        to_update_ids: toUpdateIds,
        unchanged,
        // keep shape for compatibility
        applied: { inserted: 0, updated: 0, skipped_inserts: 0, skipped_insert_ids: [] },
        errors: [],
      });
    }

    // 5) apply: updates first, then inserts
    let inserted = 0;
    let updated  = 0;
    // keep these for response shape (will stay zero/empty now that we insert without contact_gcid)
    let skipped_inserts = 0;
    const skipped_insert_ids = [];
    const errors = [];

    for (const id of toUpdateIds) {
      try {
        const patch = hsToDb(hsMap.get(id));
        const r = await updateDeal(id, patch);
        if (r?.updated) updated++;
      } catch (e) {
        errors.push({ hubspot_id: id, action: "update", error: String(e.message || e) });
      }
    }

    for (const id of toInsertIds) {
      const row = hsToDb(hsMap.get(id));
      try {
        await createDeal(row); // inserts allowed even if contact_gcid is null
        inserted++;
      } catch (e) {
        if (e?.code === "485" || e?.code === "23505") {
          // duplicate race → fallback to update
          try {
            const r = await updateDeal(id, row);
            if (r?. updated) updated++;
          } catch (e2) {
            errors.push({ hubspot_id: id, action: "update_after_duplicate", error: String(e2.message || e2) });
          }
        } else {
          errors.push({ hubspot_id: id, action: "insert", error: String(e.message || e) });
        }
      }
    }

    // 6) final report (consistent with dry-run)
    res.json({
      ok: true, since, pages, total: results.length, mode,
      partitions: { to_insert: toInsertIds.length, to_update: toUpdateIds.length, unchanged: unchanged.length },
      to_insert_ids: toInsertIds,
      to_update_ids: toUpdateIds,
      unchanged,
      // keep shape with zeros for backward compatibility
      applied: { inserted, updated, skipped_inserts, skipped_insert_ids },
      errors,
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

// POST /deals/delete-missing
router.post("/delete-missing", async (_req, res) => {
  try {
    const token = process.env.GOOGLE_CLOUD_PROJECT ? process.env.HUBSPOT_TOKEN : process.env.HUBSPOT_TOKEN;
    if (!token) return res.status(500).json({ error: "Missing HUBSPOT_TOKEN" });

    const db = await getDb();

    // 1) DB: id + hubspot_id
    const { rows } = await db.query(`
      SELECT id, hubspot_id
      FROM hello_hearing.deals
      WHERE hubspot_id IS NOT NULL
    `);

    // 2) HubSpot: collect ALL deal IDs (paginated)
    const hsIds = new Set();
    let after;
    do {
      const url = new URL("https://api.hubapi.com/crm/v3/objects/deals");
      url.searchParams.set("limit", "100");
      url.searchParams.set("archived", "false");
      if (after) url.searchParams.set("after", after);

      const r = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
      if (!r.ok) {
        const text = await r.text().catch(() => "");
        return res.status(502).json({ error: `HubSpot ${r.status}: ${text || r.statusText}` });
      }
      const data = await r.json();
      for (const it of data.results || []) hsIds.add(String(it.id));
      after = data?.ter?.next?.after ?? data?.paging?.next?.after;
    } while (after);

    // 3) Missing in HubSpot -> delete from DB
    const idsToDelete = rows.filter(r => !hsIds.has(String(r.hubspot_id))).map(r => r.id);

    if (idsToDelete.length === 0) {
      return res.json({ deleted: 0, deleted_ids: [] });
    }

    await db.query(`DELETE FROM hello_hearing.deals WHERE id = ANY($1)`, [idsToDelete]);
    res.json({ deleted: idsToDelete.length, deleted_ids: idsToDelete });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

// GET /deals  -> returns every deal (no pagination), newest first
router.get("/", async (_req, res) => {
  try {
    const db = await getDb();
    const { rows } = await db.query(
      `SELECT * FROM hello_hearing.deals ORDER BY created_at DESC`
    );
    res.json({ count: rows.length, results: rows });
  } catch (e) {
    console.error(e);
    return res.status(400).json({ error: String(e.message || e) });
  }
});

// GET /deals/h of last N hours from HubSpot, ordered by hs_lastmodifieddate DESC
router.get("/hubspot", async (req, res) => {
  try {
    const hours          = req.query.hours ? Number(req.query.hours) : 4;
    const overlapMinutes = req.query.overlapMinutes !== undefined ? Number(req.query.overlapMinutes) : 10;
    const pageSize       = req.query.pageSize !== undefined ? Number(req.query.pageSize) : 100;

    const out = await fetchRecentDealsFromHubSpot({ hours, overlapMinutes, pageSize });

    const results = (out.results || []).slice().sort((a, b) => {
      const ta = a.hs_lastmodifieddate ? Date.parse(a.hs_lastmodifieddate) : 0;
      const tb = b.hs_lastmodifieddate ? Date.parse(b.hs_lastmodifieddate) : 0;
      return tb - ta; // DESC
    });

    res.json({ ...out, results });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

export default router;
