// src/routes/deals.js
import express from "express";
import { getDb } from "../db.js";

import {
  createContact,
  updateContact,
  updateContactByGcid,
  // getContactByHubspotId,   // not used here
  // deleteContactByHubspotId // not used here
  listContacts,
} from "../queries/contacts.js";
import { fetchRecentContactsFromHubSpot } from "../services/hubspot/contacts.js";

const router = express.Router();

// tiny helpers
const clean = (v) => (v == null ? null : (typeof v === "string" ? (v.trim() || null) : v));

function hsToDb(p) {
  return {
    // IDs & timestamps
    hubspot_id: String(p.id),
    hubspot_created_at: clean(p.createdate),
    hubspot_updated_at: clean(p.lastmodifieddate),
    hubspot_created_by_user_id: clean(p.hs_created_by_user_id),
    hubspot_updated_by_user_id: clean(p.hs_updated_by_user_id),

    // PK (required by schema)
    gcid: clean(p.gcid),

    // Core Identity
    firstname: clean(p.firstname),
    middlename: clean(p.middlename),
    lastname: clean(p.lastname),
    salutation: clean(p.salutation),
    title: clean(p.title),
    birthday: clean(p.birthday),
    age: clean(p.age),
    record_type: clean(p.record_type),
    preferred_language: clean(p.preferred_language),
    veteran: clean(p.veteran),
    patient_deceased: clean(p.patient_deceased),
    credit_score: clean(p.credit_score),
    employment_status: clean(p.employment_status),

    // Contact info
    mobile: clean(p.mobile),
    landline: clean(p.landline),
    email: clean(p.email),
    timezone: clean(p.timezone),

    // Address
    address_1: clean(p.address_1),
    address_2: clean(p.address_2),
    city: clean(p.city),
    zip: clean(p.zip),
    state: clean(p.state),
    country: clean(p.country),

    // Consent
    express_written_consent: clean(p.express_written_consent),
    express_written_consent_date: clean(p.express_written_consent_date),
    hipaa_consent: clean(p.hipaa_consent),
    hipaa_consent_date: clean(p.hipaa_consent_date),
    sms_opt_in: clean(p.sms_opt_in),
    email_opt_in: clean(p.email_opt_in),

    // Insurance
    insurance_type: clean(p.insurance_type),
    insurance_company: clean(p.insurance_company),
    insurance_company_custom: clean(p.insurance_company_custom),
    insurance_member_id: clean(p.insurance_member_id),
    insurance_group_id: clean(p.insurance_group_id),
    insurance_benefit_administrator: clean(p.insurance_benefit_administrator),
    insurance_benefit: clean(p.insurance_benefit),
    insurance_benefit_renewal_time: clean(p.insurance_benefit_renewal_time),
    veteran_hearing_benefits: clean(p.veteran_hearing_benefits),

    // Marketing
    a_b_testing: clean(p.a_b_testing),
    engagement_score: clean(p.engagement_score),
    engagement_score_label: clean(p.engagement_score_label),
    ip_address: clean(p.ip_address),
    phone_os: clean(p.phone_os),
    segment: clean(p.segment),

    // Process
    patient_lifecycle_stage: clean(p.patient_lifecycle_stage),
    inquiry_for: clean(p.inquiry_for),
    hearing_aid_experience: clean(p.hearing_aid_experience),
    type_of_current_hearing_aids: clean(p.type_of_current_hearing_aids),
    brand_of_current_hearing_aids: clean(p.brand_of_current_hearing_aids),
    tech_level_of_current_hearing_aids: clean(p.tech_level_of_current_heearing_aids ?? p.tech_level_of_current_hearing_aids),
    model_of_current_hearing_aids: clean(p.model_of_current_hearing_aids),
    purchase_date_of_current_hearing_aids: clean(p.purchase_date_of_current_hearing_aids),
    wearing_time_of_current_hearing_aids: clean(p.wearing_time_of_current_hearing_aids),
    tinnitus: clean(p.tinnitus),
    main_motivation: clean(p.main_motivation),
    motivation_details: clean(p.motivation_details),
    right_loss: clean(p.right_loss),
    left_loss: clean(p.left_loss),

    // Tech
    test_flag: clean(p.test_flag),
    suno_id: clean(p.suno_id),
    sycle_id: clean(p.sycle_id),

  };
}

// POST /contacts/sync?dryrun=true&hours=4&overlapMinutes=10&pageSize=100&mode=incremental|force
router.post("/sync", async (req, res) => {
  try {
    const hours          = req.query.hours          !== undefined ? Number(req.query.hours)          : 4;
    const overlapMinutes = req.query.overlapMinutes !== undefined ? Number(req.query.overlapMinutes) : 10;
    const pageSize       = req.query.pageSize       !== undefined ? Number(req.query.pageSize)       : 100;
    const dryRun         = String(req.query.dryrun ?? "").toLowerCase() === "true";
    const mode           = String(req.query.mode ?? "incremental").toLowerCase() === "force" ? "force" : "incremental";

    // 1) pull from HubSpot
    const { since, pages, results = [] } = await fetchRecentContactsFromHubSpot({ hours, overlapMinutes, pageSize });
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

    // 2) DB lookup
    const db = await getDb();
    const { rows } = await db.query(
      `SELECT hubspot_id, hubspot_updated_at
         FROM hello_hearing.contacts
        WHERE hubspot_id = ANY($1)`,
      [ids]
    );
    const dbMap = new Map(rows.map(r => [String(r.hubspot_id), r.hubspot_updated_at]));

    // 3) partition into toInsert / toUpdate / unchanged
    const toInsertIds = [];
    const toUpdateIds = [];
    const unchanged   = [];

    for (const id of ids) {
      const hs   = hsMap.get(id);
      const hsLM = hs?.hs_lastmodifieddate ?? hs?.lastmodifieddate;
      const hsTs = hsLM ? Date.parse(hsLM) : null;

      const dbIso = dbMap.get(id);
      const dbTs  = dbIso ? new Date(dbIso).getTime() : null;

      if (!dbMap.has(id)) {
        toInsertIds.push(id);
      } else if (mode === "force" || !dbTs || (hsTs && hsTs > dbTs)) {
        toUpdateIds.push(id);
      } else {
        unchanged.push({
          hubspot_id: id,
          hs_lastmodifieddate: hsLM ?? null,
          db_lastmodifieddate: dbIso ?? null,
        });
      }
    }

    // 4) dry-run
    if (dryRun) {
      return res.json({
        ok: true, since, pages, total: results.length, mode,
        partitions: { to_insert: toInsertIds.length, to_update: toUpdateIds.length, unchanged: unchanged.length },
        to_insert_ids: toInsertIds,
        to_update_ids: toUpdateIds,
        unchanged,
        applied: { inserted: 0, updated: 0, skipped_inserts: 0, skipped_insert_ids: [] },
        errors: [],
      });
    }

    // 5) apply
    let inserted = 0, updated = 0, skipped_inserts = 0;
    const skipped_insert_ids = [];
    const errors = [];

    // updates
    for (const id of toUpdateIds) {
      try {
        const patch = hsToDb(hsMap.get(id));
        const r = await updateContact(id, patch);
        if (r?.updated) updated++;
      } catch (e) {
        errors.push({ hubspot_id: id, action: "update", error: String(e.message || e) });
      }
    }

    // inserts
    for (const id of toInsertIds) {
      const row = hsToDb(hsMap.get(id));
      // required by schema: gcid must be present
      const hasGcid = !!row.gcid;
      // likely needed by your CHECK constraint
      const hasPhone = !!row.mobile || !!row.landline;

      if (!hasGcid || !hasPhone) {
        skipped_inserts++; skipped_insert_ids.push(id);
        continue;
      }

      try {
        await createContact(row);
        inserted++;
      } catch (e) {
        if (e?.code === "23505") {
          // unique constraint -> try update by hubspot_id, then by gcid
          try {
            const r1 = await updateContact(id, row);
            if (!r1?.updated && row.gcid) {
              const r2 = await updateContactByGcid(row.gcid, row);
              if (r2?.updated) updated++;
            } else if (r1?.updated) {
              updated++;
            }
          } catch (e2) {
            errors.push({ hubspot_id: id, action: "update_after_duplicate", error: String(e2.message || e2) });
          }
        } else {
          errors.push({ hubspot_id: id, action: "insert", error: String(e.message || e) });
        }
      }
    }

    // 6) final report
    res.json({
      ok: true, since, pages, total: results.length, mode,
      partitions: { to_insert: toInsertIds.length, to_update: toUpdateIds.length, unchanged: unchanged.length },
      to_insert_ids: toInsertIds,
      to_update_ids: toUpdateIds,
      unchanged,
      applied: { inserted, updated, skipped_inserts, skipped_insert_ids },
      errors,
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

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

// GET /contacts  -> returns every deal (no pagination), newest first
router.get("/", async (_req, res) => {
  try {
    const db = await getDb();
    const { rows } = await db.query(
      `SELECT * FROM hello_hearing.contacts ORDER BY created_at DESC`
    );
    res.json({ count: rows.length, results: rows });
  } catch (e) {
    console.error(e);
    return res.status(400).json({ error: String(e.message || e) });
  }
});

// GET /contacts?hours=x of last N hours from HubSpot, ordered by hs_lastmodifieddate DESC
router.get("/hubspot", async (req, res) => {
  try {
    const hours          = req.query.hours ? Number(req.query.hours) : 4;
    const overlapMinutes = req.query.overlapMinutes !== undefined ? Number(req.query.overlapMinutes) : 10;
    const pageSize       = req.query.pageSize !== undefined ? Number(req.query.pageSize) : 100;

    const out = await fetchRecentContactsFromHubSpot({ hours, overlapMinutes, pageSize });

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
