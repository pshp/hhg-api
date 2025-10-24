import { Kysely, PostgresDialect, sql } from "kysely";
import { getDb } from "../db.js";

// casters: "", null, undefined -> NULL (+ basic types)
const nil = (v) =>
  v === undefined || v === null || (typeof v === "string" && v.trim() === "")
    ? null
    : v;
const s = (v) => nil(v);
const i = (v) => (nil(v) === null ? null : sql`${v}::int`);
const u = (v) => (nil(v) === null ? null : sql`${v}::uuid`);
const n = (v) => (nil(v) === null ? null : sql`${v}::numeric`);
const ts = (v) => (nil(v) === null ? null : sql`${v}::timestamptz`);
const d = (v) => (nil(v) === null ? null : sql`${v}::date`);
const b = (v) => (nil(v) === null ? null : sql`${v}::boolean`);

const CAST = {
  dealtype: s,
  dealname: s,
  dealstage: s,
  closing_reason: s,
  closedate: ts,
  expected_revenue: n,
  notes: s,

  utm_campaign: s,
  utm_medium: s,
  utm_source: s,

  owner_id: i,
  contact_gcid: u,
  fitter_id: i,

  type_of_future_hearing_aids: s,
  tech_level_future_hearing_aids: s,
  brand_hearing_aid_fitted: s,
  tech_level_hearing_aid_fitted: s,
  style_hearing_aid_fitted: s,
  cross_hearing_aid: s,
  full_name_of_hearing_aid: s,
  list_price: n,
  final_price: n,
  balance_paid: b,
  financing_details: s,
  purchase_date: d,

  why_now: s,
  main_motivation: s,
  currently_using_hearing_aids: s,
  how_soon: s,
  recent_hearing_test: s,

  source_id: s,

  hubspot_id: s,
  hubspot_owner_id: s,
  hubspot_created_at: ts,
  hubspot_updated_at: ts,
  hubspot_created_by_user_id: s,
  hubspot_updated_by_user_id: s,
};

const normalize = (input) => {
  const out = {};
  for (const [k, cast] of Object.entries(CAST)) {
    if (Object.prototype.hasOwnProperty.call(input, k)) out[k] = cast(input[k]);
  }
  return out;
};

const dbWithSchema = async () => {
  const pool = await getDb();
  const db = new Kysely({ dialect: new PostgresDialect({ pool }) });
  return db.withSchema("hello_hearing");
};

// CREATE
export async function createDeal(data) {
  const db = await dbWithSchema();
  const values = normalize(data);
  return db
    .insertInto("deals")
    .values(values)
    .returning(["hubspot_id"])
    .executeTakeFirst(); // { hubspot_id }
}

// UPDATE by hubspot_id (no guards)
export async function updateDeal(hubspotId, patch) {
  const db = await dbWithSchema();
  const set = normalize(patch);
  const row = await db
    .updateTable("deals")
    .set(set)
    .where("hubspot_id", "=", String(hubspotId))
    .returning(["hubspot_id"])
    .executeTakeFirst();
  return { updated: !!row, hubspot_id: row?.hubspot_id };
}

// READ
export async function getDealByHubspotId(hubspotId) {
  const db = await dbWithSchema();
  return db
    .selectFrom("deals")
    .selectAll()
    .where("hubspot_id", "=", String(hubspotId))
    .executeTakeFirst();
}

export async function listDeals({ since, limit, offset } = {}) {
  const db = await dbWithSchema();
  let q = db.selectFrom("deals").selectAll().orderBy("hubspot_created_at", "asc");

  if (since) q = q.where("hubspot_updated_at", ">=", since);
  if (typeof limit === "number") q = q.limit(limit);
  if (typeof offset === "number") q = q.offset(offset);

  return q.execute();
}

// DELETE
export async function deleteDealByHubspotId(hubspotId) {
  const db = await dbWithSchema();
  const row = await db
    .deleteFrom("deals")
    .where("hubspot_id", "=", String(hubspotId))
    .returning(["hubspot_id"])
    .executeTakeFirst();
  return { deleted: !!row, hubspot_id: row?.hubspot_id };
}
