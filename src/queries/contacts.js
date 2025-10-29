// src/queries/contacts.js
import { Kysely, PostgresDialect, sql } from "kysely";
import { validate as isUuid } from "uuid";
import { getDb } from "../db.js";

// casters
const nil = (v) =>
  v === undefined || v === null || (typeof v === "string" && v.trim() === "")
    ? null
    : v;
const s = (v) => nil(v);
const u = (v) => (nil(v) === null ? null : sql`${v}::uuid`);
const n = (v) => (nil(v) === null ? null : sql`${v}::numeric`);
const ts = (v) => (nil(v) === null ? null : sql`${v}::timestamptz`);
const b = (v) => (nil(v) === null ? null : sql`${v}::boolean`);

const CAST = {
  // PK
  gcid: u,

  // Core Identity
  firstname: s,
  middlename: s,
  lastname: s,
  salutation: s,
  title: s,
  birthday: ts,
  age: n,
  record_type: s,
  preferred_language: s,
  veteran: b,
  patient_deceased: b,
  credit_score: n,
  employment_status: s,

  // Contact info
  mobile: s,
  landline: s,
  email: s,
  timezone: s,

  // Address
  address_1: s,
  address_2: s,
  city: s,
  zip: s,
  state: s,
  country: s,

  // Consent
  express_written_consent: b,
  express_written_consent_date: ts,
  hipaa_consent: b,
  hipaa_consent_date: ts,
  sms_opt_in: b,
  email_opt_in: b,

  // Insurance
  insurance_type: s,
  insurance_company: s,
  insurance_company_custom: s,
  insurance_member_id: s,
  insurance_group_id: s,
  insurance_benefit_administrator: s,
  insurance_benefit: s,
  insurance_benefit_renewal_time: ts,
  veteran_hearing_benefits: s,

  // Marketing
  a_b_testing: s,
  engagement_score: n,
  engagement_score_label: s,
  ip_address: s,
  phone_os: s,
  segment: s,
  overflow_timeout: s,
  overflow_timeout_timestamp: ts,

  // Process
  patient_lifecycle_stage: s,
  inquiry_for: s,
  hearing_aid_experience: s,
  type_of_current_hearing_aids: s,
  brand_of_current_hearing_aids: s,
  tech_level_of_current_hearing_aids: s,
  model_of_current_hearing_aids: s,
  purchase_date_of_current_hearing_aids: ts,
  wearing_time_of_current_hearing_aids: n,
  tinnitus: s,
  main_motivation: s,
  motivation_details: s,
  right_loss: s,
  left_loss: s,

  // Tech
  test_flag: b,
  suno_id: s,
  sycle_id: s,

  // HubSpot sync
  hubspot_id: s,
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
export async function createContact(data) {
  const db = await dbWithSchema();
  const values = normalize(data);
  return db
    .insertInto("contacts")
    .values(values)
    .returning(["hubspot_id"])
    .executeTakeFirst(); // { hubspot_id }
}

// UPDATE by gcid
export async function updateContactByGcid(gcid, patch) {
  const db = await dbWithSchema();
  const set = normalize(patch);
  const row = await db
    .updateTable("contacts")
    .set(set)
    .where("gcid", "=", gcid)
    .returning(["hubspot_id"])
    .executeTakeFirst();
  return { updated: !!row, hubspot_id: row?.hubspot_id };
}

// UPDATE by hubspot_id
export async function updateContactByHubspotId(hubspotId, patch) {
  const db = await dbWithSchema();
  const set = normalize(patch);
  const row = await db
    .updateTable("contacts")
    .set(set)
    .where("hubspot_id", "=", String(hubspotId))
    .returning(["hubspot_id"])
    .executeTakeFirst();
  return { updated: !!row, hubspot_id: row?.hubspot_id };
}

// UPDATE by either id
export async function updateContactByEither(id, patch) {
  const byUuid = isUuid(id);
  const r = byUuid
    ? await updateContactByGcid(id, patch)
    : await updateContactByHubspotId(id, patch);
  if (!r?.updated) return { updated: false };
  const row = byUuid
    ? await getContactByGcid(id)
    : await getContactByHubspotId(id);
  return { updated: true, row };
}

// READ by gcid
export async function getContactByGcid(gcid) {
  const db = await dbWithSchema();
  return db
    .selectFrom("contacts")
    .selectAll()
    .where("gcid", "=", String(gcid))
    .executeTakeFirst();
}

// READ by hubspot id
export async function getContactByHubspotId(hubspotId) {
  const db = await dbWithSchema();
  return db
    .selectFrom("contacts")
    .selectAll()
    .where("hubspot_id", "=", String(hubspotId))
    .executeTakeFirst();
}

// READ by either id
export async function getContactByEither(id) {
  return isUuid(id) ? getContactByGcid(id) : getContactByHubspotId(id);
}

export async function listContacts({ since, limit, offset } = {}) {
  const db = await dbWithSchema();
  let q = db
    .selectFrom("contacts")
    .selectAll()
    .orderBy("hubspot_created_at", "asc");

  if (since) q = q.where("hubspot_updated_at", ">=", since);
  if (typeof limit === "number") q = q.limit(limit);
  if (typeof offset === "number") q = q.offset(offset);

  return q.execute();
}

// DELETE
export async function deleteContactByHubspotId(hubspotId) {
  const db = await dbWithSchema();
  const row = await db
    .deleteFrom("contacts")
    .where("hubspot_id", "=", String(hubspotId))
    .returning(["hubspot_id"])
    .executeTakeFirst();
  return { deleted: !!row, hubspot_id: row?.hubspot_id };
}
