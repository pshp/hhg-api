// src/services/hubspot/contacts.js
const HS_BASE = "https://api.hubapi.com";

export const HS_CONTACT_PROPS = [
  "timezone",
  "address_1",
  "address_2",
  "city",
  "zip",
  "state",
  "country",

  "express_written_consent",
  "express_written_consent_date",
  "hipaa_consent",
  "hipaa_consent_date",
  "sms_opt_in",
  "email_opt_in",

  "mobile",
  "landline",
  "email",

  "firstname",
  "middlename",
  "lastname",
  "birthday",
  "age",
  "salutation",
  "credit_score",
  "employment_status",
  "veteran",
  "patient_deceased",
  "title",
  "preferred_language",
  "record_type",

  "insurance_type",
  "insurance_company",
  "insurance_company_custom",
  "insurance_member_id",
  "insurance_group_id",
  "insurance_benefit_administrator",
  "insurance_benefit",
  "insurance_benefit_renewal_time",

  "engagement_score",
  "engagement_score_label",
  "a_b_testing",
  "ip_address",
  "phone_os",
  "segment",

  "patient_lifecycle_stage",
  "inquiry_for",
  "hearing_aid_experience",
  "type_of_current_hearing_aids",
  "brand_of_current_hearing_aids",
  "tech_level_of_current_hearing_aids",
  "model_of_current_hearing_aids",
  "purchase_date_of_current_hearing_aids",
  "wearing_time_of_current_hearing_aids",
  "tinnitus",
  "main_motivation",
  "motivation_details",
  "veteran_hearing_benefits",
  "right_loss",
  "left_loss",

  "hs_updated_by_user_id",
  "hs_created_by_user_id",

  // ids & timestamps
  "gcid",
  "createdate",
  "lastmodifieddate",
  "hs_object_id",

  // tech
  "suno_id",
  "test_flag",
  "sycle_id",
];

export async function fetchRecentContactsFromHubSpot({
  hours = 4,
  overlapMinutes = 10,
  properties = HS_CONTACT_PROPS,
  pageSize = 100,
} = {}) {
  const token = process.env.HUBSPOT_TOKEN;
  if (!token) throw new Error("Missing HUBSPOT_TOKEN");

  const since = new Date(
    Date.now() - (hours * 60 + overlapMinutes) * 60 * 1000
  ).toISOString();

  const results = [];
  let after,
    pages = 0;

  do {
    const body = {
      filterGroups: [
        {
          filters: [
            {
              propertyName: "lastmodifieddate",
              operator: "GTE",
              value: since,
            },
          ],
        },
      ],
      sorts: [{ propertyName: "createdate", direction: "ASCENDING" }],
      properties,
      limit: pageSize,
      after,
    };

    const r = await fetch(`${HS_BASE}/crm/v3/objects/contacts/search`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });
    if (!r.ok) {
      const text = await r.text().catch(() => r.statusText);
      throw new Error(`HubSpot ${r.status}: ${text}`);
    }

    const data = await r.json();
    for (const d of data.results || []) {
      results.push({ id: String(d.id), ...d.properties });
    }
    after = data?.paging?.next?.after;
    pages++;
  } while (after);

  return { since, pages, count: results.length, results };
}
