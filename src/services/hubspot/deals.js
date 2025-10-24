// src/services/hubspot/deals.js
const HS_BASE = "https://api.hubapi.com";

export const HS_DEAL_PROPS = [
  "balance_paid",
  "brand_hearing_aid_fitted",
  "utm_campaign",
  "utm_medium",
  "closedate",
  "closed_by",
  "closing_reason",
  "contact_gcid",
  "createdate",
  "hs_created_by_user_id",
  "cross_hearing_aid",
  "dealname",
  "hubspot_owner_id",
  "dealstage",
  "dealtype",
  "recent_hearing_test",
  "expected_revenue",
  "final_price",
  "financing_details",
  "fitter_id",
  "full_name_of_hearing_aid",
  "how_soon",
  "currently_using_hearing_aids",
  "hs_lastmodifieddate",
  "list_price",
  "main_motivation",
  "notes",
  "owner_id",
  "purchase_date",
  "hs_object_id",
  "utm_source",
  "creation_source_id",
  "style_hearing_aid_fitted",
  "tech_level_future_hearing_aids",
  "tech_level_hearing_aid_fitted",
  "type_of_future_hearing_aids",
  "hs_updated_by_user_id",
  "why_now",
];

export async function fetchRecentDealsFromHubSpot({
  hours = 4,
  overlapMinutes = 10,
  properties = HS_DEAL_PROPS,
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
              propertyName: "hs_lastmodifieddate",
              operator: "GTE",
              value: since,
            },
          ],
        },
      ],
      sorts: [{ propertyName: "hs_lastmodifieddate", direction: "ASCENDING" }],
      properties,
      limit: pageSize,
      after,
    };

    const r = await fetch(`${HS_BASE}/crm/v3/objects/deals/search`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });
    if (!r.ok)
      throw new Error(
        `HubSpot ${r.status}: ${await r.text().catch(() => r.statusText)}`
      );

    const data = await r.json();
    for (const d of data.results || []) {
      // Already filtered by HubSpot; flatten to a single object if you like:
      results.push({ id: String(d.id), ...d.properties });
    }
    after = data?.paging?.next?.after;
    pages++;
  } while (after);

  return { since, pages, count: results.length, results };
}
