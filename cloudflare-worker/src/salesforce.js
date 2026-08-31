import { SF_TOKEN_TTL } from "./constants.js";

// Mock CRM data for local dev
export function getMockSalesforceData(query) {
  const q = query.toLowerCase();
  if (q.includes("pricing") || q.includes("price")) {
    return JSON.stringify({
      source: "Salesforce CRM (Mock)",
      results: [
        { name: "Enterprise Support License", code: "SL-ENT-01", price: "$150/user/month", cached: true },
        { name: "Standard Support License", code: "SL-STD-02", price: "$75/user/month", cached: true },
        { name: "Developer Sandbox Addon", code: "SL-DEV-03", price: "$50/sandbox/month", cached: true }
      ]
    });
  }
  if (q.includes("sla") || q.includes("support")) {
    return JSON.stringify({
      source: "Salesforce CRM (Mock)",
      results: [
        { topic: "Gold SLA Tier", responseTime: "Under 1 Hour", channel: "Slack & Video Call" },
        { topic: "Silver SLA Tier", responseTime: "Under 4 Hours", channel: "Slack & Email" },
        { topic: "Bronze SLA Tier", responseTime: "Under 8 Hours", channel: "Email Only" }
      ]
    });
  }
  return JSON.stringify({
    source: "Salesforce CRM (Mock)",
    results: [
      { message: `No specific match for '${query}'. Returning general FAQ documentation index.` },
      { doc: "Standard Operating Procedures v4.2" },
      { doc: "Customer Success Onboarding Checklist" }
    ]
  });
}

export async function getSalesforceData(query, env, redis) {
  const canAuth = !!(
    env.SALESFORCE_ORG_URL &&
    env.SALESFORCE_CLIENT_ID &&
    env.SALESFORCE_CLIENT_SECRET &&
    env.SALESFORCE_USERNAME &&
    env.SALESFORCE_PASSWORD
  );

  if (!canAuth) return getMockSalesforceData(query);

  try {
    let accessToken = await redis.get("sf:token:salesforce");
    let orgUrl = env.SALESFORCE_ORG_URL.replace(/\/$/, "");

    if (!accessToken) {
      // OAuth password flow
      const tokenUrl = `${orgUrl}/services/oauth2/token`;
      const body = new URLSearchParams({
        grant_type: "password",
        client_id: env.SALESFORCE_CLIENT_ID,
        client_secret: env.SALESFORCE_CLIENT_SECRET,
        username: env.SALESFORCE_USERNAME,
        password: env.SALESFORCE_PASSWORD
      });

      const res = await fetch(tokenUrl, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: body.toString()
      });

      if (!res.ok) {
        throw new Error(`OAuth failed: (${res.status}) ${await res.text()}`);
      }

      accessToken = (await res.json()).access_token;
      await redis.setex("sf:token:salesforce", SF_TOKEN_TTL, accessToken);
    }

    // SOQL query against Product2
    const cleanQuery = query.replace(/'/g, "\\'");
    const soql = `SELECT Name, ProductCode, Description FROM Product2 WHERE Name LIKE '%${cleanQuery}%' OR Description LIKE '%${cleanQuery}%' LIMIT 3`;
    const queryUrl = `${orgUrl}/services/data/v60.0/query?q=${encodeURIComponent(soql)}`;

    const qRes = await fetch(queryUrl, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json"
      }
    });

    if (!qRes.ok) {
      if (qRes.status === 401) await redis.del("sf:token:salesforce");
      throw new Error(`Query failed: (${qRes.status}) ${await qRes.text()}`);
    }

    const data = await qRes.json();
    return JSON.stringify({
      source: "Salesforce CRM (Live Query)",
      results: data.records.map((r) => ({
        name: r.Name,
        code: r.ProductCode,
        description: r.Description
      }))
    });
  } catch (err) {
    console.error("Salesforce error, falling back to mock:", err.message);
    return getMockSalesforceData(query);
  }
}
