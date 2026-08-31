export async function postToSlack(env, payload) {
  const url = env.SLACK_WEBHOOK_URL;
  if (!url) {
    console.log("Slack not configured:", JSON.stringify(payload, null, 2));
    return;
  }
  try {
    await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
  } catch (err) {
    console.error("Slack send failed:", err);
  }
}

export function buildDashboardCard(stats) {
  const hits = parseInt(stats.cache_hits || "0", 10);
  const misses = parseInt(stats.cache_misses || "0", 10);
  const blocks = parseInt(stats.firewall_blocks || "0", 10);
  const totalCostMicro = parseInt(stats.total_cost_micro_usd || "0", 10);

  const totalReq = hits + misses;
  const hitRatio = totalReq > 0 ? ((hits / totalReq) * 100).toFixed(1) : "0.0";
  const costUsd = (totalCostMicro / 1000000).toFixed(4);

  return {
    text: "⚡ *Agentforce Edge Proxy Command Room* ⚡",
    blocks: [
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: "*⚡ Agentforce Edge Proxy Status Panel*\nActive caching, security firewall, and query warming."
        }
      },
      {
        type: "section",
        fields: [
          {
            type: "mrkdwn",
            text: `*Cache Hit Ratio:*\n📊 ${hitRatio}% (${hits} Hits / ${misses} Misses)`
          },
          { type: "mrkdwn", text: `*API Tokens Cost:*\n💵 $${costUsd} USD` },
          {
            type: "mrkdwn",
            text: `*Blocked Attacks:*\n🛡️ ${blocks} Threat blocks`
          },
          {
            type: "mrkdwn",
            text: `*Proxy Server:*\n🟢 Active (Cloudflare Workers)`
          }
        ]
      },
      {
        type: "actions",
        elements: [
          {
            type: "button",
            text: { type: "plain_text", text: "🧹 Purge Edge Cache" },
            style: "danger",
            action_id: "purge_cache"
          },
          {
            type: "button",
            text: { type: "plain_text", text: "🛡️ Simulate Attack" },
            action_id: "simulate_attack"
          },
          {
            type: "button",
            text: { type: "plain_text", text: "⚙️ Run Diagnostics" },
            action_id: "run_diagnostics"
          }
        ]
      }
    ]
  };
}
