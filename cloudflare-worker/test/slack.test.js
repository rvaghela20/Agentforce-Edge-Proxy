import { describe, it, expect } from "vitest";
import { buildDashboardCard } from "../src/slack.js";

describe("buildDashboardCard", () => {
  it("handles empty stats", () => {
    const card = buildDashboardCard({});
    expect(card.text).toContain("Command Room");
    expect(card.blocks.length).toBe(3);
  });

  it("calculates hit ratio correctly", () => {
    const card = buildDashboardCard({ cache_hits: "80", cache_misses: "20" });
    const fields = card.blocks[1].fields;
    const hitField = fields.find((f) => f.text.includes("Cache Hit Ratio"));
    expect(hitField.text).toContain("80.0%");
  });

  it("shows 0% when no requests", () => {
    const card = buildDashboardCard({ cache_hits: "0", cache_misses: "0" });
    const fields = card.blocks[1].fields;
    const hitField = fields.find((f) => f.text.includes("Cache Hit Ratio"));
    expect(hitField.text).toContain("0.0%");
  });

  it("formats cost in USD", () => {
    const card = buildDashboardCard({ total_cost_micro_usd: "1500000" });
    const fields = card.blocks[1].fields;
    const costField = fields.find((f) => f.text.includes("Cost"));
    expect(costField.text).toContain("$1.5000");
  });

  it("includes action buttons", () => {
    const card = buildDashboardCard({});
    const actions = card.blocks[2];
    expect(actions.type).toBe("actions");
    expect(actions.elements.length).toBe(3);
    const ids = actions.elements.map((e) => e.action_id);
    expect(ids).toContain("purge_cache");
    expect(ids).toContain("simulate_attack");
    expect(ids).toContain("run_diagnostics");
  });
});
