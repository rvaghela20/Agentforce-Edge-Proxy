import { describe, it, expect } from "vitest";
import { getMockSalesforceData } from "../src/salesforce.js";

describe("getMockSalesforceData", () => {
  it("returns pricing data for 'pricing' query", () => {
    const result = JSON.parse(getMockSalesforceData("pricing"));
    expect(result.source).toContain("Mock");
    expect(result.results.length).toBe(3);
    expect(result.results[0].price).toBeDefined();
  });

  it("returns pricing data for 'price' query too", () => {
    const result = JSON.parse(getMockSalesforceData("what is the price"));
    expect(result.results[0]).toHaveProperty("price");
  });

  it("returns SLA data for 'support' query", () => {
    const result = JSON.parse(getMockSalesforceData("support options"));
    expect(result.results.length).toBe(3);
    expect(result.results[0]).toHaveProperty("responseTime");
  });

  it("returns SLA data for 'sla' query", () => {
    const result = JSON.parse(getMockSalesforceData("SLA tiers"));
    expect(result.results[0].topic).toContain("Gold");
  });

  it("returns FAQ fallback for unknown queries", () => {
    const result = JSON.parse(getMockSalesforceData("random stuff"));
    expect(result.results[0].message).toContain("No specific match");
  });

  it("is case insensitive", () => {
    const r1 = JSON.parse(getMockSalesforceData("PRICING"));
    const r2 = JSON.parse(getMockSalesforceData("pricing"));
    expect(r1.results.length).toBe(r2.results.length);
  });
});
