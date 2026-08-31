import { describe, it, expect } from "vitest";
import { estimateCost } from "../src/telemetry.js";

describe("estimateCost", () => {
  it("returns 0 for 0 tokens", () => {
    expect(estimateCost(0, 0)).toBe(0);
  });

  it("calculates with only input tokens", () => {
    const cost = estimateCost(1_000_000, 0);
    expect(cost).toBeCloseTo(0.15, 4);
  });

  it("calculates with only output tokens", () => {
    const cost = estimateCost(0, 1_000_000);
    expect(cost).toBeCloseTo(0.6, 4);
  });

  it("calculates mixed tokens", () => {
    const cost = estimateCost(500, 200);
    // 500 * 0.15/1M + 200 * 0.60/1M
    expect(cost).toBeGreaterThan(0);
    expect(cost).toBeLessThan(0.001);
  });
});
