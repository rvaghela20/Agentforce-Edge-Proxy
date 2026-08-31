import { describe, it, expect } from "vitest";
import { getRedisClient, safeRedisCall } from "../src/redis-client.js";

describe("getRedisClient", () => {
  it("returns mock when no creds", () => {
    const client = getRedisClient({});
    expect(client).toBeDefined();
    expect(typeof client.get).toBe("function");
    expect(typeof client.pipeline).toBe("function");
  });

  it("returns mock when creds are empty strings", () => {
    const client = getRedisClient({
      UPSTASH_REDIS_REST_URL: "  ",
      UPSTASH_REDIS_REST_TOKEN: ""
    });
    expect(typeof client.get).toBe("function");
  });

  it("mock get returns null", async () => {
    const client = getRedisClient({});
    expect(await client.get("anything")).toBe(null);
  });

  it("mock setex returns OK", async () => {
    const client = getRedisClient({});
    expect(await client.setex("k", 60, "v")).toBe("OK");
  });

  it("mock pipeline chains and executes", async () => {
    const client = getRedisClient({});
    const pipe = client.pipeline();
    const chained = pipe.zincrby("k", 1, "m").hincrby("k", "f", 1);
    expect(chained).toBe(pipe); // chainable
    const result = await pipe.exec();
    expect(Array.isArray(result)).toBe(true);
  });

  it("mock sismember returns 0", async () => {
    const client = getRedisClient({});
    expect(await client.sismember("set", "member")).toBe(0);
  });

  it("mock hgetall returns empty object", async () => {
    const client = getRedisClient({});
    const result = await client.hgetall("key");
    expect(result).toEqual({});
  });
});

describe("safeRedisCall", () => {
  it("returns value on success", async () => {
    const result = await safeRedisCall(Promise.resolve("ok"), "test");
    expect(result).toBe("ok");
  });

  it("returns null on error", async () => {
    const result = await safeRedisCall(
      Promise.reject(new Error("boom")),
      "test"
    );
    expect(result).toBe(null);
  });
});
