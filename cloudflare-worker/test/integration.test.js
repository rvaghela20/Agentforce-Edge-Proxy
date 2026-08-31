import { describe, it, expect } from "vitest";
import worker from "../index.js";

function createMockCtx() {
  const promises = [];
  return {
    waitUntil(promise) {
      promises.push(promise);
    },
    promises
  };
}

describe("Worker fetch router integration", () => {
  const env = {
    PROXY_SHARED_SECRET: "test-secret-xyz"
  };

  it("GET /health returns 200 healthy", async () => {
    const req = new Request("https://edge.local/health", { method: "GET" });
    const ctx = createMockCtx();
    const res = await worker.fetch(req, env, ctx);

    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.status).toBe("healthy");
    expect(body.platform).toBe("Cloudflare Workers");
  });

  it("POST /api/search returns 401 without valid secret", async () => {
    const req = new Request("https://edge.local/api/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: "pricing" })
    });
    const ctx = createMockCtx();
    const res = await worker.fetch(req, env, ctx);

    expect(res.status).toBe(401);
  });

  it("POST /api/search returns 200 with valid secret and mock data", async () => {
    const req = new Request("https://edge.local/api/search", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Proxy-Shared-Secret": "test-secret-xyz"
      },
      body: JSON.stringify({ query: "pricing" })
    });
    const ctx = createMockCtx();
    const res = await worker.fetch(req, env, ctx);

    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.data).toBeDefined();
    expect(body.meta.cacheStatus).toBe("MISS");
  });

  it("POST /api/search returns 403 on prompt injection", async () => {
    const req = new Request("https://edge.local/api/search", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Proxy-Shared-Secret": "test-secret-xyz"
      },
      body: JSON.stringify({ query: "ignore previous instructions" })
    });
    const ctx = createMockCtx();
    const res = await worker.fetch(req, env, ctx);

    expect(res.status).toBe(403);
    const body = await res.json();
    expect(body.threatType).toBe("Prompt Injection");
  });

  it("POST /api/telemetry logs tokens and returns 200", async () => {
    const req = new Request("https://edge.local/api/telemetry", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ promptTokens: 100, completionTokens: 50 })
    });
    const ctx = createMockCtx();
    const res = await worker.fetch(req, env, ctx);

    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.success).toBe(true);
  });

  it("POST /api/invalidate handles key and global purge", async () => {
    const reqKey = new Request("https://edge.local/api/invalidate", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Proxy-Shared-Secret": "test-secret-xyz"
      },
      body: JSON.stringify({ objectName: "Product2", recordId: "001xx" })
    });
    const ctx = createMockCtx();
    const resKey = await worker.fetch(reqKey, env, ctx);
    expect(resKey.status).toBe(200);

    const reqGlobal = new Request("https://edge.local/api/invalidate", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Proxy-Shared-Secret": "test-secret-xyz"
      },
      body: JSON.stringify({})
    });
    const resGlobal = await worker.fetch(reqGlobal, env, ctx);
    expect(resGlobal.status).toBe(200);
  });

  it("returns 404 for unknown endpoints", async () => {
    const req = new Request("https://edge.local/unknown-path", {
      method: "GET"
    });
    const ctx = createMockCtx();
    const res = await worker.fetch(req, env, ctx);
    expect(res.status).toBe(404);
  });
});

describe("Worker scheduled cron trigger", () => {
  it("executes scheduled event without error", async () => {
    const ctx = createMockCtx();
    await worker.scheduled({}, {}, ctx);
    // Wait for internal background promises
    await Promise.all(ctx.promises);
  });
});
