import { Redis } from "@upstash/redis";

// Stub that satisfies the same interface as the real Upstash client.
// Used for local dev when no Redis creds are set.
function createMockClient() {
  return {
    get: async () => null,
    set: async () => "OK",
    setex: async () => "OK",
    del: async () => 1,
    incr: async () => 1,
    hincrby: async (_k, _f, v) => v,
    hgetall: async () => ({}),
    zincrby: async (_k, inc) => inc,
    zrevrange: async () => [],
    sadd: async () => 1,
    smembers: async () => [],
    sismember: async () => 0,
    pipeline: () => {
      const p = {
        zincrby: () => p,
        hincrby: () => p,
        exec: async () => []
      };
      return p;
    }
  };
}

// Returns an Upstash client, or a stub if creds are missing.
export function getRedisClient(env) {
  const url = (env.UPSTASH_REDIS_REST_URL || "").trim();
  const token = (env.UPSTASH_REDIS_REST_TOKEN || "").trim();

  if (!url || !token) {
    console.warn("Redis creds missing, using mock client");
    return createMockClient();
  }

  try {
    return new Redis({ url, token });
  } catch (err) {
    console.error("Redis init failed:", err.message);
    return createMockClient();
  }
}

// Swallow Redis errors so they don't crash the worker.
export async function safeRedisCall(promise, label) {
  try {
    return await promise;
  } catch (err) {
    console.error(`Redis [${label}]:`, err.message);
    return null;
  }
}
