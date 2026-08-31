import { describe, it, expect } from "vitest";
import { authorizeRequest } from "../src/auth.js";

function fakeReq(secret) {
  return {
    headers: {
      get: (name) => (name === "X-Proxy-Shared-Secret" ? secret : null)
    }
  };
}

describe("authorizeRequest", () => {
  const env = { PROXY_SHARED_SECRET: "test-secret-123" };

  it("accepts valid secret", () => {
    expect(authorizeRequest(fakeReq("test-secret-123"), env)).toBe(true);
  });

  it("rejects wrong secret", () => {
    expect(authorizeRequest(fakeReq("wrong"), env)).toBe(false);
  });

  it("rejects missing header", () => {
    expect(authorizeRequest(fakeReq(null), env)).toBe(false);
  });

  it("fails secure when env secret is not set", () => {
    expect(authorizeRequest(fakeReq("anything"), {})).toBe(false);
    expect(
      authorizeRequest(fakeReq("anything"), { PROXY_SHARED_SECRET: "" })
    ).toBe(false);
  });
});
