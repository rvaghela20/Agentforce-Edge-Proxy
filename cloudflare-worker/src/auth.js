export function authorizeRequest(request, env) {
  const secret = request.headers.get("X-Proxy-Shared-Secret");
  if (!env.PROXY_SHARED_SECRET) return false; // fail-secure
  return secret === env.PROXY_SHARED_SECRET;
}
