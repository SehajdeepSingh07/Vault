(function () {
  const raw = (window.VAULT_CONFIG && window.VAULT_CONFIG.apiBase) || "http://127.0.0.1:8080";
  const base = raw.replace(/\/+$/, "");

  function url(path) {
    if (/^https?:\/\//i.test(path)) return path;
    return base + (path.startsWith("/") ? path : "/" + path);
  }

  async function request(path, options) {
    const response = await fetch(url(path), options || {});
    const text = await response.text();
    let body = null;
    try { body = text ? JSON.parse(text) : null; } catch (_) { body = text; }
    if (!response.ok) {
      const message = body && body.error ? body.error : (typeof body === "string" ? body : `HTTP ${response.status}`);
      const error = new Error(message);
      error.status = response.status;
      error.body = body;
      error.requestId = response.headers.get("X-Vault-Request-ID") || "";
      throw error;
    }
    return { response, body };
  }

  async function json(path, options) {
    const result = await request(path, options);
    return result.body;
  }

  window.VaultAPI = {
    base,
    url,
    request,
    json
  };
})();
