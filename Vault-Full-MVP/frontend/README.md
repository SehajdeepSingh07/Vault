# Vault Frontend

This is a standalone API client. It has no dependency on the Vault backend's filesystem and is intentionally kept separate so another UI can replace it later without changing the storage engine.

## Configure

Edit `config.js`:

```js
window.VAULT_CONFIG = {
  apiBase: "http://127.0.0.1:8080"
};
```

## Run locally

### Windows PowerShell

```powershell
python -m http.server 3000
```

Open `http://127.0.0.1:3000`.

### Linux/macOS

```bash
python3 -m http.server 3000
```

The backend should allow the frontend origin through `VAULT_CORS_ORIGINS`.

## API contract

The frontend only depends on `/v1/*` public APIs. A future frontend can use `docs/openapi.yaml` and ignore this UI completely.
