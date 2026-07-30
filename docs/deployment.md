# Deployment

---

## Docker

The image uses a multi-stage build: a Go 1.26 alpine builder produces a
statically linked binary copied into a scratch-based final image. The process
runs as a non-root user (UID 65532).

```bash
# Build
docker build -t greenthreads:local .

# Run (loopback only, no auth required)
docker run --rm -p 8080:8080 greenthreads:local

# Run (internet-facing, auth + TLS)
docker run --rm \
  -p 8443:8443 \
  -e GREENTHREADS_LISTEN="0.0.0.0:8443" \
  -e GREENTHREADS_AUTH_TOKEN="$(openssl rand -hex 32)" \
  -e GREENTHREADS_TLS_CERT="/etc/tls/cert.pem" \
  -e GREENTHREADS_TLS_KEY="/etc/tls/key.pem" \
  -v /etc/tls:/etc/tls:ro \
  greenthreads:local
```

---

## Health checks

```bash
# Liveness (no auth required)
curl http://localhost:8080/healthz    # 200 = alive

# Readiness (no auth required)
curl http://localhost:8080/readyz     # 200 = runtime active; 503 = not ready

# Metrics (auth required on non-loopback)
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/metrics
```

Use `/healthz` for liveness probes and `/readyz` for readiness probes in
Kubernetes or other orchestrators.

---

## Environment variables

Configure the server exclusively via environment variables in containerized
environments. Avoid embedding secrets in command-line arguments, which may
appear in process listings.

| Variable | Description |
|---|---|
| `GREENTHREADS_AUTH_TOKEN` | Bearer token for `/metrics` and `/ws` on non-loopback binds. Required for internet-facing deployments. |
| `GREENTHREADS_LISTEN` | Bind address. Use `0.0.0.0:8080` behind a reverse proxy; keep `127.0.0.1:8080` for localhost-only. |
| `GREENTHREADS_TLS_CERT` | Path to TLS certificate PEM. Both cert and key required to enable TLS. |
| `GREENTHREADS_TLS_KEY` | Path to TLS private key PEM. |
| `LOG_LEVEL` | `DEBUG` \| `INFO` \| `WARN` \| `ERROR`. Default: `INFO`. |

---

## Kubernetes example

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: greenthreads
spec:
  replicas: 1
  selector:
    matchLabels:
      app: greenthreads
  template:
    metadata:
      labels:
        app: greenthreads
    spec:
      containers:
        - name: greenthreads
          image: ghcr.io/sanskarpan/greenthreads:latest
          ports:
            - containerPort: 8080
          env:
            - name: GREENTHREADS_LISTEN
              value: "0.0.0.0:8080"
            - name: GREENTHREADS_AUTH_TOKEN
              valueFrom:
                secretKeyRef:
                  name: greenthreads-secret
                  key: auth-token
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8080
            initialDelaySeconds: 3
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /readyz
              port: 8080
            initialDelaySeconds: 3
            periodSeconds: 5
          securityContext:
            runAsNonRoot: true
            runAsUser: 65532
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
```

---

## Reverse proxy (nginx)

```nginx
server {
    listen 443 ssl;
    server_name greenthreads.example.com;

    ssl_certificate     /etc/tls/cert.pem;
    ssl_certificate_key /etc/tls/key.pem;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        # Required for WebSocket upgrade
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 3600s;
    }
}
```

---

## Security hardening

- Set `GREENTHREADS_AUTH_TOKEN` on any non-loopback bind. Without it, the
  server refuses to start on a public address.
- Use TLS in production. Pass `GREENTHREADS_TLS_CERT` and
  `GREENTHREADS_TLS_KEY`, or terminate TLS at your reverse proxy.
- The pprof endpoint (`-pprof-addr`) is always on a **separate** address and
  is never exposed on the main listener. Bind it to loopback only.
- The embedded UI at `/` is not auth-gated by default. Restrict it via your
  reverse proxy if needed.
