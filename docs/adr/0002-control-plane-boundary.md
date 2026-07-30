# ADR 0002: Bounded Authenticated Control Plane

**Status:** Accepted  
**Date:** 2026-07

---

## Problem

The visualization server can do more than render a dashboard — it can also
initialise runtimes and spawn fiber work via the WebSocket control plane. This
makes it a **remote code execution surface** if left unprotected.

An early audit identified three classes of risk:

1. **Origin confusion.** Without `Origin` header validation, any page the user
   has open can send WebSocket commands to `localhost:8080` (cross-origin
   WebSocket does not enforce CORS; only the `Origin` header can be checked
   server-side).

2. **Unauthenticated public exposure.** Binding to `0.0.0.0` without an auth
   token would let anyone on the network spawn arbitrary fiber workloads,
   exhaust memory via `maxFibers`, or read internal metrics.

3. **Denial of service via message flood.** Without a rate limit or max message
   size, a single WebSocket client could saturate the server with large frames
   and trigger unbounded allocation.

---

## Decision

The following controls are enforced at the `web.Server` boundary:

- **Private `http.ServeMux`.** The server does not register on the default
  `http.DefaultServeMux`, preventing accidental handler exposure.

- **Same-origin enforcement by default.** On every WebSocket upgrade, the
  `Origin` header is validated against the server's own host. An explicit
  allowlist (`WithAllowedOrigins`) overrides the default for cross-origin
  deployments.

- **Bearer-token authentication on non-loopback binds.** If the listen address
  is not `127.0.0.1` or `::1`, `GREENTHREADS_AUTH_TOKEN` must be set. Requests
  without a valid `Authorization: Bearer <token>` header (or `?token=` query
  parameter as fallback) receive HTTP 401.

- **Bounded clients, message size, and rate.** Maximum 64 simultaneous
  WebSocket connections; max frame size 32 KiB; 30 messages / client / second.
  Clients exceeding the rate limit are disconnected.

- **Typed range validation at the WebSocket boundary.** All incoming message
  fields are validated (scheduler type enum, port range, duration bounds) before
  they reach the runtime. Invalid messages return a generic error that does not
  expose internal state.

- **Generic client errors.** The server never reflects internal error messages
  back to the client. `500 Internal Server Error` is returned for unexpected
  conditions without detail.

---

## Rationale

- **Loopback-only default is zero-friction for development.** Running
  `go run ./cmd/server` binds to `127.0.0.1:8080` by default. No token, no
  TLS — the developer gets immediate access. The auth requirement is triggered
  only when explicitly binding to a non-loopback address.

- **Bearer token over session cookies.** The WebSocket client is a
  single-page application, not a traditional session-based site. Bearer tokens
  are easier to manage in browser JS and do not have the CSRF surface of
  cookies.

- **Rate limiting at the server layer, not the runtime.** Limiting messages
  per client at the HTTP layer means a misbehaving client cannot cause
  unbounded `Spawn` calls at the runtime level.

---

## Consequences

**Positive:**

- Local development remains copy-pasteable — `go run ./cmd/server`, open
  browser, done.
- Public deployments require only a token and a TLS terminator (nginx/ingress).
- The Prometheus `/metrics` endpoint is auth-gated on non-loopback, so it can
  be safely exposed alongside the visualization without an extra proxy layer.
- Health endpoints (`/healthz`, `/readyz`) are intentionally unauthenticated
  so Kubernetes probes work without token management.

**Negative:**

- Operators must manage a long-lived token secret. Rotation requires restarting
  the server (the token is read at startup).
- The embedded UI at `/` is not auth-gated — if the operator wants to restrict
  access to the dashboard itself, they must do so at the reverse-proxy layer.
- The `?token=` query-parameter fallback leaves the token visible in URL logs
  on the server side. The `Authorization: Bearer` header is the recommended
  path.

---

## Alternatives considered

| Alternative | Reason rejected |
|---|---|
| Allow all origins | Creates an ambient authority cross-origin attack surface; rejected at first audit |
| Full identity provider (OAuth, OIDC) | No deployment integration exists in this repo; deferred until operator identity requirements are clearer |
| Mutual TLS | Adds certificate management overhead; out of scope for a single-binary dev tool |
| Rate limiting per IP | Ineffective behind NAT/proxy; per-connection limit is simpler and sufficient |
