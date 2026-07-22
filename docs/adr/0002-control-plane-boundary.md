# ADR 0002: Bounded Authenticated Control Plane

## Context

The visualization server can initialize runtimes and spawn work. An
unrestricted WebSocket origin and unchecked payload assertions made it a remote
control and denial-of-service surface.

## Decision

Use a private HTTP mux, same-origin checking by default, an optional bearer
token required for non-loopback operation, bounded messages/clients/rates, and
typed range validation at the WebSocket boundary. Return generic client errors.

## Consequences

Local development remains copy-pasteable. Public deployments need a secret and
an ingress for TLS. Clients must handle protocol errors and rate limits, and
operators can use health and Prometheus endpoints.

## Alternatives considered

Allowing all origins was retained only as an audit finding, not as a supported
production mode. A full identity provider was deferred because this repository
has no service account or deployment integration.
