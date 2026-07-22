# Production Runbook

## Deploy

1. Build and scan the image in CI.
2. Generate a high-entropy `GREENTHREADS_AUTH_TOKEN` in the deployment secret
   manager; never place it in the image or command history.
3. Run the image with `-p 8080:8080`, the token environment variable, and a
   platform restart policy. Put TLS and any external identity layer in the
   ingress because this server speaks plain HTTP/WebSocket.
4. Verify `/healthz` returns 200 and `/readyz` returns 200 after the runtime is
   initialized. Verify `/metrics` is reachable only through the intended
   network policy.

## Roll Back

Stop traffic, deploy the previous immutable image tag, and verify health and
readiness before reopening traffic. A runtime reset is not a deployment
rollback: it discards fibers and metrics in the current process.

## Diagnosis

### 1. Readiness is 503

Check that the process is listening and `/healthz` is 200. Then inspect the
WebSocket client or deployment logs: readiness remains 503 until an `init`
message successfully starts a runtime.

### 2. WebSocket clients receive forbidden responses

Confirm the request Origin matches the server host and that the Bearer token
matches `GREENTHREADS_AUTH_TOKEN`. Do not disable origin checks to work around
an ingress configuration.

### 3. Fiber counts or completion metrics stop changing

Check `greenthreads_runtime_running`, active fibers, and context switches in
`/metrics`. A fiber function may be blocked or may never return; inspect the
fiber event stream and application-owned cancellation paths.

### 4. Latency or memory rises under client load

Check client count, message rate, active fibers, and failed WebSocket writes in
structured logs. Disconnect slow clients and reduce spawn rates. The server is
bounded but cannot make an unbounded user function finite.

### 5. The process exits during a request

Inspect the structured error and request ID. Malformed protocol messages should
produce a generic error, so a process exit indicates an unrecovered defect or
host-level failure. Preserve logs, the image digest, and the failing message
shape for a regression test.

## Escalation

The on-call engineer owns first response, traffic isolation, and rollback. The
runtime maintainer owns scheduler, fiber, and synchronization defects. The
security owner owns token, origin, dependency, and image findings. Escalate a
critical incident to the service owner and security owner immediately; attach
the image digest, request ID, timestamps, metrics snapshot, and reproduction.
