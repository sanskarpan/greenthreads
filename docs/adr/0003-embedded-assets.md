# ADR 0003: Embedded Web Assets

**Status:** Accepted  
**Date:** 2026-07

---

## Problem

The browser visualization UI consists of a single `index.html` with inlined
CSS and JavaScript served from `web/static/`. An early implementation served
this directory from a relative filesystem path at runtime:

```go
http.Handle("/", http.FileServer(http.Dir("web/static")))
```

This created two concrete problems:

1. **Working-directory dependency.** The server had to be launched from the
   repository root. Running the binary from any other directory (e.g.
   `/usr/local/bin/greenthreads-server`) produced a 404 on `/` because
   `web/static` did not exist relative to the process working directory.

2. **Container fragility.** The multi-stage Docker build copies only the
   compiled binary into the final `scratch` image. A filesystem-relative
   `http.Dir` path would always fail in the container because the `web/`
   directory is not present in the final image layer.

---

## Decision

Use Go's `//go:embed` directive (introduced in Go 1.16) to bake the contents
of `web/static/` into the compiled binary. The server registers the embedded
filesystem on a private `http.ServeMux`:

```go
//go:embed static
var staticFS embed.FS

func newMux() *http.ServeMux {
    mux := http.NewServeMux()
    mux.Handle("/", http.FileServerFS(staticFS))
    return mux
}
```

The resulting binary is fully self-contained: one file, launchable from any
directory, with no external dependencies at runtime.

---

## Rationale

- **Zero deployment surface for static assets.** The operator does not need to
  manage a separate web root, CDN, or asset pipeline. The binary *is* the
  deployment unit.

- **Reproducible builds.** Because the assets are compiled in, the binary
  checksum covers the UI as well as the Go code. A tampered UI would produce a
  different binary hash.

- **Container image simplicity.** The final Docker image is a `scratch` image
  with a single binary and a `passwd` file for the non-root user. No nginx
  sidecar, no volume mount, no shared filesystem between build stages.

- **Go 1.16 is widely available.** The project already requires Go 1.21+, so
  `//go:embed` is unconditionally available.

---

## Consequences

**Positive:**

- The server binary starts correctly from any working directory, including
  `/`, `/tmp`, or a Kubernetes `emptyDir`.
- Container images built with `FROM scratch` work without modification.
- `go build` is the only step needed to produce a deployable artifact.

**Negative:**

- **Asset changes require rebuilding the binary.** Hot-reloading the UI during
  development requires either running `go run ./cmd/server` (which recompiles
  on invocation) or using a separate dev server that serves `web/static/`
  directly from the filesystem.

- **Binary size increases.** The `index.html` (currently ~25 KiB) is included
  in every binary build. This is negligible for the use case but worth noting
  for audits.

- **Cache and versioning headers are a deployment concern.** The embedded
  `FileServerFS` does not automatically add content-hash suffixes to asset
  URLs. If the UI gains separate CSS/JS files in the future, a cache-busting
  strategy (e.g. query-string version, or subresource integrity hashes) will
  need to be added.

---

## Alternatives considered

| Alternative | Reason rejected |
|---|---|
| Runtime filesystem `http.Dir` path | Working-directory dependent; fails in containers |
| External object storage (S3, GCS) | Introduces a required external service dependency; violates the single-binary constraint |
| nginx sidecar in Docker Compose | Added operational complexity for what is fundamentally a single-process tool |
| Separate frontend CDN deployment | Out of scope; the UI is a dev/observability tool, not a consumer-facing product |
