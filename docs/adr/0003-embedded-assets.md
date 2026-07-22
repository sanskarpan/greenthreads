# ADR 0003: Embedded Web Assets

## Context

Serving `web/static` from a relative filesystem path depended on the process
working directory and made container packaging fragile.

## Decision

Embed the static assets into the Go server and serve them through a private
`http.ServeMux`.

## Consequences

The image is self-contained and can be started from any directory. Asset
changes require rebuilding the binary, and cache/version headers remain a
deployment concern.

## Alternatives considered

Runtime filesystem paths were rejected because they were not reproducible.
External object storage was rejected because the visualization has no required
external service dependency.
