# Security Policy

## Supported Versions

| Version | Supported |
| ------- | --------- |
| v0.x    | Yes       |

greenthreads is currently pre-1.0. The latest release on the `main` branch receives security fixes. Older patch releases within the same minor version are not backported unless the impact is critical.

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Email **sanskar@noclick.com** with the subject line `[greenthreads] Security Report`. Include:

- A description of the vulnerability and its potential impact
- Steps to reproduce or a minimal proof-of-concept
- The greenthreads version or commit SHA affected
- Your Go version and operating system

### Response Timeline

| Milestone                         | Target     |
| --------------------------------- | ---------- |
| Acknowledgement of your report    | 48 hours   |
| Triage and severity assessment    | 5 days     |
| Patch for critical severity       | 14 days    |
| Patch for high/medium severity    | 30 days    |
| Public disclosure (coordinated)   | After patch ships |

We will work with you to agree on a disclosure date. If you do not receive an acknowledgement within 48 hours, follow up at the same address.

## Scope

### In Scope

- The greenthreads library code (`internal/`, `web/`, `cmd/`)
- Auth token handling in the WebSocket control plane (`web/server.go`)
- The WebSocket server itself — message parsing, origin validation, rate limiting, TLS configuration
- Metrics endpoint authorization
- Deadlock detector behavior that could be exploited to suppress alerts

### Out of Scope

- Vulnerabilities in the Go standard library or third-party dependencies (please report those upstream)
- Panics in user-supplied fiber functions (the library captures and isolates them by design)
- Denial-of-service via unbounded fiber spawning when no `maxFibers` cap is configured (this is a known trade-off, not a vulnerability)
- Issues that require an attacker to already have local code execution on the host

## No Bounty Program

There is no bug bounty program at this time. Researchers who disclose vulnerabilities responsibly will be credited in the release notes and `CHANGELOG.md` (unless they prefer to remain anonymous).
