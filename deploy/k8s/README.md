# Kubernetes Deployment

## Prerequisites
- Kubernetes 1.24+
- kubectl configured for your cluster
- A CNI that enforces `NetworkPolicy` (e.g. Calico, Cilium) for the network
  policy to take effect

## Manifests

| File | Purpose |
|---|---|
| `deployment.yaml` | 2 replicas, distroless non-root, Restricted PSS security context (seccomp `RuntimeDefault`, read-only rootfs, no privilege escalation, dropped caps, no service-account token) |
| `service.yaml` | ClusterIP service on port 8080 |
| `secret.yaml` | Template for the `greenthreads-secrets` auth token |
| `networkpolicy.yaml` | Default-deny; allows ingress only from pods labelled `greenthreads-client: "true"` / namespaces labelled `greenthreads-ingress: "true"`, egress only for DNS |
| `pdb.yaml` | PodDisruptionBudget `minAvailable: 1` for node drains / upgrades |
| `hpa.yaml` | Optional HorizontalPodAutoscaler |

The image is `ghcr.io/sanskarpan/greenthreads:latest`, published and cosign-signed
by the release workflow. Pin to an immutable digest in production
(`ghcr.io/sanskarpan/greenthreads@sha256:...`).

## Deploy

1. Create the auth token secret:
   ```bash
   kubectl create secret generic greenthreads-secrets \
     --from-literal=auth-token=$(openssl rand -hex 32)
   ```

2. Apply manifests:
   ```bash
   kubectl apply -f deploy/k8s/
   ```

3. Label the clients allowed to reach the service (otherwise the NetworkPolicy
   denies all ingress):
   ```bash
   kubectl label pod <ingress-or-scraper-pod> greenthreads-client=true
   ```

## Verify image signature

```bash
cosign verify ghcr.io/sanskarpan/greenthreads:latest \
  --certificate-identity-regexp 'https://github.com/sanskarpan/greenthreads/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

## Resource Sizing

Memory formula: 256Mi base + (64Ki × GREENTHREADS_MAX_FIBERS).
Default (1000 fibers): ~320Mi. Adjust limits and GREENTHREADS_MAX_FIBERS together.

## Prometheus Scraping

The deployment has `prometheus.io/scrape: "true"` annotations. Ensure your
Prometheus has pod-level scraping configured. The /metrics endpoint requires
the auth token in the Authorization header.
