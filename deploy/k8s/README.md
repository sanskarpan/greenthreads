# Kubernetes Deployment

## Prerequisites
- Kubernetes 1.24+
- kubectl configured for your cluster

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

## Resource Sizing

Memory formula: 256Mi base + (64Ki × GREENTHREADS_MAX_FIBERS).
Default (1000 fibers): ~320Mi. Adjust limits and GREENTHREADS_MAX_FIBERS together.

## Prometheus Scraping

The deployment has `prometheus.io/scrape: "true"` annotations. Ensure your
Prometheus has pod-level scraping configured. The /metrics endpoint requires
the auth token in the Authorization header.
