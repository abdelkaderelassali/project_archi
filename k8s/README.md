# Kubernetes deployment

Six functional layers, six namespaces, six manifests applied in order.

| File | Namespace | Components |
|---|---|---|
| `00-namespaces.yaml`   | — | All six namespaces |
| `01-storage.yaml`      | `newslake-storage`       | MinIO (StatefulSet + bucket-init Job), Postgres warehouse |
| `02-streaming.yaml`    | `newslake-streaming`     | Zookeeper, Kafka |
| `03-processing.yaml`   | `newslake-processing`    | Scraper, Bronze/Silver consumer (ConfigMap + Secret) |
| `04-orchestration.yaml`| `newslake-orchestration` | Airflow (standalone) |
| `05-bi.yaml`           | `newslake-bi`            | Metabase (NodePort 30030) |
| `06-monitoring.yaml`   | `newslake-monitoring`    | Prometheus (NodePort 30090) + Grafana (NodePort 30300) |

## Quick deploy (kind / minikube / any cluster)

```bash
# 1. Build & push the two project images first
docker build -t REGISTRY/newslake-scraper:latest     ./scraper
docker build -t REGISTRY/newslake-transformer:latest ./transformer
docker push REGISTRY/newslake-scraper:latest
docker push REGISTRY/newslake-transformer:latest

# (For local kind clusters: `kind load docker-image REGISTRY/newslake-scraper:latest` instead)

# 2. Apply everything
kubectl apply -f k8s/00-namespaces.yaml
kubectl apply -f k8s/01-storage.yaml
kubectl apply -f k8s/02-streaming.yaml
kubectl apply -f k8s/03-processing.yaml
kubectl apply -f k8s/04-orchestration.yaml
kubectl apply -f k8s/05-bi.yaml
kubectl apply -f k8s/06-monitoring.yaml

# 3. Watch
kubectl get pods -A | grep newslake
```

## Access

| UI | URL |
|---|---|
| MinIO console | `kubectl port-forward -n newslake-storage svc/minio 9001:9001` → http://localhost:9001 |
| Airflow | `kubectl port-forward -n newslake-orchestration svc/airflow 8080:8080` |
| Metabase | http://NODE_IP:30030 |
| Prometheus | http://NODE_IP:30090 |
| Grafana | http://NODE_IP:30300 (admin / admin) |

## Production hardening (next steps)

- Replace plain Secrets with **External Secrets Operator** + Vault.
- Replace these manifests with the official **Apache Airflow Helm chart** (CeleryExecutor, HPA, autoscaling workers).
- Replace bespoke Kafka with **Strimzi**, Postgres with **CloudNativePG**, MinIO with the **MinIO Operator**.
- Replace this monitoring with **kube-prometheus-stack** Helm chart for full ServiceMonitors / Alertmanager.
- Add **NetworkPolicies** so each namespace only talks to the next layer.
- Add **Ingress** + cert-manager for proper TLS instead of NodePorts.
