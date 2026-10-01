# satellite-rms-8-kafka

Eighth project in the satellite-rms series.
Introduces **Apache Kafka** as a durable message broker between the log collector and Loki.

## What changed from rms-7

| rms-7 | rms-8 |
|---|---|
| Promtail DaemonSet → Loki (direct HTTP push) | **Removed** |
| — | **Kafka (KRaft, single node)** — topic `loki-logs` |
| — | **Vector Collector DaemonSet** — reads pod logs → publishes to Kafka |
| — | **Vector Forwarder Deployment** — consumes Kafka → pushes to Loki |

Everything else (application services, Prometheus, Loki, Grafana, all app k8s manifests) is unchanged.

## Pipeline

```
/var/log/pods/
      ↓
Vector Collector (DaemonSet)
      ↓  [JSON events, topic: loki-logs]
Kafka (KRaft, bitnami/kafka:3.7.0)
      ↓  [consumer group: vector-loki-forwarder]
Vector Forwarder (Deployment)
      ↓  [HTTP POST /loki/api/v1/push]
Loki
      ↓
Grafana
```

## Prerequisites

- Minikube running (`minikube start --ports=80:30080`)
- Ingress addon enabled (`minikube addons enable ingress`)
- Hosts file entries:
  ```
  127.0.0.1  satellite.rms.local
  127.0.0.1  grafana.rms.local
  ```

## Deploy

### 1. Patch ingress NodePort

```powershell
kubectl patch svc ingress-nginx-controller -n ingress-nginx `
  --type='json' `
  -p='[{"op":"replace","path":"/spec/ports/0/nodePort","value":30080}]'
```

### 2. Namespaces and RBAC

```powershell
kubectl create namespace satellite-rms-8-kafka
kubectl apply -f k8s/monitoring/namespace.yaml
kubectl apply -f k8s/monitoring/prometheus-clusterrole.yaml
kubectl apply -f k8s/monitoring/vector-collector-serviceaccount.yaml
```

### 3. Monitoring stack

```powershell
kubectl apply -f k8s/monitoring/
```

Wait for all monitoring pods to be Running:
```powershell
kubectl get pods -n monitoring -w
```

Expected pods: `kafka`, `vector-collector-*` (one per node), `vector-forwarder-*`, `loki-*`, `prometheus-*`, `grafana-*`.

### 4. Build application images

```powershell
minikube docker-env | Invoke-Expression

docker build -t auth-service:latest     ./auth-service/
docker build -t dashboard-api:latest    ./dashboard-api/
docker build -t css-go-stateless:latest ./css/
docker build -t scheduler-go:latest     ./scheduler/
docker build -t telemetry-go:latest     ./telemetry/
```

### 5. Deploy application

```powershell
kubectl apply -n satellite-rms-8-kafka -f k8s/
kubectl get pods -n satellite-rms-8-kafka -w
```

## Access

| URL | Credentials |
|---|---|
| http://satellite.rms.local | alice/alice123 or bob/bob123 |
| http://grafana.rms.local | admin/admin |

## Verify the Kafka pipeline

```powershell
# List Kafka topics (should show loki-logs)
kubectl exec -n monitoring deploy/kafka -- `
  kafka-topics.sh --bootstrap-server localhost:9092 --list

# Peek at 3 log events in the topic
kubectl exec -n monitoring deploy/kafka -- `
  kafka-console-consumer.sh `
    --bootstrap-server localhost:9092 `
    --topic loki-logs `
    --max-messages 3 `
    --from-beginning

# Check consumer group lag (LAG=0 means forwarder is keeping up)
kubectl exec -n monitoring deploy/kafka -- `
  kafka-consumer-groups.sh `
    --bootstrap-server localhost:9092 `
    --describe --group vector-loki-forwarder
```

In Grafana → Explore → Loki:
```logql
{namespace="satellite-rms-8-kafka", pipeline="kafka"}
```

## Teardown

```powershell
# App only (keeps monitoring)
kubectl delete namespace satellite-rms-8-kafka

# Everything
kubectl delete namespace satellite-rms-8-kafka
kubectl delete namespace monitoring
kubectl delete clusterrole prometheus vector-collector
kubectl delete clusterrolebinding prometheus vector-collector

# Full reset
minikube delete
```
