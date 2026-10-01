# satellite-rms-8-kafka — CLAUDE.md

This file gives Claude Code the full context needed to work in this project.

---

## What This Project Is

`satellite-rms-8-kafka` is the eighth project in the satellite-rms series.
It is built on top of `satellite-rms-7-monitor` and introduces **Apache Kafka**
as a durable message broker between the log collector and Loki.

Zero changes to business logic from rms-7. The entire change is in the
observability pipeline — Promtail is replaced by two Vector instances with
Kafka sitting between them.

---

## Why Kafka?

In rms-7, Promtail pushes logs directly to Loki. If Loki restarts or falls
behind, logs are lost. Kafka adds:

- **Durability** — logs survive a Loki outage; they stay in the topic until
  consumed (1-hour retention in this dev setup).
- **Decoupling** — the collector (producer) and Loki (consumer) are independent;
  either side can restart without blocking the other.
- **Fan-out potential** — additional consumers (e.g. an alerting service, an
  archive writer) can read the same topic without touching the collector.

---

## Pipeline Architecture

```
/var/log/pods/ (node filesystem)
       ↓
Vector Collector DaemonSet    [replaces Promtail]
  source: kubernetes_logs     [reads + enriches with k8s metadata]
  sink:   kafka               [produces JSON events → topic "loki-logs"]
       ↓
Kafka Broker (KRaft, single node)
  topic: loki-logs            [1 partition, 1h retention]
       ↓
Vector Forwarder Deployment
  source: kafka               [consumer group "vector-loki-forwarder"]
  sink:   loki                [HTTP POST to /loki/api/v1/push]
       ↓
Loki
       ↓
Grafana (LogQL queries — identical to rms-7)
```

---

## Project Structure

```
satellite-rms-8-kafka/
├── dashboard-api/          HTTP→gRPC bridge, JWT auth, zone filtering, rate limiting
├── auth-service/           Login endpoint, issues JWT tokens
├── css/                    Constellation State Service — gRPC server, orbit simulation
├── scheduler/              Schedule optimizer — pure gRPC client, writes to CSS
├── telemetry/              Health monitor — pure gRPC client, reads from CSS
├── k8s/                    App Kubernetes manifests (unchanged from rms-7)
│   └── monitoring/         Observability stack (17 YAML files)
├── CLAUDE.md
└── README.md
```

---

## Services (unchanged from rms-7)

All five application services are identical to rms-7. See rms-7 CLAUDE.md for
full documentation on ports, gRPC methods, Prometheus metrics, and credentials.

---

## Kubernetes Namespaces

### satellite-rms-8-kafka
App pods: dashboard-api, auth-service, css (×2 replicas), scheduler, telemetry, redis.

### monitoring
Observability stack: Prometheus, Kafka, Vector Collector (DaemonSet),
Vector Forwarder (Deployment), Loki, Grafana.
Kept separate so it survives `kubectl delete namespace satellite-rms-8-kafka`.

---

## Monitoring Stack Files (k8s/monitoring/)

| File | What it does |
|---|---|
| namespace.yaml | Creates the `monitoring` namespace |
| prometheus-serviceaccount.yaml | ServiceAccount for Prometheus |
| prometheus-clusterrole.yaml | ClusterRole + ClusterRoleBinding (cross-namespace pod discovery) |
| prometheus-configmap.yaml | scrape_configs — pod annotation-based discovery |
| prometheus-deployment.yaml | prom/prometheus:v2.53.0 |
| prometheus-service.yaml | ClusterIP :9090 |
| loki-configmap.yaml | Loki config (WAL, boltdb-shipper, compactor) |
| loki-deployment.yaml | grafana/loki:2.9.8 |
| loki-service.yaml | ClusterIP :3100 |
| **kafka-deployment.yaml** | **apache/kafka:3.7.1 — KRaft mode, single node** |
| **kafka-service.yaml** | **ClusterIP :9092 (broker), :9093 (controller)** |
| **vector-collector-serviceaccount.yaml** | **SA + ClusterRole + ClusterRoleBinding for pod/node discovery** |
| **vector-collector-configmap.yaml** | **Vector config: kubernetes_logs → Kafka** |
| **vector-collector-daemonset.yaml** | **timberio/vector:0.39.0-alpine DaemonSet (replaces Promtail)** |
| **vector-forwarder-configmap.yaml** | **Vector config: Kafka → Loki** |
| **vector-forwarder-deployment.yaml** | **timberio/vector:0.39.0-alpine Deployment** |
| grafana-datasources-configmap.yaml | Pre-provisions Prometheus + Loki datasources |
| grafana-deployment.yaml | grafana/grafana:10.4.2 |
| grafana-service.yaml | ClusterIP :3000 |
| grafana-ingress.yaml | grafana.rms.local → grafana-service:3000 |

Bold = new in rms-8. The three `promtail-*.yaml` files from rms-7 are removed.

---

## Kafka Configuration Notes

- **Image** — `apache/kafka:3.7.1` (official Apache Software Foundation image).
  Bitnami images were removed from Docker Hub by Broadcom in 2024.
  The apache/kafka image uses `KAFKA_*` env vars — no `KAFKA_CFG_` prefix.
  Scripts live at `/opt/kafka/bin/` (NOT in $PATH — always use full path).
- **KRaft mode** — no Zookeeper. `KAFKA_PROCESS_ROLES=broker,controller`.
  Requires `CLUSTER_ID` (fixed base64 UUID: `MkU3OEVBNTcwNTJENDM2Qk`).
- **Auto-create topics** — `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` means the
  `loki-logs` topic is created on the first produce call.
- **Retention** — `KAFKA_LOG_RETENTION_HOURS=1`. Messages are relay data;
  Loki is the durable store.
- **Storage** — `emptyDir`, mount at `/var/lib/kafka/data`. Data is ephemeral;
  on pod restart the topic is re-created. Fine for a dev pipeline.
- **Partitions** — 1 (default). The single Vector Forwarder consumer is sufficient.
  Add partitions only when scaling forwarder replicas to match.

---

## Kafka Concepts: Topic, Partition, Offset

### Topic
A named channel. `"loki-logs"` is just a human label — Kafka does NOT restrict
access based on the name. Any process that knows the broker address and topic
name can read or write. Think of it as a named bulletin board, not a
point-to-point pipe. Multiple independent consumer groups can read the same
topic simultaneously without interfering with each other.

### Partition
An ordered, append-only log within a topic. This project uses 1 partition
(auto-created default). Each message appended gets the next sequential offset.

```
partition 0:
  offset 0  → {"message":"[auth] Login alice","kubernetes":{...}}
  offset 1  → {"message":"[css] v2212 total=50 degraded=11","kubernetes":{...}}
  offset 2  → {"message":"[dashboard-api] GET /api/satellites","kubernetes":{...}}
  ...
```

### Offset
A sequential integer ID per message within a partition. The consumer group
commits its current offset back to Kafka after processing. On restart the
forwarder resumes from the committed offset — no messages re-ingested, none
skipped.

`auto_offset_reset = "latest"` applies only on the very first connect (no
stored offset yet): start from the newest message, ignore historical backlog.
After first connect the committed offset governs position.

### Consumer group LAG
```
CURRENT-OFFSET  LOG-END-OFFSET  LAG
2845            2848            3
```
LAG = messages in Kafka not yet forwarded to Loki. LAG=0 means the forwarder
is keeping up in real time. A spike that recovers means Kafka absorbed a burst.
Growing LAG means the forwarder is falling behind.

---

## Multi-Consumer Fan-out

The topic name does not restrict who reads. To add a second consumer (e.g.
error alerting to a webhook) add a new Vector instance with a different
`group_id` — nothing else changes:

```toml
[sources.kafka_in]
  type              = "kafka"
  bootstrap_servers = "kafka.monitoring.svc.cluster.local:9092"
  topics            = ["loki-logs"]
  group_id          = "vector-slack-forwarder"   ← unique group_id is the only requirement
  auto_offset_reset = "latest"
```

Two consumers with the same `group_id` load-balance partitions between them
(each reads a subset). Two consumers with different `group_ids` each receive
every message independently.

This project uses no Kafka authentication (PLAINTEXT). Any pod in the cluster
that can reach port 9092 can read any topic. For production, add:
- Kafka ACLs (per-topic read/write permissions per client)
- SASL/SCRAM (username + password)
- Kubernetes NetworkPolicy restricting port 9092 to known pods

---

## Pipeline Latency

The two-Vector-hop adds ~1–2 seconds compared to rms-7's direct Promtail push:

| Hop | Typical delay |
|---|---|
| stdout → /var/log/pods/ | ~0 ms (containerd synchronous write) |
| Vector Collector batch window | 0–1 s (batch.timeout_secs = 1 s) |
| Kafka produce + disk flush | 2–10 ms |
| Forwarder consumer poll | 0–500 ms |
| Forwarder batch window | 0–1 s |
| HTTP POST → Loki | 1–5 ms |
| **Total rms-8** | **~2–4 s** |
| rms-7 (Promtail direct) | ~1–2 s |

This is acceptable for log observability (Grafana refreshes every 5 s+, alerting
evaluates every 1 min+). At high throughput Kafka reduces p99 latency spikes
because it absorbs bursts rather than dropping them.

---

## Grafana: Kafka is NOT a Datasource

Kafka does not appear in Grafana's datasource picker — this is correct.
Kafka is a transport layer, not a query layer. To see logs that traveled
through Kafka, select **Loki** and filter by the `pipeline="kafka"` label:

```logql
{namespace="satellite-rms-8-kafka", pipeline="kafka"}
```

This label is stamped by Vector Forwarder's VRL remap transform and is present
on every log line that passed through the Kafka broker.

---

## Data at Each Pipeline Stage

### Layer 1 — Metrics (PULL)
```
Go promauto counter  →  in-memory registry  →  GET /metrics (Prometheus text format)
  →  Prometheus TSDB  →  PromQL JSON response  →  Grafana panel
```

### Layer 2 — Logs (buffered PUSH)
```
fmt.Printf()  →  containerd wraps in JSON → /var/log/pods/.../0.log
  →  Vector Collector adds kubernetes.* metadata → Kafka JSON message
  →  Kafka stores at offset N (bytes verbatim)
  →  Vector Forwarder VRL remap flattens kubernetes.* to top-level fields
  →  HTTP POST streams JSON to Loki (/loki/api/v1/push)
  →  LogQL JSON response  →  Grafana Explore
```

VRL remap is required because Loki sink label templates (`{{ field }}`) only
reference top-level event fields. The nested `.kubernetes.pod_namespace` must
be promoted to `.k8s_namespace` before it can be used as a label.

---

## Vector Collector Notes

- **Image** — `timberio/vector:0.39.0-alpine`
- **Source** — `kubernetes_logs` built-in source. Vector reads `/var/log/pods/`
  on the node filesystem and enriches events with pod namespace, pod name,
  container name, and all pod labels automatically.
- **Sink** — `kafka` sink. Each log line becomes a JSON event on the `loki-logs` topic.
- **RBAC** — needs `get/list/watch` on pods, nodes, namespaces (ClusterRole
  `vector-collector`, same scope as the old Promtail ClusterRole).
- **HostPath mounts** — `/var/log/pods/` and `/var/lib/docker/containers/` are mounted
  read-only. `/var/lib/vector/` is mounted as a checkpoint store so Vector resumes
  from its last read position after a pod restart.

---

## Vector Forwarder Notes

- **Image** — `timberio/vector:0.39.0-alpine`
- **Source** — `kafka` source, consumer group `vector-loki-forwarder`,
  `auto_offset_reset = "latest"` (avoid re-ingesting on forwarder restart).
- **Transform** — `remap` VRL stage extracts `kubernetes.*` metadata fields into
  flat top-level fields so the Loki sink label templates can reference them.
- **Sink** — `loki` sink. Labels match what rms-7 Promtail produced:
  `namespace`, `pod`, `container`, `app`. An extra `pipeline=kafka` label is added
  so you can filter for Kafka-brokered logs specifically.
- **No RBAC needed** — the forwarder only talks to Kafka and Loki over the cluster
  network; it does not call the k8s API.

---

## Loki Configuration Notes (unchanged from rms-7)

Two fixes remain important:

1. **Compactor** — `working_directory: /loki/compactor` must be set.
2. **WAL path** — `ingester.wal.dir: /loki/wal` (emptyDir mount, non-root).

---

## Docker Images

| Image tag | Source directory |
|---|---|
| auth-service:latest | ./auth-service/ |
| dashboard-api:latest | ./dashboard-api/ |
| css-go-stateless:latest | ./css/ |
| scheduler-go:latest | ./scheduler/ |
| telemetry-go:latest | ./telemetry/ |

`apache/kafka:3.7.1` and `timberio/vector:0.39.0-alpine` are pulled from Docker Hub
(no custom build needed).

All custom images built inside Minikube's Docker daemon (`minikube docker-env | Invoke-Expression`).

Image loading strategy: pull on host Docker Desktop (authenticated, higher rate
limits), then `minikube image load <image>` to transfer into Minikube's daemon.
Use `imagePullPolicy: IfNotPresent` on all three external-image deployments so
Minikube does not re-attempt Docker Hub pulls on pod restarts.

---

## Deploy Sequence

```
1.  minikube start --ports=80:30080
2.  minikube addons enable ingress
3.  Patch ingress NodePort to 30080
4.  Add hosts file entries (satellite.rms.local, grafana.rms.local → 127.0.0.1)
5.  Pull and load external images:
      docker pull apache/kafka:3.7.1
      docker pull timberio/vector:0.39.0-alpine
      minikube image load apache/kafka:3.7.1
      minikube image load timberio/vector:0.39.0-alpine
6.  kubectl create namespace satellite-rms-8-kafka
7.  kubectl apply -f k8s/monitoring/namespace.yaml
8.  Deploy monitoring RBAC:
      kubectl apply -f k8s/monitoring/prometheus-clusterrole.yaml
      kubectl apply -f k8s/monitoring/vector-collector-serviceaccount.yaml
9.  Deploy monitoring stack:
      kubectl apply -f k8s/monitoring/
10. Wait for monitoring pods Running:
      kubectl get pods -n monitoring -w
11. Build and deploy application:
      minikube docker-env | Invoke-Expression
      (build all 5 images)
      kubectl apply -n satellite-rms-8-kafka -f k8s/
```

Full commands in README.md.

---

## Access URLs

| URL | Service |
|---|---|
| http://satellite.rms.local | Operator dashboard (alice/alice123, bob/bob123) |
| http://grafana.rms.local | Grafana (admin/admin) |

---

## Verifying the Kafka Pipeline

### 1 — Check Kafka has the topic
```bash
kubectl exec -n monitoring deploy/kafka -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
Expected output: `loki-logs`

### 2 — Inspect live messages in the topic
```bash
kubectl exec -n monitoring deploy/kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic loki-logs \
    --max-messages 3 \
    --from-beginning
```
Each message is a JSON object with `message`, `kubernetes.pod_namespace`, etc.

### 3 — List all consumer groups
```bash
kubectl exec -n monitoring deploy/kafka -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --list
```

### 4 — Check consumer group lag
```bash
kubectl exec -n monitoring deploy/kafka -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe --group vector-loki-forwarder
```
`LAG` of 0 means the forwarder is keeping up with the collector.

### 5 — Confirm logs reach Loki (Grafana)
Open Grafana → Explore → select **Loki** datasource (Kafka does NOT appear as
a datasource — it is a transport layer, not a query layer) → run:
```logql
{namespace="satellite-rms-8-kafka", pipeline="kafka"}
```

### 6 — Check forwarder logs for errors
```bash
kubectl logs -n monitoring deploy/vector-forwarder --tail=30
```

---

## Useful LogQL Queries (same as rms-7 — labels are preserved)

```logql
{namespace="satellite-rms-8-kafka", app="dashboard-api"}
{namespace="satellite-rms-8-kafka"} |= "ERROR"
{namespace="satellite-rms-8-kafka", app="auth"} |= "Login"
{namespace="satellite-rms-8-kafka", app="css"} |= "orbit"
{namespace="satellite-rms-8-kafka", pipeline="kafka"}
sum by (pod) (rate({namespace="satellite-rms-8-kafka"}[1m]))
```

---

## Useful PromQL Queries (unchanged from rms-7)

```promql
up{namespace="satellite-rms-8-kafka"}
rate(rms_http_requests_total[1m])
rms_css_degraded_satellites
sum(rate(rms_http_requests_total{status=~"[45].."}[5m]))
```

---

## Teardown

```powershell
# Remove app only (keeps monitoring running)
kubectl delete namespace satellite-rms-8-kafka

# Remove everything
kubectl delete namespace satellite-rms-8-kafka
kubectl delete namespace monitoring
kubectl delete clusterrole prometheus vector-collector
kubectl delete clusterrolebinding prometheus vector-collector

# Full reset
minikube delete
```
