# Autoscaling QfK Consumers with KEDA on Confluent Cloud

The KEDA version of the restaurant kitchen demo runs on Kubernetes. Confluent Cloud handles the Kafka side with a Dedicated cluster with Queues for Kafka, while KEDA manages consumer pods locally, scaling them based on how many orders are sitting in the queue.

## How It Works

In this demo, we make use of a metric published in the Kafka Admin API to help with scaling. 

1. A **lag exporter** calls `AdminClient.listShareGroupOffsets()` against Confluent Cloud and publishes the backlog as a Prometheus gauge (`qfk_share_group_backlog`)
2. **Prometheus** scrapes that metric every second
3. **KEDA** queries Prometheus and defines the scaling logic based on the size of the backlog
4. Kubernetes adds or removes consumer pods based on the scaling parameters

Share groups make this work well. Unlike traditional consumer groups, there's no rebalancing when pods come and go. New consumers start getting messages immediately, and messages from terminated pods get retried automatically. KEDA can scale freely without causing throughput dips.


## Prerequisites

- A **Confluent Cloud Dedicated cluster** with Queues for Kafka. The [Terraform](../terraform/) in this repo creates one
- [Kind](https://kind.sigs.k8s.io/) — `setup.sh` creates a local cluster
- Helm 3 — used by `setup.sh` for Prometheus and KEDA
- Java 17+ and Maven 3.8+
- Docker
- kubectl (Kind configures it for you)

**Mac (Homebrew):**

```bash
brew install kind helm kubectl
```

> **Windows:** Everything works on Windows too. The `.gitattributes` file keeps Dockerfiles and YAML on LF line endings. Use PowerShell or Git Bash for the commands below.


## Step-by-Step

### 1. Set up Confluent Cloud

If you haven't created the Confluent Cloud Dedicated Cluster from the [main demo](../README.md) already, do so now. You'll need a **Cloud resource management** API key. Create one at https://confluent.cloud/settings/api-keys (pick "Cloud resource management", not "Kafka cluster").

```bash
cd ../terraform
terraform init
terraform apply
```

When it finishes, grab the values for the Kubernetes manifests:

```bash
terraform output k8s_values
```

Keep this output handy — you'll need it in step 4.

```bash
cd ../keda-scaler
```

### 2. Create the Kind cluster and install Prometheus + KEDA

```bash
./setup.sh
```

This creates a Kind cluster named `qfk-demo`, installs Prometheus (in `monitoring` namespace) and KEDA (in `keda` namespace). The Prometheus address matches what the ScaledObject expects out of the box, and annotation-based scraping picks up the lag exporter automatically.

### 3. Build the Docker images

Three images:

- **Lag exporter** — queries Confluent Cloud for the share group backlog, exposes it as a Prometheus metric
- **Consumer** — the same QConsumer from the main demo, containerized
- **Dashboard** — the web UI

```bash
# Build and test the exporter
mvn clean package
docker build -t qfk-lag-exporter:latest .

# Consumer and dashboard build from the repo root
cd ..
docker build -f keda-scaler/Dockerfile.consumer -t my-qfk-consumer:latest .
docker build -f keda-scaler/Dockerfile.dashboard -t my-qfk-dashboard:latest .
cd keda-scaler
```

Load them into Kind (locally-built images aren't automatically visible to the cluster):

```bash
kind load docker-image qfk-lag-exporter:latest --name qfk-demo
kind load docker-image my-qfk-consumer:latest --name qfk-demo
kind load docker-image my-qfk-dashboard:latest --name qfk-demo
```

### 4. Configure the Kubernetes manifests

Fill in the placeholders in `k8s/` using the Terraform output from step 1:

| File | What to change |
|------|---------------|
| `k8s/ccloud-admin-secret.yaml` | Your `KAFKA_API_KEY` and `KAFKA_API_SECRET` |
| `k8s/qfk-configmap.yaml` | Your cluster's `KAFKA_BOOTSTRAP_SERVERS` endpoint |

### 5. Deploy

```bash
kubectl apply -f k8s/
```

### 6. Open the dashboard

```bash
kubectl port-forward -n qfk-autoscaling-demo svc/qfk-dashboard 8080:8080
```

### 7. Watch it scale

Open [http://localhost:8080](http://localhost:8080). The dashboard shows chef status, inventory, and queue depth from the lag exporter. Click the "Generate 40 Orders" button to send a burst and watch KEDA scale up consumer pods.

As orders pile up, new pods appear within seconds. The ScaledObject scales from 1 to 4 replicas based on a threshold of 5 messages per consumer. When the backlog clears, KEDA scales back down.


## Teardown

Remove the Kubernetes resources:

```bash
kubectl delete -f k8s/ --ignore-not-found
```

To also tear down the Confluent Cloud cluster and stop billing:

```bash
cd ../terraform
terraform destroy
```