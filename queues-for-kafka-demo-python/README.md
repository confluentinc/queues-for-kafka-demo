# Queues for Kafka Demo (Python)

[Queues for Kafka](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka) (KIP-932) brings true queue semantics to Apache Kafka through **share groups**. Unlike traditional consumer groups where each partition is exclusively assigned to one consumer, share groups allow multiple consumers to process messages from the same partition concurrently, while Kafka still ensures each message is delivered to exactly one consumer at a time.

> [!IMPORTANT]
> This is a Python port of Confluent's [queues-for-kafka-demo](https://github.com/confluentinc/queues-for-kafka-demo) (itself based on [ifnesi's original](https://github.com/ifnesi/queues-for-kafka)), built on [confluent-kafka-python's `ShareConsumer`](https://github.com/confluentinc/confluent-kafka-python/blob/master/docs/kip-932-share-consumer.md). 

It turns KIP-932 into a restaurant kitchen:

- **Waiters** (a standard producer) submit orders to a Kafka topic
- **Chefs** (`ShareConsumer` workers in a share group) each grab orders off the queue
- An **inventory tracker** (standard consumer group) watches every order flow through
- A **dashboard** ties it all together with live auto-scaling based on queue depth

This version runs entirely against a local Kafka broker in Docker — no Terraform, no Confluent Cloud account needed.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (with Docker Compose)
- Python 3.9+

**Mac (Homebrew):**

```bash
brew install python@3.11
```

## Getting Started

### 1. Start Kafka

```bash
docker compose up -d
```

This starts a single-node Confluent Platform broker (`confluentinc/cp-kafka`, KRaft mode, built on Apache Kafka 4.2.x+) on `localhost:9092`, then runs a one-shot `kafka-init` job that enables share groups (`share.version=1`) and creates the `orders-queue` topic. Wait for it to finish:

```bash
docker compose logs -f kafka-init
```

You should see `Kafka ready: share groups enabled, topic 'orders-queue' created.` — then Ctrl+C out of the log tail.

### 2. Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> The `ShareConsumer` API is a **Preview** feature, currently only in `confluent-kafka` >= 2.15.0. It requires a broker with share groups enabled (Apache Kafka 4.2.0+), which is exactly what `docker compose up` gives you above.

### 3. Start the Dashboard

```bash
python -m qfk_demo.dashboard_server
```

Open http://localhost:8080 — you'll see an empty kitchen, no orders yet. The dashboard also spins up the inventory consumer and the chef auto-scaler (which starts with the minimum number of chefs, all idle).

> **Give the chefs a few seconds before sending orders.** Share groups default to `share.auto.offset.reset=latest`, and a brand-new share group's partition assignment can take ~10-20s to stabilize on first join (this is a broker-driven heartbeat protocol, not an instant rebalance). Orders produced before a chef's assignment stabilizes won't be seen by that chef. In practice, the time it takes to open the dashboard in a browser and switch to another terminal is usually enough — but if you script this demo, add a short sleep after starting chefs/dashboard before producing.

### 4. Start the Producer

In a separate terminal (with the virtualenv activated):

```bash
python -m qfk_demo.producer
```

The producer fires off 40 orders immediately to flood the kitchen, then keeps a steady stream going. Back on the dashboard, you'll see chefs spin up to handle the backlog, then scale back down as the queue drains.

## What to Look For

### Accept, Release, Reject

Share groups give consumers new ways to handle a message that plain consumer groups don't have:

| Action | What Happens |
|--------|--------------|
| **Accept** | Done — message is acknowledged and leaves the queue |
| **Release** | Put it back — another consumer will pick it up |
| **Reject** | Discard it (bad data, unprocessable, etc.) — archived after the delivery limit |

Each chef randomly accepts/releases/rejects orders (tunable via `chef.accept.rate` in `config.properties`) so you can watch redelivery and `delivery_count()` climb on released orders in the chef's terminal output.

### Auto-Scaling

The dashboard watches queue depth and spins chefs up or down automatically. When the producer sends that initial burst of 40 orders, you'll see chefs appear to handle the spike, then disappear as things calm down.

### Multiple Consumers, One Partition

In traditional Kafka, one partition means one consumer per group. Share groups break that rule by allowing multiple consumers in a single group. The topic in this demo runs with a single partition, but scales to as many chefs as needed.

### Queuing and Streaming Side by Side

The demo runs both patterns on the same topic at the same time:

- A **share group** (`chefs-share-group`) for queue-style processing — each order goes to one chef
- A **standard consumer group** (`inventory-analytics-group`) for streaming — the inventory system sees every order

## Configuration

Settings live in [`config.properties`](config.properties) at the project root, read once at startup by every component (dashboard, producer, chefs, inventory consumer). Most lines ship commented out, showing their default — uncomment a line and change its value to override it. The ShareConsumer (KIP-932) settings below are the exception: they ship active (uncommented), since they're the literal config the chefs' `ShareConsumer` is constructed with. All components read the same file, so a change there applies everywhere without needing to export anything per-terminal.

To point at a different file (e.g. for a second demo instance), set the `QFK_CONFIG_FILE` environment variable to its path.

| Key | Default | Description |
|-----|---------|--------------|
| `bootstrap.servers` | `localhost:9092` | Kafka bootstrap servers |
| `dashboard.url` | `http://localhost:8080` | Where chefs/autoscaler report status |
| `port` | `8080` | Dashboard HTTP port |
| `order.interval.ms` | `5000` | Delay between orders after the initial burst |
| `chef.processing.delay.ms` | `2000` | Simulated per-order processing time (min 2000ms) |
| `chef.accept.rate` | `0.80` | Fraction of orders a chef accepts (rest split release/reject) |
| `auto.scale.enabled` | `true` | Enable the chef auto-scaler |
| `auto.scale.min.chefs` | `1` | Minimum chef processes |
| `auto.scale.max.chefs` | `4` | Maximum chef processes |
| `auto.scale.up.threshold` | `5` | Queue depth per chef that triggers scale-up |
| `auto.scale.down.threshold` | `2` | Queue depth at/below which the scaler returns to the minimum |
| `auto.scale.check.interval.ms` | `3000` | How often the autoscaler re-evaluates |
| `chef.group.id` *(active)* | `chefs-share-group` | KIP-932 share group ID the chefs' `ShareConsumer` joins |
| `share.acknowledgement.mode` *(active)* | `explicit` | ShareConsumer ack mode — `explicit` is what lets chefs accept/release/reject individually; `implicit` auto-accepts on the next poll/commit |
| `manual.scale.mode` | `false` | Disable automatic scaling; use the dashboard's Add/Remove Chef buttons instead |

### Running a chef by hand

You can also start chefs manually instead of relying on the auto-scaler (uncomment `auto.scale.enabled=false` in `config.properties` first to avoid double-scaling):

```bash
python -m qfk_demo.chef_consumer "Chef Manual 1"
```

Start a second one in another terminal and watch orders split between them — that's the share group in action.

## Teardown

Stop things in the reverse order you started them:

### 1. Stop the producer

In the producer's terminal, press `Ctrl+C`. It finishes flushing in-flight messages and exits.

### 2. Stop the dashboard

In the dashboard's terminal, press `Ctrl+C`. This also stops the inventory consumer and tells the auto-scaler to terminate every chef process it spawned (`Chef-Auto-*`) — you don't need to stop those separately.

If you started any chefs by hand (`python -m qfk_demo.chef_consumer ...`), `Ctrl+C` each of those terminals too.

### 3. Check for orphaned processes

If a terminal was closed instead of `Ctrl+C`'d (or the dashboard was killed with `kill -9`), spawned chef processes can be left running. Check for and clean those up:

```bash
pgrep -fl "qfk_demo"          # list anything still running
pkill -f "qfk_demo.chef_consumer"   # kill any orphaned chefs
```

### 4. Stop Kafka

```bash
docker compose down
```

This stops and removes the broker container (and the one-shot `kafka-init` container, if it's still around). There are no named volumes, so this also discards the broker's topic data — a fresh `docker compose up -d` starts from an empty topic.

### 5. (Optional) Wipe dashboard state

To reset accepted/rejected order history and inventory counts between runs:

```bash
rm -f dashboard-state.json
```

## Project Structure

```
qfk_demo/
  models.py              # Order / OrderItem + JSON (de)serialization
  config.py               # Loads settings from config.properties
  producer.py             # Waiters: automated order producer
  chef_consumer.py        # Chefs: KIP-932 ShareConsumer worker
  inventory_consumer.py   # Inventory tracker: standard consumer group
  autoscaler.py            # Spawns/terminates chef processes based on queue depth
  dashboard_server.py      # Flask REST API + serves public/
public/
  index.html, dashboard.js, styles.css   # Dashboard UI (polls the REST API)
config.properties         # All configuration, defaults shown as comments
docker-compose.yml         # Local Confluent Platform (KRaft) broker + share-group/topic init
```

## Learn More

- [KIP-932: Queues for Kafka](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)
- [confluent-kafka-python Share Consumer guide](https://github.com/confluentinc/confluent-kafka-python/blob/master/docs/kip-932-share-consumer.md)
- [Confluent Developer Portal](https://developer.confluent.io)
