# cluster

A hands-on MongoDB replica set lab demonstrating **CAP theorem trade-offs**, **write concern durability**, **automatic failover**, and **causal consistency** — all running locally via Docker Compose.

---

## What This Explores

Distributed databases make fundamental trade-offs between **Consistency**, **Availability**, and **Partition tolerance** (CAP theorem). This project makes those trade-offs observable and measurable by running a 3-node MongoDB replica set and scripting real experiments against it.

---

## Architecture

```
                        ┌─────────────────────────────────┐
                        │      MongoDB Replica Set (rs0)   │
                        │                                  │
  Python scripts ──────►│  mongo1 :27017  ← PRIMARY        │
                        │  mongo2 :27018  ← SECONDARY       │
                        │  mongo3 :27019  ← SECONDARY       │
                        │                                  │
                        │  Priority: mongo1=5, others=1    │
                        └─────────────────────────────────┘
```

- **mongo1** is the preferred primary (priority 5)
- All three nodes form replica set `rs0` over a shared Docker bridge network
- Scripts connect via the full replica set URI or directly to individual nodes for observation

---

## Experiments

### 1. Consistency Models (`scripts/consistency.py`)

Demonstrates all three consistency levels MongoDB supports, side by side:

| Model | Write Concern | CAP Trade-off | Best For |
|-------|--------------|---------------|----------|
| **Strong** | `w:'majority', j:True` | CP — sacrifices availability | Banking, financial ledgers |
| **Eventual** | `w:1` | AP — allows stale reads | Social feeds, IoT, analytics |
| **Causal** | Session-level | Ordered reads within a session | Comment threads, dependent ops |

Each mode inserts documents and immediately reads from all three nodes to show replication lag in action.

---

### 2. Durability & Write Concern Benchmarking (`scripts/durability.py`, `scripts/write_concern.py`)

Benchmarks 100 writes at each write concern level and measures latency:

```
w:1        → Fastest  (primary-only acknowledgment)
w:majority → Balanced (2 of 3 nodes confirm)
w:3        → Slowest  (all 3 nodes confirm before ack)
```

Also demonstrates what happens when nodes are behind — documents inserted with `w:1` may be missing from secondaries immediately after the write, but converge after replication lag.

---

### 3. Failover Experiment (`scripts/failover.py`)

A live, destructive failover test that:

1. Starts a **continuous background writer** (40 writes, 1s interval, `w:1`)
2. After 10 writes, **stops the primary container** (`docker stop mongo1`)
3. Monitors the **election of a new primary** (up to 60s)
4. Records the **exact timestamps** of first failure and first success after failover
5. Calculates **approximate downtime** (gap between first failure → first success)
6. Restarts the old primary and checks for **replication gaps** (missing sequence numbers)
7. Repeats with `w:majority, j:True` to show those writes survive the failover without data loss

**Key insight:** `w:1` writes acknowledged by the old primary but not yet replicated can be lost during failover. `w:majority` writes are durable.

---

### 4. User Model (`scripts/usermodel.py`)

Demonstrates MongoDB **JSON Schema validation** on a `user_profiles` collection:

- Required fields: `user_id`, `username`, `email`, `created_at`, `is_active`
- Email format validation via regex
- Unique indexes on `user_id`, `username`, `email`
- Index on `last_login_time` for query performance
- `validationLevel: moderate` — existing docs exempt, new/updated docs validated

---

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- Python 3.9+
- `pymongo` installed

```bash
pip install pymongo
```

### 1. Start the replica set

```bash
docker compose up -d
```

### 2. Initialise the replica set

```bash
docker exec -it mongo1 mongosh --eval "
rs.initiate({
  _id: 'rs0',
  members: [
    { _id: 0, host: 'mongo1:27017', priority: 5 },
    { _id: 1, host: 'mongo2:27017', priority: 1 },
    { _id: 2, host: 'mongo3:27017', priority: 1 }
  ]
})"
```

Wait ~5 seconds for the primary election, then verify:

```bash
docker exec -it mongo1 mongosh --eval "rs.status().members.map(m => ({name: m.name, state: m.stateStr}))"
```

### 3. Run the experiments

```bash
# Consistency models (strong / eventual / causal)
python scripts/consistency.py

# Write concern latency benchmark
python scripts/write_concern.py

# Durability & replication lag demo
python scripts/durability.py

# User profile collection with schema validation
python scripts/usermodel.py

# Live failover experiment (destructive — stops mongo1 container)
python scripts/failover.py
```

> ⚠️ `failover.py` calls `docker stop` and `docker start` directly. Run it from the host machine, not inside a container.

---

## Sample Output

### Write concern benchmark
```
✅ Replication Factor (RF): 3

--- Write Concern Benchmark ---
w:1          → Avg:  0.42 ms | Min:  0.31 | Max:  1.84
w:majority   → Avg:  2.17 ms | Min:  1.63 | Max:  6.41
w:3          → Avg:  3.89 ms | Min:  2.91 | Max: 12.30
```

### Failover experiment
```
Primary before experiment: mongo1:27017
*** Stopping primary container mongo1 NOW (destructive) ***
First writer failure at: 2025-10-26T20:00:12.431
New primary elected: mongo2:27017
First writer success after failover: 2025-10-26T20:00:17.882
Approx downtime (s): 5.45

Writer summary: attempts=40, successes=35, failures=5
Missing seqs relative to mongo2 (baseline): [10, 11]  ← w:1 data loss
```

---

## Project Structure

```
mongo-replica/
├── scripts/
│   ├── consistency.py      # Strong / eventual / causal consistency demo
│   ├── durability.py       # Per-node replication lag observation
│   ├── failover.py         # Live destructive failover + downtime measurement
│   ├── usermodel.py        # JSON schema validation + indexing
│   └── write_concern.py    # Write concern latency benchmark (w:1 / majority / w:3)
├── docker-compose.yml      # 3-node MongoDB replica set
├── init-replica.js         # rs.initiate() snippet
└── README.md
```

---

## Key Concepts Demonstrated

- **CAP theorem in practice** — observable consistency vs availability trade-offs, not just theory
- **Write concern durability** — measured latency cost of each level, and what data loss looks like with `w:1` during failover
- **Replica set election** — automatic primary re-election timed and observed live
- **Replication lag** — missing sequence numbers after destructive failover expose unacknowledged writes
- **Causal consistency sessions** — ensures causally related operations (e.g. post → comment) are read in order across nodes
- **JSON Schema validation** — enforcing document structure at the database level, not just the application layer
- **Majority-acked durability** — `w:majority, j:True` writes survive single primary failure with zero data loss

---

## Future Improvements

- [ ] Grafana + Prometheus dashboard for replication lag and op counters
- [ ] Network partition simulation (iptables rules) alongside container stop
- [ ] Jepsen-style linearisability verification
- [ ] Sharding demo alongside replication
