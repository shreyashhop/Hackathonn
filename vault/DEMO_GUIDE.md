# Vault — Demo Guide

## 1. Start Vault

From the `vault/` project directory:

```powershell
docker compose up -d
docker compose ps
```

Expected:
- coordinator — port 8000
- frontend — port 3000
- node-1 to node-6 — ports 8001–8006

Open:

```text
http://localhost:3000
```

---

## 2. Dashboard — Show the Cluster

Open **Dashboard**.

Show:
- 6/6 healthy nodes
- cluster health
- storage capacity
- latency
- live node status
- real-time events

Explain:

> “Vault has one coordinator and six independent storage nodes. The coordinator tracks metadata and cluster health while the nodes independently store object replicas.”

---

## 3. Upload an Object

Open **Object Catalog** and upload a small file.

Show:
- object ID
- size
- SHA-256
- replica information
- successful storage event

Explain:

> “The coordinator computes the object's SHA-256, selects replicas using HRW placement, and writes the object concurrently to the required storage nodes.”

---

## 4. Demonstrate Replication

Open **Replication / Object details**.

Show the object's replica set.

Current policy:
- Replication factor: **3**
- Write quorum: **2**

Explain:

> “Each object normally has three physical replicas. A write succeeds after the configured write quorum is satisfied, while metadata records the authoritative replica state.”

---

## 5. Demonstrate Node Failure and Repair

Open **Fault Lab**.

Use the node-failure control on one storage node.

Then open **Nodes / Repair Center**.

Show:
- node transitioning through failure states
- cluster becoming degraded
- affected replica
- repair task
- healthy replacement target
- repaired replica
- SHA verification
- final replication factor returning to 3

Explain:

> “The health monitor detects failed heartbeats and marks the node down. The repair manager copies the object from a healthy source to a healthy target, verifies size and SHA-256, and only then updates replica metadata.”

---

## 6. Demonstrate Data Corruption

Open **Fault Lab → Corruption**.

Inject corruption into an object.

Open **Data Integrity**.

Show:
- checksum mismatch
- corrupted state
- integrity detection
- repair
- SHA-256 verification
- recovered object

Explain:

> “Vault does not rely only on node availability. It verifies object integrity using SHA-256 and can automatically repair a corrupted replica from a healthy copy.”

---

## 7. Demonstrate Network Partition

Open **Fault Lab → Network Partition**.

Partition one storage node.

Show:
- PARTITIONED / affected node state
- healthy replicas remaining available
- events
- writes continuing when quorum is still satisfied

Then restore the partition.

Show:
- reconciliation
- byte transfer
- checksum verification
- metadata consistency

Explain:

> “The partition simulates an actual communication failure. Vault continues operating through healthy replicas when the configured quorum permits it, then reconciles the recovered node.”

---

## 8. Demonstrate Dynamic Rebalancing

Open **Rebalancing**.

### Before adding a node

Start with the existing 5-node membership and objects already stored.

Register `node-6`.

Show:
- 6/6 healthy
- node-6 eligible
- HRW membership change

Click **Preview Plan**.

If migrations are required, show:
- affected objects
- source and target nodes
- bytes to move
- migration count

Then click **Start Rebalance**.

Show:
- migration progress
- completed migrations
- transferred bytes
- failed migrations
- final balanced state

Explain:

> “Adding a storage node changes HRW placement. Vault calculates only the objects whose placement changes and migrates those replicas. Data is copied and verified before the old replica is removed.”

A verified browser run produced:
- **4 migrations**
- **4/4 completed**
- **9 B transferred**
- **0 failures**

---

## 9. Show Live Events

Open **Events**.

Perform one or two actions while watching the event stream.

Show events such as:
- node status changes
- object operations
- repair activity
- integrity events
- rebalancing events

Explain:

> “The frontend receives operational events through WebSocket rather than relying only on periodic page refreshes.”

---

## 10. Show Topology

Open **Topology / Replication Mesh**.

Show:
- coordinator
- storage nodes
- object-to-replica relationships
- cluster connectivity

Use this to explain the distributed architecture visually.

---

## 11. Final Verification

For a technical verification session, run:

```powershell
docker compose ps
```

Then:

```powershell
curl.exe http://localhost:8000/health
```

Expected:
- coordinator healthy
- 6/6 nodes healthy
- failed heartbeats: 0

Frontend check:

```powershell
curl.exe -I http://localhost:3000
```

Expected:

```text
HTTP/1.1 200 OK
```

---

## 12. Automated Test Results

Current verified results:

| Area | Result |
|---|---:|
| Phase 0 | Passed |
| Phase 1 | Passed |
| Phase 2 | Passed |
| Phase 3 | 13/13 |
| Phase 4 | 14/14 |
| Phase 5 | 21/21 |
| Phase 6 | 25/25 |
| Security | 41/41 |
| Browser rebalancing | 4/4 migrations |

---

## 13. Architecture Summary

```text
                    ┌───────────────────┐
                    │   React Frontend  │
                    │      :3000        │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │  Vault Coordinator│
                    │      :8000        │
                    │                   │
                    │ Metadata / SQLite │
                    │ Health Monitor    │
                    │ Replication       │
                    │ Repair            │
                    │ Integrity         │
                    │ Rebalancing       │
                    │ WebSocket Events  │
                    └─────────┬─────────┘
                              │
          ┌───────────┬───────┼───────┬───────────┐
          ▼           ▼       ▼       ▼           ▼
       Node-1      Node-2  Node-3  Node-4      Node-5
       :8001       :8002   :8003   :8004       :8005
                              │
                              ▼
                           Node-6
                           :8006

                 Each node has independent
                    persistent storage
```

---

## 14. Important Demo Rules

- Do not start a rebalance when the preview shows `0/0` migrations.
- Do not delete demo objects needed for later tests.
- For rebalancing, objects should exist **before** the membership change so HRW redistribution can be observed.
- After fault tests, wait for repair/reconciliation to finish before starting another failure scenario.
- Do not stop Docker volumes during a demo; the volumes contain the node data.
- For a clean demonstration, keep the browser open at `http://localhost:3000`.

---

## 15. Recommended Demo Order

For a complete technical demonstration:

1. Dashboard
2. Upload object
3. Replication
4. Node failure
5. Automatic repair
6. Corruption injection
7. Integrity repair
8. Network partition
9. Partition reconciliation
10. Register node-6
11. Preview rebalancing
12. Start rebalancing
13. Show live events
14. Show final healthy cluster

This order demonstrates the major distributed-storage behaviors without unnecessarily changing the implementation.
