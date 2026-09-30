from collections.abc import Set
import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import httpx

from .config import settings
from .events import manager
from .repair_manager import repair_mgr
from ..db.database import db

HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT = 1.0
SUSPECT_THRESHOLD = 1
DOWN_THRESHOLD = 3


class NodeHealthMonitor:
    """
    Active Node Heartbeat Monitor & State Machine.
    State Machine:
      HEALTHY --(1 failure)--> SUSPECT --(3 failures)--> DOWN
      DOWN --(1 success)--> RECOVERING --(manifest reconcile)--> HEALTHY
    """

    def __init__(self):
        self._nodes: Dict[str, Dict[str, Any]] = {}
        self._monitor_task: Optional[asyncio.Task] = None
        self._running = False

        # Initialize node tracking records
        for n in settings.STORAGE_NODES:
            nid = n["id"]
            self._nodes[nid] = {
                "id": nid,
                "node_id": nid,
                "host": n["host"],
                "port": n["port"],
                "url": n["url"],
                "status": "HEALTHY",
                "last_heartbeat": None,
                "last_successful_heartbeat": None,
                "failed_heartbeats": 0,
                "latency_ms": None,
                "capacity": None,
                "uptime_seconds": None,
                "objects_count": 0,
                "error": None,
            }

        self._partitioned_nodes: Set[str] = set()
        repair_mgr.set_node_health_getter(self.get_all_nodes)

    def start(self):
        if not self._running:
            self._running = True
            self._monitor_task = asyncio.create_task(self._monitor_loop())
            print("[NodeHealthMonitor] Started active heartbeat monitor (tick: 2.0s, timeout: 1.0s)")

    async def stop(self):
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            self._monitor_task = None

    def get_all_nodes(self) -> List[Dict[str, Any]]:
        return list(self._nodes.values())

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        return self._nodes.get(node_id)

    def get_healthy_node_ids(self) -> List[str]:
        return [nid for nid, n in self._nodes.items() if n["status"] == "HEALTHY"]

    def is_node_partitioned(self, node_id: str) -> bool:
        return node_id in self._partitioned_nodes or self._nodes.get(node_id, {}).get("status") == "PARTITIONED"

    async def register_node(self, node_cfg: Dict[str, Any]) -> Dict[str, Any]:
        """Registers a new storage node into the health monitor and begins active heartbeats."""
        nid = node_cfg.get("id") or node_cfg.get("node_id")
        host = node_cfg["host"]
        port = int(node_cfg["port"])
        url = node_cfg.get("url") or f"http://{host}:{port}"

        # Save to database
        db.register_storage_node({
            "id": nid,
            "host": host,
            "port": port,
            "url": url,
            "status": "HEALTHY",
        })

        node_rec = {
            "node_id": nid,
            "id": nid,
            "host": host,
            "port": port,
            "url": url,
            "status": "HEALTHY",
            "last_heartbeat": None,
            "last_successful_heartbeat": None,
            "failed_heartbeats": 0,
            "latency_ms": None,
            "capacity": None,
            "uptime_seconds": None,
            "objects_count": 0,
            "error": None,
        }
        self._nodes[nid] = node_rec

        # Probe immediately in background
        asyncio.create_task(self._probe_single_node(node_rec))

        now = datetime.now(timezone.utc).isoformat()
        await manager.broadcast("REBALANCE_NODE_REGISTERED", {
            "node_id": nid,
            "host": host,
            "port": port,
            "url": url,
            "timestamp": now,
        })
        return node_rec

    async def _probe_single_node(self, node_rec: Dict[str, Any]):
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                await self._check_node(node_rec, client)
        except Exception as e:
            print(f"[NodeHealthMonitor] Initial probe for {node_rec.get('node_id')}: {e}")

    def set_node_status(self, node_id: str, status: str):
        """Sets node status with state change event broadcast."""
        if node_id in self._nodes:
            prev = self._nodes[node_id]["status"]
            self._nodes[node_id]["status"] = status
            now = datetime.now(timezone.utc).isoformat()
            asyncio.create_task(manager.broadcast("NODE_STATE_CHANGED", {
                "node_id": node_id,
                "previous_status": prev,
                "current_status": status,
                "timestamp": now,
            }))

    def deregister_node(self, node_id: str, purge: bool = True) -> bool:
        """Decommissions and stops monitoring a node."""
        if node_id in self._nodes:
            self._nodes[node_id]["status"] = "DECOMMISSIONED"
            db.deregister_storage_node(node_id)
            now = datetime.now(timezone.utc).isoformat()
            asyncio.create_task(manager.broadcast("REBALANCE_NODE_DECOMMISSIONED", {
                "node_id": node_id,
                "timestamp": now,
            }))
            if purge:
                self._nodes.pop(node_id, None)
            return True
        return False

    async def partition_node(self, node_id: str):
        """Immediately marks node as partitioned and isolates its communication."""
        self._partitioned_nodes.add(node_id)
        node_rec = self._nodes.get(node_id)
        now = datetime.now(timezone.utc).isoformat()
        if node_rec:
            prev_status = node_rec["status"]
            node_rec["failed_heartbeats"] = DOWN_THRESHOLD
            node_rec["error"] = f"Network partition: communication isolated with {node_id}"
            node_rec["status"] = "PARTITIONED"

            # Replicas on this node become PARTITIONED
            db.update_replica_status_by_node(node_id, "STORED", "PARTITIONED")

            await manager.broadcast("NETWORK_PARTITION_STARTED", {
                "node_id": node_id,
                "timestamp": now,
            })
            await manager.broadcast("NODE_PARTITIONED", {
                "node_id": node_id,
                "timestamp": now,
            })
            await manager.broadcast("NODE_STATE_CHANGED", {
                "node_id": node_id,
                "previous_status": prev_status,
                "current_status": "PARTITIONED",
                "timestamp": now,
            })
            print(f"[NodeHealthMonitor] Node {node_id} transitioned to PARTITIONED.")

    async def restore_node_partition(self, node_id: str):
        """Restores communication and initiates partition reconciliation."""
        node_cfg = None
        for n in settings.STORAGE_NODES:
            if n["id"] == node_id:
                node_cfg = n
                break
        if not node_cfg and node_id in self._nodes:
            node_cfg = self._nodes[node_id]
        if not node_cfg:
            return

        # 1. Instruct storage node to lift partition
        async with httpx.AsyncClient(timeout=3.0) as client:
            try:
                await client.post(f"{node_cfg['url']}/fault/restore")
            except Exception as e:
                print(f"[NodeHealthMonitor] Warning restoring node network: {e}")

        # 2. Update state in coordinator
        self._partitioned_nodes.discard(node_id)
        node_rec = self._nodes.get(node_id)
        now = datetime.now(timezone.utc).isoformat()
        if node_rec:
            prev_status = node_rec["status"]
            node_rec["failed_heartbeats"] = 0
            node_rec["error"] = None
            node_rec["status"] = "RECOVERING"

            await manager.broadcast("NETWORK_PARTITION_REMOVED", {
                "node_id": node_id,
                "timestamp": now,
            })
            await manager.broadcast("NODE_RECOVERING", {
                "node_id": node_id,
                "previous_status": prev_status,
                "current_status": "RECOVERING",
                "timestamp": now,
            })
            await manager.broadcast("NODE_STATE_CHANGED", {
                "node_id": node_id,
                "previous_status": prev_status,
                "current_status": "RECOVERING",
                "timestamp": now,
            })

        # 3. Trigger reconciliation
        asyncio.create_task(self._reconcile_partitioned_node(node_cfg))

    async def _monitor_loop(self):
        # Initial brief sleep to let containers finish initialization
        await asyncio.sleep(1.0)
        async with httpx.AsyncClient(timeout=HEARTBEAT_TIMEOUT) as client:
            while self._running:
                try:
                    await self._tick(client)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    print(f"[NodeHealthMonitor] Tick error: {e}")
                await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _tick(self, client: httpx.AsyncClient):
        # Check all registered nodes that are not decommissioned
        active_nodes = [
            n for n in list(self._nodes.values())
            if n.get("status") != "DECOMMISSIONED"
        ]
        tasks = [self._check_node(node_rec, client) for node_rec in active_nodes]
        await asyncio.gather(*tasks, return_exceptions=True)

        healthy_count = sum(1 for n in self._nodes.values() if n["status"] == "HEALTHY")
        partitioned_count = sum(1 for n in self._nodes.values() if n["status"] == "PARTITIONED")
        cluster_state = "HEALTHY" if healthy_count == len(self._nodes) else ("DEGRADED" if healthy_count > 0 else "CRITICAL")

        # Periodic cluster heartbeat event
        await manager.broadcast("CLUSTER_HEARTBEAT", {
            "total_nodes": len(self._nodes),
            "healthy_nodes": healthy_count,
            "suspect_nodes": sum(1 for n in self._nodes.values() if n["status"] == "SUSPECT"),
            "down_nodes": sum(1 for n in self._nodes.values() if n["status"] == "DOWN"),
            "partitioned_nodes": partitioned_count,
            "recovering_nodes": sum(1 for n in self._nodes.values() if n["status"] == "RECOVERING"),
            "cluster_state": cluster_state,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    async def _check_node(self, node_cfg: Dict[str, Any], client: httpx.AsyncClient):
        nid = node_cfg.get("id") or node_cfg.get("node_id")
        node_rec = self._nodes.get(nid)
        if not node_rec:
            return
        url = f"{node_cfg['url']}/health"
        now = datetime.now(timezone.utc).isoformat()
        node_rec["last_heartbeat"] = now

        start_time = time.time()
        success = False
        health_data = None
        error_msg = None

        try:
            resp = await client.get(url)
            latency = round((time.time() - start_time) * 1000, 2)
            node_rec["latency_ms"] = latency

            if resp.status_code == 200:
                success = True
                health_data = resp.json()
            else:
                error_msg = f"HTTP {resp.status_code}"
        except httpx.TimeoutException:
            error_msg = f"Heartbeat timed out after {HEARTBEAT_TIMEOUT}s"
        except httpx.RequestError as exc:
            error_msg = f"Connection error: {str(exc)}"
        except Exception as exc:
            error_msg = str(exc)

        # Apply State Machine Transitions
        prev_status = node_rec["status"]

        if success and health_data:
            node_rec["last_successful_heartbeat"] = now
            node_rec["failed_heartbeats"] = 0
            node_rec["capacity"] = health_data.get("capacity")
            node_rec["uptime_seconds"] = health_data.get("uptime_seconds")
            node_rec["objects_count"] = health_data.get("objects_count", 0)
            node_rec["error"] = None

            # If node was recovering or suspect, transition back to HEALTHY
            if prev_status == "DOWN":
                node_rec["status"] = "RECOVERING"
                await manager.broadcast("NODE_RECOVERING", {
                    "node_id": nid,
                    "previous_status": prev_status,
                    "current_status": "RECOVERING",
                    "timestamp": now,
                })
                await manager.broadcast("NODE_STATE_CHANGED", {
                    "node_id": nid,
                    "previous_status": prev_status,
                    "current_status": "RECOVERING",
                    "timestamp": now,
                })
                asyncio.create_task(self._reconcile_recovering_node(node_cfg))
            elif prev_status == "PARTITIONED":
                # Node became reachable without explicit /restore API call
                self._partitioned_nodes.discard(nid)
                node_rec["status"] = "RECOVERING"
                await manager.broadcast("NETWORK_PARTITION_REMOVED", {
                    "node_id": nid,
                    "timestamp": now,
                })
                await manager.broadcast("NODE_RECOVERING", {
                    "node_id": nid,
                    "previous_status": prev_status,
                    "current_status": "RECOVERING",
                    "timestamp": now,
                })
                asyncio.create_task(self._reconcile_partitioned_node(node_cfg))
            elif prev_status == "SUSPECT":
                node_rec["status"] = "HEALTHY"
                await manager.broadcast("NODE_STATE_CHANGED", {
                    "node_id": nid,
                    "previous_status": prev_status,
                    "current_status": "HEALTHY",
                    "timestamp": now,
                })
            elif prev_status == "RECOVERING":
                # Asynchronous reconciliation is in progress; let it finish and transition to HEALTHY
                pass
            else:
                node_rec["status"] = "HEALTHY"

        else:
            # Heartbeat Failure
            node_rec["failed_heartbeats"] += 1
            node_rec["error"] = error_msg
            failed_count = node_rec["failed_heartbeats"]

            if nid in self._partitioned_nodes:
                # Node is partitioned
                await manager.broadcast("PARTITION_REQUEST_FAILED", {
                    "node_id": nid,
                    "operation": "heartbeat",
                    "error": error_msg,
                    "timestamp": now,
                })

                if prev_status == "HEALTHY" and failed_count >= SUSPECT_THRESHOLD:
                    node_rec["status"] = "SUSPECT"
                    await manager.broadcast("NODE_SUSPECT", {
                        "node_id": nid,
                        "failed_count": failed_count,
                        "error": error_msg,
                        "timestamp": now,
                    })
                    await manager.broadcast("NODE_STATE_CHANGED", {
                        "node_id": nid,
                        "previous_status": prev_status,
                        "current_status": "SUSPECT",
                        "timestamp": now,
                    })
                elif prev_status in ("HEALTHY", "SUSPECT") and failed_count >= DOWN_THRESHOLD:
                    node_rec["status"] = "PARTITIONED"
                    db.update_replica_status_by_node(nid, "STORED", "PARTITIONED")
                    await manager.broadcast("NODE_PARTITIONED", {
                        "node_id": nid,
                        "timestamp": now,
                    })
                    await manager.broadcast("NODE_STATE_CHANGED", {
                        "node_id": nid,
                        "previous_status": prev_status,
                        "current_status": "PARTITIONED",
                        "timestamp": now,
                    })
            else:
                # Standard node failure (Phase 3)
                if prev_status == "HEALTHY" and failed_count >= SUSPECT_THRESHOLD:
                    node_rec["status"] = "SUSPECT"
                    await manager.broadcast("NODE_SUSPECT", {
                        "node_id": nid,
                        "failed_count": failed_count,
                        "error": error_msg,
                        "timestamp": now,
                    })
                    await manager.broadcast("NODE_STATE_CHANGED", {
                        "node_id": nid,
                        "previous_status": prev_status,
                        "current_status": "SUSPECT",
                        "timestamp": now,
                    })

                elif prev_status == "SUSPECT" and failed_count >= DOWN_THRESHOLD:
                    node_rec["status"] = "DOWN"
                    await manager.broadcast("NODE_DOWN", {
                        "node_id": nid,
                        "failed_count": failed_count,
                        "error": error_msg,
                        "timestamp": now,
                    })
                    await manager.broadcast("NODE_STATE_CHANGED", {
                        "node_id": nid,
                        "previous_status": prev_status,
                        "current_status": "DOWN",
                        "timestamp": now,
                    })

                    # Mark replicas on this node as UNAVAILABLE
                    updated_count = db.update_replica_status_by_node(nid, "STORED", "UNAVAILABLE")
                    print(f"[NodeHealthMonitor] Node {nid} marked DOWN. {updated_count} replicas set to UNAVAILABLE.")

                    # Trigger automatic repair
                    asyncio.create_task(repair_mgr.trigger_repairs_for_node(nid))

    async def _reconcile_partitioned_node(self, node_cfg: Dict[str, Any]):
        """
        Reconciles returning partitioned node's physical manifest with coordinator catalog.
        - Deletes stale objects that were removed while partitioned (preventing resurrection).
        - Prunes redundant 4th copies if RF=3 was already restored elsewhere (Rule 17).
        - Reconciles missing replicas that were written while partitioned.
        - Reconciles checksum mismatches.
        - Verifies bytes and updates replica status to STORED.
        - Transitions node RECOVERING -> HEALTHY.
        """
        nid = node_cfg.get("id") or node_cfg.get("node_id")
        node_url = node_cfg.get("url") or f"http://{node_cfg.get('host')}:{node_cfg.get('port')}"
        manifest_url = f"{node_url}/manifest"
        print(f"[NodeHealthMonitor] Starting partition reconciliation for node {nid}...")

        enqueued_jobs = []

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.get(manifest_url)
                if res.status_code == 200:
                    manifest = res.json().get("items", [])
                    manifest_map = {item["object_id"]: item for item in manifest}
                    print(f"[NodeHealthMonitor] Node {nid} reported {len(manifest)} physical items.")

                    # 1. Check existing manifest items
                    for item in manifest:
                        oid = item["object_id"]
                        obj = db.get_object(oid)
                        if not obj:
                            # Stale object deleted while partitioned! Purge it to prevent resurrection.
                            print(f"[NodeHealthMonitor] Purging stale object {oid} deleted while partitioned from node {nid}.")
                            try:
                                await client.delete(f"{node_url}/delete/{oid}")
                            except Exception as e:
                                print(f"[NodeHealthMonitor] Failed deleting stale object {oid}: {e}")
                            db.delete_replica(oid, nid)
                            continue

                        # Check if RF=3 is already satisfied by other healthy nodes
                        all_reps = db.get_replicas_for_object(oid)
                        other_healthy = [
                            r for r in all_reps
                            if r["node_id"] != nid and r["status"] == "STORED"
                            and r.get("sha256") == obj["sha256"]
                            and self._nodes.get(r["node_id"], {}).get("status") == "HEALTHY"
                        ]
                        if len(other_healthy) >= settings.REPLICATION_FACTOR:
                            print(f"[NodeHealthMonitor] Object {oid} already has {len(other_healthy)} replicas. Pruning redundant replica on {nid}.")
                            try:
                                await client.delete(f"{node_url}/delete/{oid}")
                            except Exception:
                                pass
                            db.delete_replica(oid, nid)
                            continue

                        # Check if checksum matches
                        if item["sha256"] == obj["sha256"] and item["size_bytes"] == obj["size_bytes"]:
                            db.update_replica_status(oid, nid, "STORED", item["size_bytes"], item["sha256"])
                            print(f"[NodeHealthMonitor] Verified valid replica {oid} on {nid} -> STORED.")
                        else:
                            print(f"[NodeHealthMonitor] Checksum mismatch for {oid} on {nid}. Reconciling.")
                            await repair_mgr.trigger_partition_reconciliation(oid, nid)
                            enqueued_jobs.append(oid)

                    # 2. Check for missing replicas written while partitioned
                    all_objects = db.list_objects()
                    for obj in all_objects:
                        oid = obj["object_id"]
                        if oid in enqueued_jobs:
                            continue
                        all_reps = db.get_replicas_for_object(oid)
                        rep_on_this_node = next((r for r in all_reps if r["node_id"] == nid), None)

                        if rep_on_this_node and rep_on_this_node["status"] in ("PARTITIONED", "FAILED", "PENDING"):
                            if oid not in manifest_map or manifest_map[oid]["sha256"] != obj["sha256"]:
                                print(f"[NodeHealthMonitor] Object {oid} missing replica on {nid}. Reconciling.")
                                await repair_mgr.trigger_partition_reconciliation(oid, nid)
                                enqueued_jobs.append(oid)
                            else:
                                db.update_replica_status(oid, nid, "STORED", obj["size_bytes"], obj["sha256"])

        except Exception as e:
            print(f"[NodeHealthMonitor] Manifest query failed for {nid}: {e}")

        # Wait for all reconciliation jobs for this node to finish
        max_wait = 20.0
        start_wait = time.time()
        while time.time() - start_wait < max_wait:
            active_jobs = [
                oid for oid in enqueued_jobs
                if db.get_active_repair_for_object(oid, target_node_id=nid)
            ]
            if not active_jobs:
                break
            await asyncio.sleep(0.5)

        # Transition RECOVERING -> HEALTHY
        node_rec = self._nodes.get(nid)
        if node_rec:
            prev_status = node_rec["status"]
            node_rec["status"] = "HEALTHY"
            node_rec["error"] = None
            now = datetime.now(timezone.utc).isoformat()

            await manager.broadcast("NODE_HEALTHY", {
                "node_id": nid,
                "timestamp": now,
            })
            await manager.broadcast("NODE_RECOVERED", {
                "node_id": nid,
                "timestamp": now,
            })
            await manager.broadcast("NODE_STATE_CHANGED", {
                "node_id": nid,
                "previous_status": prev_status,
                "current_status": "HEALTHY",
                "timestamp": now,
            })
            print(f"[NodeHealthMonitor] Node {nid} reconciliation completed. Node marked HEALTHY.")

    async def _reconcile_recovering_node(self, node_cfg: Dict[str, Any]):
        """
        Reconciles returning node's physical manifest with coordinator catalog.
        Rule 17: If an object was already repaired and has 3 healthy replicas,
        do NOT create a 4th replica. Prune the redundant copy.
        """
        nid = node_cfg.get("id") or node_cfg.get("node_id")
        node_url = node_cfg.get("url") or f"http://{node_cfg.get('host')}:{node_cfg.get('port')}"
        manifest_url = f"{node_url}/manifest"
        print(f"[NodeHealthMonitor] Reconciling manifest for returning node {nid}...")

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.get(manifest_url)
                if res.status_code == 200:
                    manifest = res.json().get("items", [])
                    print(f"[NodeHealthMonitor] Node {nid} reported {len(manifest)} physical objects.")

                    for item in manifest:
                        oid = item["object_id"]
                        obj = db.get_object(oid)
                        if not obj:
                            # Stale object deleted while down! Purge it to prevent resurrection.
                            try:
                                await client.delete(f"{node_url}/delete/{oid}")
                            except Exception:
                                pass
                            db.delete_replica(oid, nid)
                            continue

                        # Check healthy replicas excluding this returning node
                        all_reps = db.get_replicas_for_object(oid)
                        other_healthy = [
                            r for r in all_reps
                            if r["node_id"] != nid and r["status"] == "STORED"
                            and r.get("sha256") == obj["sha256"]
                            and self._nodes.get(r["node_id"], {}).get("status") == "HEALTHY"
                        ]

                        if len(other_healthy) >= settings.REPLICATION_FACTOR:
                            # Rule 17: Object already has RF=3 healthy replicas elsewhere!
                            # Delete redundant replica from returning node to maintain RF=3
                            print(f"[NodeHealthMonitor] Object {oid} already has {len(other_healthy)} replicas. Pruning redundant copy from {nid}.")
                            try:
                                await client.delete(f"{node_url}/delete/{oid}")
                            except Exception as e:
                                print(f"[NodeHealthMonitor] Failed deleting physical redundant replica {oid} on {nid}: {e}")
                            db.delete_replica(oid, nid)
                        else:
                            # Restore replica status to STORED if checksum matches
                            if item["sha256"] == obj["sha256"]:
                                db.update_replica_status(oid, nid, "STORED", item["size_bytes"], item["sha256"])
                                print(f"[NodeHealthMonitor] Restored replica {oid} on returning node {nid} to STORED.")

                    # Also inspect all catalog objects where this node still has a replica record
                    all_objects = db.list_objects()
                    for obj in all_objects:
                        oid = obj["object_id"]
                        all_reps = db.get_replicas_for_object(oid)
                        rep_on_this = next((r for r in all_reps if r["node_id"] == nid), None)
                        if rep_on_this:
                            other_healthy = [
                                r for r in all_reps
                                if r["node_id"] != nid and r["status"] == "STORED"
                                and r.get("sha256") == obj["sha256"]
                                and self._nodes.get(r["node_id"], {}).get("status") == "HEALTHY"
                            ]
                            if len(other_healthy) >= settings.REPLICATION_FACTOR:
                                try:
                                    await client.delete(f"{node_url}/delete/{oid}")
                                except Exception:
                                    pass
                                db.delete_replica(oid, nid)
        except Exception as e:
            print(f"[NodeHealthMonitor] Manifest reconciliation failed for {nid}: {e}")

        # Transition RECOVERING -> HEALTHY
        node_rec = self._nodes.get(nid)
        if node_rec:
            prev_status = node_rec["status"]
            node_rec["status"] = "HEALTHY"
            node_rec["error"] = None
            now = datetime.now(timezone.utc).isoformat()

            await manager.broadcast("NODE_HEALTHY", {
                "node_id": nid,
                "timestamp": now,
            })
            await manager.broadcast("NODE_RECOVERED", {
                "node_id": nid,
                "timestamp": now,
            })
            await manager.broadcast("NODE_STATE_CHANGED", {
                "node_id": nid,
                "previous_status": prev_status,
                "current_status": "HEALTHY",
                "timestamp": now,
            })
            print(f"[NodeHealthMonitor] Node {nid} successfully recovered and marked HEALTHY.")


# Singleton instance
health_monitor = NodeHealthMonitor()
