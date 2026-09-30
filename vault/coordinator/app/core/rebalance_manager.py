import os
import uuid
import time
import asyncio
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import httpx

from .config import settings
from .events import manager
from .replication import select_replica_nodes, compute_hrw_score
from .health_monitor import health_monitor
from ..db.database import db

MAX_CONCURRENT_REBALANCES = int(os.getenv("MAX_CONCURRENT_REBALANCES", "2"))


class RebalanceManager:
    """
    Asynchronous Dynamic Rebalancing Engine.
    Responsibilities:
      - Node membership changes (register, decommission)
      - Deterministic HRW migration plan generation with minimal churn
      - Bounded-concurrency physical object migration
      - Mandatory Copy-Before-Delete safety invariant
      - Real-time WebSocket telemetry and SQLite persistence
      - Idempotency and safe cancellation
    """

    def __init__(self):
        self._concurrency_limit = MAX_CONCURRENT_REBALANCES
        self._semaphore = asyncio.Semaphore(self._concurrency_limit)
        self._lock = asyncio.Lock()
        self._cancel_event = asyncio.Event()
        self._current_task: Optional[asyncio.Task] = None
        self._active_run_id: Optional[str] = None
        self._active_migrations: List[Dict[str, Any]] = []

    def start(self):
        print(f"[RebalanceManager] Engine initialized (max concurrency: {self._concurrency_limit})")

    async def stop(self):
        if self._current_task and not self._current_task.done():
            self._cancel_event.set()
            try:
                await asyncio.wait_for(self._current_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            self._current_task = None

    def _get_node_url(self, node_id: str) -> Optional[str]:
        node_rec = health_monitor.get_node(node_id)
        if node_rec and node_rec.get("url"):
            return node_rec["url"]
        for n in settings.STORAGE_NODES:
            if n["id"] == node_id:
                return n["url"]
        return None

    def generate_plan(
        self,
        active_node_ids: Optional[List[str]] = None,
        target_nodes_override: Optional[List[Dict[str, Any]]] = None,
        exclude_node_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Calculates a real dry-run migration plan based on Rendezvous Hashing (HRW).
        Zero physical mutations are performed during plan generation.
        """
        plan_id = f"plan-{uuid.uuid4().hex[:8]}"
        now = datetime.now(timezone.utc).isoformat()
        exclude_set = set(exclude_node_ids or [])

        # 1. Determine eligible nodes for HRW placement
        if target_nodes_override is not None:
            eligible_nodes = [
                n for n in target_nodes_override
                if (n.get("id") or n.get("node_id")) not in exclude_set
            ]
        else:
            all_nodes = health_monitor.get_all_nodes()
            eligible_nodes = []
            for n in all_nodes:
                nid = n.get("node_id") or n.get("id")
                if nid in exclude_set:
                    continue
                status = str(n.get("status", "")).upper()
                if active_node_ids is not None:
                    if nid in active_node_ids and status != "DECOMMISSIONED":
                        eligible_nodes.append({
                            "id": nid,
                            "node_id": nid,
                            "host": n["host"],
                            "port": n["port"],
                            "url": n["url"],
                        })
                else:
                    # Healthy, non-decommissioned nodes
                    if status in ("HEALTHY", "RECOVERING") and status != "DECOMMISSIONED":
                        eligible_nodes.append({
                            "id": nid,
                            "node_id": nid,
                            "host": n["host"],
                            "port": n["port"],
                            "url": n["url"],
                        })

        node_states = {
            (n.get("node_id") or n.get("id")): str(n.get("status", "")).upper()
            for n in health_monitor.get_all_nodes()
        }

        all_objects = db.list_objects()
        migrations: List[Dict[str, Any]] = []
        affected_objects = 0
        bytes_total = 0
        source_dist: Dict[str, int] = {}
        target_dist: Dict[str, int] = {}

        rf = settings.REPLICATION_FACTOR

        for obj in all_objects:
            oid = obj["object_id"]
            obj_name = obj["object_name"]
            size_bytes = obj["size_bytes"]
            sha256_hash = obj["sha256"]
            all_reps = obj.get("replicas", [])

            # Current stored replicas on healthy/reachable nodes
            current_healthy_reps = [
                r for r in all_reps
                if r["status"] == "STORED" and node_states.get(r["node_id"]) not in ("DOWN", "DECOMMISSIONED")
            ]
            current_nids = {r["node_id"] for r in current_healthy_reps}

            # If node is being excluded (e.g. decommission), include its replicas in current for evacuation
            for r in all_reps:
                if r["status"] == "STORED" and r["node_id"] in exclude_set:
                    current_nids.add(r["node_id"])

            if not eligible_nodes:
                continue

            # Calculate Desired placement using existing verified HRW algorithm
            desired_nodes = select_replica_nodes(oid, eligible_nodes, rf)
            desired_nids = {n.get("id") or n.get("node_id") for n in desired_nodes}

            # Calculate placement diff
            needed_nids = sorted(list(desired_nids - current_nids))
            surplus_nids = sorted(list(current_nids - desired_nids))

            if not needed_nids and not surplus_nids:
                # Placement already optimal — zero churn!
                continue

            affected_objects += 1

            # Candidate healthy source nodes: nodes that have the replica STORED and are not being decommissioned
            valid_sources = [
                r["node_id"] for r in all_reps
                if r["status"] == "STORED" and r["node_id"] not in exclude_set and node_states.get(r["node_id"]) == "HEALTHY"
            ]
            # Fallback source if no other healthy node available
            if not valid_sources:
                valid_sources = [
                    r["node_id"] for r in all_reps
                    if r["status"] == "STORED"
                ]

            if not valid_sources:
                continue

            # Select primary source (stable selection using HRW score)
            valid_sources.sort(key=lambda n: compute_hrw_score(oid, n), reverse=True)
            source_nid = valid_sources[0]

            # Pair needed target nodes with surplus displaced nodes
            num_steps = max(len(needed_nids), len(surplus_nids))
            for i in range(num_steps):
                target_nid = needed_nids[i] if i < len(needed_nids) else None
                displaced_nid = surplus_nids[i] if i < len(surplus_nids) else None

                reason = "NODE_DECOMMISSIONED" if (displaced_nid and displaced_nid in exclude_set) else (
                    "NODE_ADDED" if target_nid else "PLACEMENT_CHANGED"
                )

                if target_nid:
                    step = {
                        "object_id": oid,
                        "object_name": obj_name,
                        "source_node_id": source_nid,
                        "target_node_id": target_nid,
                        "displaced_node_id": displaced_nid,
                        "size_bytes": size_bytes,
                        "sha256": sha256_hash,
                        "reason": reason,
                    }
                    migrations.append(step)
                    bytes_total += size_bytes
                    source_dist[source_nid] = source_dist.get(source_nid, 0) + 1
                    target_dist[target_nid] = target_dist.get(target_nid, 0) + 1
                elif displaced_nid:
                    # Surplus replica without new target (e.g. RF was temporarily > 3)
                    step = {
                        "object_id": oid,
                        "object_name": obj_name,
                        "source_node_id": source_nid,
                        "target_node_id": source_nid,  # no-op target
                        "displaced_node_id": displaced_nid,
                        "size_bytes": size_bytes,
                        "sha256": sha256_hash,
                        "reason": "PRUNE_SURPLUS",
                    }
                    migrations.append(step)

        plan = {
            "plan_id": plan_id,
            "timestamp": now,
            "total_objects": len(all_objects),
            "affected_objects": affected_objects,
            "total_migrations": len(migrations),
            "bytes_total": bytes_total,
            "migrations": migrations,
            "source_distribution": source_dist,
            "target_distribution": target_dist,
            "active_nodes": [n.get("id") or n.get("node_id") for n in eligible_nodes],
        }

        asyncio.create_task(manager.broadcast("REBALANCE_PLAN_CREATED", {
            "plan_id": plan_id,
            "affected_objects": affected_objects,
            "total_migrations": len(migrations),
            "bytes_total": bytes_total,
            "timestamp": now,
        }))
        return plan

    async def start_rebalance(self, plan: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Initiates dynamic rebalancing based on the provided or freshly generated plan.
        """
        async with self._lock:
            # Check if a rebalance is already running
            active_run = db.get_active_rebalance_run()
            if active_run:
                return {
                    "status": "already_running",
                    "run_id": active_run["run_id"],
                    "message": "A rebalance migration run is already active",
                    **self.get_status(),
                }

            if plan is None:
                plan = self.generate_plan()

            run_id = f"rebal-{uuid.uuid4().hex[:8]}"
            now = datetime.now(timezone.utc).isoformat()

            import json
            run_data = {
                "run_id": run_id,
                "status": "RUNNING",
                "started_at": now,
                "total_objects": plan.get("total_objects", 0),
                "total_migrations": plan.get("total_migrations", 0),
                "completed_migrations": 0,
                "failed_migrations": 0,
                "bytes_total": plan.get("bytes_total", 0),
                "bytes_transferred": 0,
                "error_message": None,
                "plan_json": json.dumps(plan),
            }
            db.create_rebalance_run(run_data)
            self._active_run_id = run_id
            self._cancel_event.clear()

            await manager.broadcast("REBALANCE_STARTED", {
                "run_id": run_id,
                "total_migrations": plan.get("total_migrations", 0),
                "bytes_total": plan.get("bytes_total", 0),
                "timestamp": now,
            })

            # If no migrations required (Current == Desired), complete immediately!
            if plan.get("total_migrations", 0) == 0:
                completed_at = datetime.now(timezone.utc).isoformat()
                db.update_rebalance_run(run_id, {
                    "status": "COMPLETED",
                    "completed_at": completed_at,
                })
                self._active_run_id = None
                await manager.broadcast("REBALANCE_COMPLETED", {
                    "run_id": run_id,
                    "total_migrations": 0,
                    "completed_migrations": 0,
                    "failed_migrations": 0,
                    "bytes_transferred": 0,
                    "timestamp": completed_at,
                })
                return {
                    "status": "completed",
                    "run_id": run_id,
                    "message": "Cluster placement already optimal. Zero migrations required.",
                    "total_migrations": 0,
                }

            # Spawn background execution task
            self._current_task = asyncio.create_task(self._execute_rebalance_loop(run_id, plan))
            return {
                "status": "started",
                "run_id": run_id,
                "total_migrations": plan.get("total_migrations", 0),
                "bytes_total": plan.get("bytes_total", 0),
            }

    async def _execute_rebalance_loop(self, run_id: str, plan: Dict[str, Any]):
        migrations = plan.get("migrations", [])
        total = len(migrations)
        completed = 0
        failed = 0
        bytes_transferred = 0

        async def run_single_step(step: Dict[str, Any]):
            nonlocal completed, failed, bytes_transferred
            if self._cancel_event.is_set():
                return False

            oid = step["object_id"]
            target_nid = step["target_node_id"]
            source_nid = step["source_node_id"]

            async with self._semaphore:
                if self._cancel_event.is_set():
                    return False

                now = datetime.now(timezone.utc).isoformat()
                self._active_migrations.append(step)
                await manager.broadcast("REBALANCE_MIGRATION_STARTED", {
                    "run_id": run_id,
                    "object_id": oid,
                    "source_node_id": source_nid,
                    "target_node_id": target_nid,
                    "timestamp": now,
                })

                success = False
                err_msg = None
                try:
                    success = await self._migrate_single_replica(step)
                except Exception as exc:
                    err_msg = str(exc)

                if step in self._active_migrations:
                    self._active_migrations.remove(step)

                now = datetime.now(timezone.utc).isoformat()
                if success:
                    completed += 1
                    bytes_transferred += step["size_bytes"]
                    await manager.broadcast("REBALANCE_MIGRATION_COMPLETED", {
                        "run_id": run_id,
                        "object_id": oid,
                        "source_node_id": source_nid,
                        "target_node_id": target_nid,
                        "bytes_transferred": step["size_bytes"],
                        "timestamp": now,
                    })
                else:
                    failed += 1
                    await manager.broadcast("REBALANCE_MIGRATION_FAILED", {
                        "run_id": run_id,
                        "object_id": oid,
                        "source_node_id": source_nid,
                        "target_node_id": target_nid,
                        "error": err_msg or "Migration verification failed",
                        "timestamp": now,
                    })

                # Broadcast progress update
                progress = round((completed + failed) / total * 100, 1) if total > 0 else 100.0
                await manager.broadcast("REBALANCE_PROGRESS", {
                    "run_id": run_id,
                    "completed": completed,
                    "failed": failed,
                    "total": total,
                    "bytes_transferred": bytes_transferred,
                    "progress_percent": progress,
                    "timestamp": now,
                })
                # Update SQLite
                db.update_rebalance_run(run_id, {
                    "completed_migrations": completed,
                    "failed_migrations": failed,
                    "bytes_transferred": bytes_transferred,
                })
                return success

        # Execute all migration steps concurrently bounded by semaphore
        tasks = [asyncio.create_task(run_single_step(s)) for s in migrations]
        await asyncio.gather(*tasks, return_exceptions=True)

        now = datetime.now(timezone.utc).isoformat()
        if self._cancel_event.is_set():
            db.update_rebalance_run(run_id, {
                "status": "CANCELLED",
                "completed_at": now,
            })
            await manager.broadcast("REBALANCE_CANCELLED", {
                "run_id": run_id,
                "completed": completed,
                "failed": failed,
                "total": total,
                "timestamp": now,
            })
            print(f"[RebalanceManager] Rebalance run {run_id} CANCELLED ({completed}/{total} completed)")
        else:
            final_status = "COMPLETED" if failed == 0 else ("FAILED" if completed == 0 else "COMPLETED")
            db.update_rebalance_run(run_id, {
                "status": final_status,
                "completed_at": now,
            })
            await manager.broadcast("REBALANCE_COMPLETED", {
                "run_id": run_id,
                "status": final_status,
                "completed_migrations": completed,
                "failed_migrations": failed,
                "total_migrations": total,
                "bytes_transferred": bytes_transferred,
                "timestamp": now,
            })
            print(f"[RebalanceManager] Rebalance run {run_id} finished ({final_status}): {completed}/{total} completed, {failed} failed")

        self._active_run_id = None
        self._current_task = None

    async def _migrate_single_replica(self, step: Dict[str, Any]) -> bool:
        """
        Mandatory Copy-Before-Delete Invariant:
        1. Select healthy source replica.
        2. GET physical bytes from source.
        3. Verify source SHA-256 against authoritative metadata.
        4. PUT bytes to target storage node.
        5. Verify target response.
        6. Verify target SHA-256 and size.
        7. Mark target replica as STORED in SQLite.
        8. Confirm RF safety.
        9. ONLY THEN delete displaced replica from old storage node.
        10. Remove displaced replica from SQLite.
        If ANY step fails, source is NEVER deleted and target is NOT marked STORED.
        """
        oid = step["object_id"]
        source_nid = step["source_node_id"]
        target_nid = step["target_node_id"]
        displaced_nid = step.get("displaced_node_id")
        expected_sha = step["sha256"]
        expected_size = step["size_bytes"]
        is_prune_only = (step.get("reason") == "PRUNE_SURPLUS")

        source_url = self._get_node_url(source_nid)
        target_url = self._get_node_url(target_nid)

        if not is_prune_only and (not source_url or not target_url):
            print(f"[RebalanceManager] Missing URL for source ({source_nid}) or target ({target_nid})")
            return False

        async with httpx.AsyncClient(timeout=30.0) as client:
            if not is_prune_only:
                # 1. Idempotency Check: does target already store a valid verified replica?
                existing_target_rep = db.get_replica(oid, target_nid)
                target_already_valid = False
                if existing_target_rep and existing_target_rep.get("status") == "STORED" and existing_target_rep.get("sha256") == expected_sha:
                    try:
                        chk_res = await client.get(f"{target_url}/checksum/{oid}", timeout=3.0)
                        if chk_res.status_code == 200 and chk_res.json().get("sha256") == expected_sha:
                            target_already_valid = True
                    except Exception:
                        pass

                if not target_already_valid:
                    # 2. Retrieve physical bytes from source node
                    get_res = await client.get(f"{source_url}/retrieve/{oid}")
                    if get_res.status_code != 200:
                        print(f"[RebalanceManager] Failed retrieving {oid} from source {source_nid}: HTTP {get_res.status_code}")
                        return False

                    payload = get_res.content

                    # 3. Cryptographic SHA-256 verification of source bytes
                    source_sha = hashlib.sha256(payload).hexdigest()
                    if source_sha != expected_sha or len(payload) != expected_size:
                        print(f"[RebalanceManager] Checksum mismatch on source {source_nid}: got {source_sha}, expected {expected_sha}")
                        return False

                    # 4. PUT physical bytes to target node
                    put_res = await client.put(f"{target_url}/store/{oid}", content=payload)
                    if put_res.status_code != 200:
                        print(f"[RebalanceManager] Target {target_nid} store failed: HTTP {put_res.status_code}")
                        return False

                    target_data = put_res.json()
                    target_sha = target_data.get("sha256")
                    target_size = target_data.get("size_bytes")

                    # 5. Cryptographic and size verification of target replica
                    if target_sha != expected_sha or target_size != expected_size:
                        print(f"[RebalanceManager] Verification failed on target {target_nid}: sha={target_sha}, size={target_size}")
                        # Clean up failed write on target
                        try:
                            await client.delete(f"{target_url}/delete/{oid}", timeout=2.0)
                        except Exception:
                            pass
                        return False

                    # 6. Commit target replica as STORED in SQLite
                    db.insert_replica({
                        "object_id": oid,
                        "node_id": target_nid,
                        "status": "STORED",
                        "size_bytes": target_size,
                        "sha256": target_sha,
                    })
                    print(f"[RebalanceManager] Successfully stored & verified replica {oid} on {target_nid}")

            # 7. ONLY AFTER TARGET VERIFICATION: Safely prune displaced replica
            if displaced_nid and displaced_nid != target_nid:
                all_stored = [
                    r for r in db.get_replicas_for_object(oid)
                    if r["status"] == "STORED" and r["node_id"] != displaced_nid
                ]
                # Ensure object retains at least RF (3) stored replicas
                if len(all_stored) >= settings.REPLICATION_FACTOR or is_prune_only:
                    displaced_url = self._get_node_url(displaced_nid)
                    if displaced_url:
                        try:
                            del_res = await client.delete(f"{displaced_url}/delete/{oid}", timeout=4.0)
                            if del_res.status_code in (200, 404):
                                print(f"[RebalanceManager] Safely pruned physical replica {oid} from displaced {displaced_nid}")
                        except Exception as e:
                            print(f"[RebalanceManager] Warning deleting displaced replica from {displaced_nid}: {e}")
                    # Remove metadata from SQLite
                    db.delete_replica(oid, displaced_nid)
                    print(f"[RebalanceManager] Removed replica metadata for {oid} on {displaced_nid}")

        return True

    def get_status(self) -> Dict[str, Any]:
        """Returns live execution status, metrics, and progress."""
        active_run = db.get_active_rebalance_run()
        if not active_run:
            recent_runs = db.list_rebalance_runs(limit=1)
            if recent_runs:
                last_run = recent_runs[0]
                total = last_run["total_migrations"]
                completed = last_run["completed_migrations"]
                failed = last_run["failed_migrations"]
                progress = round((completed + failed) / total * 100, 1) if total > 0 else 100.0
                return {
                    "run_id": last_run["run_id"],
                    "status": last_run["status"],
                    "started_at": last_run["started_at"],
                    "completed_at": last_run["completed_at"],
                    "total_objects": last_run["total_objects"],
                    "total_migrations": total,
                    "completed_migrations": completed,
                    "failed_migrations": failed,
                    "bytes_total": last_run["bytes_total"],
                    "bytes_transferred": last_run["bytes_transferred"],
                    "progress_percent": progress,
                    "error_message": last_run.get("error_message"),
                    "active_migrations": [],
                }
            return {
                "status": "IDLE",
                "progress_percent": 0.0,
                "total_migrations": 0,
                "completed_migrations": 0,
                "failed_migrations": 0,
                "bytes_transferred": 0,
                "active_migrations": [],
            }

        total = active_run["total_migrations"]
        completed = active_run["completed_migrations"]
        failed = active_run["failed_migrations"]
        progress = round((completed + failed) / total * 100, 1) if total > 0 else 0.0

        return {
            "run_id": active_run["run_id"],
            "status": active_run["status"],
            "started_at": active_run["started_at"],
            "completed_at": active_run["completed_at"],
            "total_objects": active_run["total_objects"],
            "total_migrations": total,
            "completed_migrations": completed,
            "failed_migrations": failed,
            "bytes_total": active_run["bytes_total"],
            "bytes_transferred": active_run["bytes_transferred"],
            "progress_percent": progress,
            "error_message": active_run.get("error_message"),
            "active_migrations": list(self._active_migrations),
        }

    async def cancel_rebalance(self) -> Dict[str, Any]:
        """Requests graceful cancellation of active rebalance migration."""
        if not self._active_run_id and not db.get_active_rebalance_run():
            return {"status": "not_running", "message": "No active rebalance run to cancel"}

        self._cancel_event.set()
        run_id = self._active_run_id or (db.get_active_rebalance_run() or {}).get("run_id")
        if run_id:
            now = datetime.now(timezone.utc).isoformat()
            db.update_rebalance_run(run_id, {
                "status": "CANCELLED",
                "completed_at": now,
            })
            await manager.broadcast("REBALANCE_CANCELLED", {
                "run_id": run_id,
                "timestamp": now,
            })
        return {"status": "cancelling", "run_id": run_id}

    async def register_node(self, node_cfg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Dynamically registers a new storage node into cluster topology.
        - Validates node configuration.
        - Enters health monitor active heartbeat loop.
        - Triggers initial probe.
        - Recalculates HRW placement availability.
        """
        return await health_monitor.register_node(node_cfg)

    async def decommission_node(self, node_id: str) -> Dict[str, Any]:
        """
        Safely decommissions a storage node from the cluster.
        1. Marks node DECOMMISSIONING to stop new uploads.
        2. Calculates migration plan evacuating replicas to healthy replacement nodes.
        3. Migrates and cryptographically verifies all replicas.
        4. Prunes old replicas from decommissioned node.
        5. Marks node DECOMMISSIONED and stops monitoring.
        """
        node_rec = health_monitor.get_node(node_id)
        if not node_rec:
            raise ValueError(f"Node {node_id} not found in cluster configuration")

        # 1. Transition node state to DECOMMISSIONING
        health_monitor.set_node_status(node_id, "DECOMMISSIONING")
        now = datetime.now(timezone.utc).isoformat()
        await manager.broadcast("REBALANCE_NODE_DECOMMISSIONING", {
            "node_id": node_id,
            "timestamp": now,
        })

        # 2. Generate migration plan excluding the decommissioned node
        plan = self.generate_plan(exclude_node_ids=[node_id])

        # 3. Execute evacuation migrations
        run_res = await self.start_rebalance(plan)

        # Wait for evacuation rebalance to complete if running
        if self._current_task:
            try:
                await self._current_task
            except Exception:
                pass

        # Check final state
        active_run = db.get_active_rebalance_run()
        last_runs = db.list_rebalance_runs(limit=1)
        last_run = last_runs[0] if last_runs else None

        if last_run and last_run.get("failed_migrations", 0) > 0:
            return {
                "status": "decommission_failed",
                "node_id": node_id,
                "message": "Replication evacuation had failures. Node retained in safe state.",
                "run": last_run,
            }

        # 4. Successfully evacuated — mark DECOMMISSIONED
        health_monitor.deregister_node(node_id)
        return {
            "status": "decommissioned",
            "node_id": node_id,
            "message": f"Node {node_id} successfully decommissioned and evacuated.",
            "run": last_run,
        }


# Singleton instance
rebalance_mgr = RebalanceManager()
