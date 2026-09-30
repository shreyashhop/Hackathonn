import os
import time
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import httpx

from .config import settings
from .events import manager
from .repair_manager import repair_mgr
from ..db.database import db

INTEGRITY_SCAN_INTERVAL_SEC = int(os.getenv("INTEGRITY_SCAN_INTERVAL_SEC", "30"))
MAX_INTEGRITY_CHECKS = 3


class IntegrityScanner:
    """
    Background & on-demand cryptographic data integrity verification engine.
    Audits physical object checksums across all storage nodes against the authoritative
    catalog SHA-256 digests. Detects silent data corruption / bit rot and triggers automatic repair.
    """

    def __init__(self):
        self._interval_sec = INTEGRITY_SCAN_INTERVAL_SEC
        self._semaphore = asyncio.Semaphore(MAX_INTEGRITY_CHECKS)
        self._scanner_task: Optional[asyncio.Task] = None
        self._is_scanning = False

        # Cumulative metrics
        self.stats = {
            "total_scans": 0,
            "last_scan_time": None,
            "scan_duration_ms": 0.0,
            "checks_performed": 0,
            "mismatches_found": 0,
            "repairs_triggered": 0,
        }

    def start(self):
        """Starts periodic background scanning loop."""
        if self._scanner_task is None or self._scanner_task.done():
            self._scanner_task = asyncio.create_task(self._scan_loop())
            print(f"[IntegrityScanner] Background scanner started (interval: {self._interval_sec}s)")

    async def stop(self):
        """Stops background scanning loop."""
        if self._scanner_task:
            self._scanner_task.cancel()
            try:
                await self._scanner_task
            except asyncio.CancelledError:
                pass
            self._scanner_task = None

    async def _scan_loop(self):
        # Initial delay before starting periodic scans
        await asyncio.sleep(5)
        while True:
            try:
                await self.run_scan()
                await asyncio.sleep(self._interval_sec)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[IntegrityScanner] Background scan exception: {e}")
                await asyncio.sleep(5)

    def _get_node_url(self, node_id: str) -> Optional[str]:
        for n in settings.STORAGE_NODES:
            if n["id"] == node_id:
                return n["url"]
        return None

    async def run_scan(self) -> Dict[str, Any]:
        """
        Executes a complete cluster integrity scan across all stored replicas.
        """
        if self._is_scanning:
            return {
                "status": "in_progress",
                "message": "Integrity scan is already running",
                **self.get_summary()
            }

        self._is_scanning = True
        start_time = time.time()
        start_iso = datetime.now(timezone.utc).isoformat()

        await manager.broadcast("INTEGRITY_SCAN_STARTED", {
            "timestamp": start_iso,
        })

        all_objects = {obj["object_id"]: obj for obj in db.list_objects()}
        all_replicas = db.get_all_replicas()

        scan_checks = 0
        scan_mismatches = 0
        scan_repairs = 0
        check_details = []

        async def check_replica(replica: Dict[str, Any], client: httpx.AsyncClient):
            nonlocal scan_checks, scan_mismatches, scan_repairs
            oid = replica["object_id"]
            nid = replica["node_id"]
            obj = all_objects.get(oid)
            if not obj:
                return

            expected_sha = obj["sha256"]
            expected_size = obj["size_bytes"]
            node_url = self._get_node_url(nid)
            if not node_url:
                return

            async with self._semaphore:
                scan_checks += 1
                await manager.broadcast("INTEGRITY_CHECK_STARTED", {
                    "object_id": oid,
                    "node_id": nid,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

                try:
                    resp = await client.get(f"{node_url}/checksum/{oid}", timeout=4.0)
                    if resp.status_code == 200:
                        data = resp.json()
                        actual_sha = data.get("sha256")
                        actual_size = data.get("size_bytes")

                        if actual_sha == expected_sha and actual_size == expected_size:
                            # Re-verify replica is marked STORED
                            if replica["status"] != "STORED":
                                all_reps = db.get_replicas_for_object(oid)
                                other_stored = [
                                    r for r in all_reps
                                    if r["node_id"] != nid and r["status"] == "STORED"
                                    and r.get("sha256") == expected_sha
                                ]
                                if len(other_stored) >= settings.REPLICATION_FACTOR:
                                    try:
                                        await client.delete(f"{node_url}/delete/{oid}")
                                    except Exception:
                                        pass
                                    db.delete_replica(oid, nid)
                                    return
                                else:
                                    db.update_replica_status(oid, nid, "STORED", actual_size, actual_sha)

                            await manager.broadcast("INTEGRITY_CHECK_PASSED", {
                                "object_id": oid,
                                "node_id": nid,
                                "sha256": actual_sha,
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                            })
                            check_details.append({
                                "object_id": oid,
                                "object_name": obj["object_name"],
                                "node_id": nid,
                                "status": "VALID",
                                "expected_sha": expected_sha,
                                "actual_sha": actual_sha,
                            })
                        else:
                            # Checksum or size mismatch detected!
                            scan_mismatches += 1
                            db.update_replica_status(oid, nid, "CORRUPT")

                            await manager.broadcast("REPLICA_CORRUPT", {
                                "object_id": oid,
                                "object_name": obj["object_name"],
                                "node_id": nid,
                                "expected_sha": expected_sha,
                                "actual_sha": actual_sha,
                                "detected_during": "integrity_scan",
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                            })
                            await manager.broadcast("INTEGRITY_CHECK_FAILED", {
                                "object_id": oid,
                                "node_id": nid,
                                "expected_sha": expected_sha,
                                "actual_sha": actual_sha,
                                "error": f"Checksum mismatch: expected {expected_sha}, got {actual_sha}",
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                            })

                            # Trigger automatic repair
                            await repair_mgr.trigger_corruption_repair(oid, nid)
                            scan_repairs += 1

                            check_details.append({
                                "object_id": oid,
                                "object_name": obj["object_name"],
                                "node_id": nid,
                                "status": "CORRUPT",
                                "expected_sha": expected_sha,
                                "actual_sha": actual_sha,
                            })

                    elif resp.status_code == 404:
                        # File missing entirely
                        scan_mismatches += 1
                        db.update_replica_status(oid, nid, "FAILED")
                        await manager.broadcast("INTEGRITY_CHECK_FAILED", {
                            "object_id": oid,
                            "node_id": nid,
                            "error": "Physical file missing on node",
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        })
                        check_details.append({
                            "object_id": oid,
                            "object_name": obj["object_name"],
                            "node_id": nid,
                            "status": "MISSING",
                            "expected_sha": expected_sha,
                            "actual_sha": None,
                        })

                except Exception as e:
                    # Node might be down or unreachable
                    check_details.append({
                        "object_id": oid,
                        "object_name": obj["object_name"],
                        "node_id": nid,
                        "status": "UNREACHABLE",
                        "error": str(e),
                    })

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                tasks = [check_replica(r, client) for r in all_replicas]
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            duration_ms = round((time.time() - start_time) * 1000, 2)
            end_iso = datetime.now(timezone.utc).isoformat()

            # Update cumulative stats
            self.stats["total_scans"] += 1
            self.stats["last_scan_time"] = end_iso
            self.stats["scan_duration_ms"] = duration_ms
            self.stats["checks_performed"] += scan_checks
            self.stats["mismatches_found"] += scan_mismatches
            self.stats["repairs_triggered"] += scan_repairs

            result_summary = {
                "scan_id": f"scan-{int(start_time)}",
                "status": "completed",
                "timestamp": end_iso,
                "duration_ms": duration_ms,
                "checks_performed": scan_checks,
                "mismatches_found": scan_mismatches,
                "repairs_triggered": scan_repairs,
                "details": check_details,
            }

            await manager.broadcast("INTEGRITY_SCAN_COMPLETED", {
                "timestamp": end_iso,
                "duration_ms": duration_ms,
                "checks_performed": scan_checks,
                "mismatches_found": scan_mismatches,
                "repairs_triggered": scan_repairs,
            })

            return result_summary
        finally:
            self._is_scanning = False

    def get_summary(self) -> Dict[str, Any]:
        """Returns current cumulative integrity status and replica counts."""
        rep_summary = db.get_replication_summary()
        return {
            "integrity_status": "CORRUPT_DETECTED" if rep_summary.get("corrupt_replicas", 0) > 0 else "HEALTHY",
            "healthy_replicas": rep_summary.get("healthy_replicas", 0),
            "corrupt_replicas": rep_summary.get("corrupt_replicas", 0),
            "unavailable_replicas": rep_summary.get("unavailable_replicas", 0),
            "total_physical_replicas": rep_summary.get("total_physical_replicas", 0),
            "last_scan": self.stats["last_scan_time"],
            "scan_duration_ms": self.stats["scan_duration_ms"],
            "checks_performed": self.stats["checks_performed"],
            "mismatches_found": self.stats["mismatches_found"],
            "repairs_triggered": self.stats["repairs_triggered"],
            "total_scans": self.stats["total_scans"],
            "is_scanning": self._is_scanning,
        }


# Singleton instance
integrity_scanner = IntegrityScanner()
