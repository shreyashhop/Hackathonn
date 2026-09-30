import os
import sqlite3
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone


class MetadataDatabase:
    """
    Metadata database manager using SQLite with WAL mode.
    Phase 0: Initializes database schema and node table.
    Phase 1: Real object metadata catalog with SQLite WAL.
    Phase 2: Real distributed replica tracking with object_replicas table.
    """

    def __init__(self, db_path: str = "./data/coordinator/metadata.db"):
        self.db_path = os.path.abspath(db_path)
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.init_db()

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        # Enforce WAL mode, foreign keys, and busy timeout
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def init_db(self):
        """Creates or migrates metadata schema."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # Nodes registry
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS storage_nodes (
                    id TEXT PRIMARY KEY,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    url TEXT NOT NULL,
                    status TEXT DEFAULT 'unknown',
                    last_seen TIMESTAMP
                )
            """)

            # Objects catalog schema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS objects (
                    object_id TEXT PRIMARY KEY,
                    object_name TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    content_type TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    storage_node_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'stored',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            # Phase 2 Object replicas table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS object_replicas (
                    object_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (object_id, node_id),
                    FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE
                )
            """)
            # Phase 3 Repair jobs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS repair_jobs (
                    job_id TEXT PRIMARY KEY,
                    object_id TEXT NOT NULL,
                    source_node_id TEXT NOT NULL,
                    target_node_id TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED')),
                    bytes_transferred INTEGER DEFAULT 0,
                    source_sha256 TEXT,
                    target_sha256 TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_repair_jobs_object ON repair_jobs(object_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_repair_jobs_status ON repair_jobs(status)")

            # Phase 6 Rebalance runs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS rebalance_runs (
                    run_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL CHECK(status IN ('PLANNED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')),
                    started_at TEXT,
                    completed_at TEXT,
                    total_objects INTEGER DEFAULT 0,
                    total_migrations INTEGER DEFAULT 0,
                    completed_migrations INTEGER DEFAULT 0,
                    failed_migrations INTEGER DEFAULT 0,
                    bytes_total INTEGER DEFAULT 0,
                    bytes_transferred INTEGER DEFAULT 0,
                    error_message TEXT,
                    plan_json TEXT,
                    created_at TEXT NOT NULL
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_rebalance_runs_status ON rebalance_runs(status)")

            conn.commit()

    # ============================================================
    # Objects Operations
    # ============================================================

    def insert_object(self, obj: Dict[str, Any]) -> Dict[str, Any]:
        """Inserts an object metadata entry into the catalog."""
        now = datetime.now(timezone.utc).isoformat()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO objects (
                    object_id, object_name, size_bytes, content_type,
                    sha256, storage_node_id, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                obj["object_id"],
                obj["object_name"],
                obj["size_bytes"],
                obj.get("content_type", "application/octet-stream"),
                obj["sha256"],
                obj.get("storage_node_id", ""),
                obj.get("status", "stored"),
                obj.get("created_at", now),
                obj.get("updated_at", now),
            ))
            conn.commit()
        return self.get_object(obj["object_id"])

    def get_object(self, object_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves object metadata including all replicas."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM objects WHERE object_id = ?", (object_id,))
            row = cursor.fetchone()
            if not row:
                return None
            obj_dict = dict(row)
            reps = self.get_replicas_for_object(object_id)
            obj_dict["replicas"] = reps
            obj_dict["replication_factor"] = len(reps)
            obj_dict["stored_replicas"] = sum(1 for r in reps if r["status"] == "STORED")
            return obj_dict

    def list_objects(self) -> List[Dict[str, Any]]:
        """Lists all objects in the catalog with their replica metadata."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM objects ORDER BY created_at DESC")
            objects = [dict(row) for row in cursor.fetchall()]

            # Fetch all replicas in one query for optimal performance
            cursor.execute("SELECT * FROM object_replicas ORDER BY node_id ASC")
            all_replicas = [dict(r) for r in cursor.fetchall()]

            replicas_by_obj: Dict[str, List[Dict[str, Any]]] = {}
            for rep in all_replicas:
                replicas_by_obj.setdefault(rep["object_id"], []).append(rep)

            for obj in objects:
                obj_replicas = replicas_by_obj.get(obj["object_id"], [])
                obj["replicas"] = obj_replicas
                obj["stored_replicas_count"] = sum(1 for r in obj_replicas if r["status"] == "STORED")
                obj["total_replicas_count"] = len(obj_replicas)

            return objects

    def delete_object(self, object_id: str) -> bool:
        """Deletes object metadata and its replicas."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM object_replicas WHERE object_id = ?", (object_id,))
            cursor.execute("DELETE FROM objects WHERE object_id = ?", (object_id,))
            conn.commit()
            return cursor.rowcount > 0

    def update_object_status(self, object_id: str, status: str) -> Optional[Dict[str, Any]]:
        """Updates status and updated_at timestamp."""
        now = datetime.now(timezone.utc).isoformat()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE objects
                SET status = ?, updated_at = ?
                WHERE object_id = ?
            """, (status, now, object_id))
            conn.commit()
        return self.get_object(object_id)

    # ============================================================
    # Phase 2: Object Replicas Operations
    # ============================================================

    def insert_replica(self, replica: Dict[str, Any]) -> Dict[str, Any]:
        """Inserts or replaces an object replica record."""
        now = datetime.now(timezone.utc).isoformat()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO object_replicas (
                    object_id, node_id, status, size_bytes, sha256, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                replica["object_id"],
                replica["node_id"],
                replica.get("status", "PENDING"),
                replica.get("size_bytes", 0),
                replica.get("sha256", ""),
                replica.get("created_at", now),
                replica.get("updated_at", now),
            ))
            conn.commit()
        return replica

    def update_replica_status(
        self,
        object_id: str,
        node_id: str,
        status: str,
        size_bytes: Optional[int] = None,
        sha256: Optional[str] = None,
    ) -> bool:
        """Updates status, size, and checksum for a specific replica."""
        now = datetime.now(timezone.utc).isoformat()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if size_bytes is not None and sha256 is not None:
                cursor.execute("""
                    UPDATE object_replicas
                    SET status = ?, size_bytes = ?, sha256 = ?, updated_at = ?
                    WHERE object_id = ? AND node_id = ?
                """, (status, size_bytes, sha256, now, object_id, node_id))
            else:
                cursor.execute("""
                    UPDATE object_replicas
                    SET status = ?, updated_at = ?
                    WHERE object_id = ? AND node_id = ?
                """, (status, now, object_id, node_id))
            conn.commit()
            return cursor.rowcount > 0

    def get_replicas_for_object(self, object_id: str) -> List[Dict[str, Any]]:
        """Retrieves all replicas for a specific object."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM object_replicas WHERE object_id = ? ORDER BY node_id ASC",
                (object_id,),
            )
            return [dict(row) for row in cursor.fetchall()]

    def delete_replicas_for_object(self, object_id: str) -> int:
        """Deletes all replica records for an object."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM object_replicas WHERE object_id = ?", (object_id,))
            conn.commit()
            return cursor.rowcount

    def get_replication_summary(self, configured_rf: int = 3, configured_w: int = 2) -> Dict[str, Any]:
        """Calculates cluster-wide replication statistics and distribution."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM objects")
            total_logical = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM object_replicas")
            total_physical = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM object_replicas WHERE status = 'STORED'")
            healthy_replicas = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM object_replicas WHERE status = 'CORRUPT'")
            corrupt_replicas = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM object_replicas WHERE status = 'UNAVAILABLE'")
            unavailable_replicas = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM object_replicas WHERE status = 'FAILED'")
            failed_replicas = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM object_replicas WHERE status = 'PARTITIONED'")
            partitioned_replicas = cursor.fetchone()[0]

            # Replica distribution per storage node
            distribution = {f"node-{i}": 0 for i in range(1, 6)}
            try:
                cursor.execute("SELECT id FROM storage_nodes")
                for r in cursor.fetchall():
                    distribution.setdefault(r["id"], 0)
            except Exception:
                pass

            cursor.execute("""
                SELECT node_id, COUNT(*) as count
                FROM object_replicas
                WHERE status = 'STORED'
                GROUP BY node_id
            """)
            for row in cursor.fetchall():
                distribution[row["node_id"]] = row["count"]

            return {
                "replication_factor": configured_rf,
                "write_quorum": configured_w,
                "total_logical_objects": total_logical,
                "total_physical_replicas": total_physical,
                "healthy_replicas": healthy_replicas,
                "corrupt_replicas": corrupt_replicas,
                "unavailable_replicas": unavailable_replicas,
                "failed_replicas": failed_replicas,
                "partitioned_replicas": partitioned_replicas,
                "replica_distribution": distribution,
            }

    def get_replica(self, object_id: str, node_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a single replica record by object_id and node_id."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM object_replicas WHERE object_id = ? AND node_id = ?",
                (object_id, node_id),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def delete_replica(self, object_id: str, node_id: str) -> bool:
        """Deletes a specific replica record."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM object_replicas WHERE object_id = ? AND node_id = ?", (object_id, node_id))
            conn.commit()
            return cursor.rowcount > 0

    def get_replicas_for_node(self, node_id: str) -> List[Dict[str, Any]]:
        """Retrieves all replica records hosted on a given node."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM object_replicas WHERE node_id = ?", (node_id,))
            return [dict(row) for row in cursor.fetchall()]

    def update_replica_status_by_node(self, node_id: str, old_status: str, new_status: str) -> int:
        """Updates replica status for all replicas on a given node matching old_status."""
        now = datetime.now(timezone.utc).isoformat()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE object_replicas
                SET status = ?, updated_at = ?
                WHERE node_id = ? AND status = ?
            """, (new_status, now, node_id, old_status))
            conn.commit()
            return cursor.rowcount

    # ============================================================
    # Phase 3: Repair Jobs Operations
    # ============================================================

    def create_repair_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Creates a repair job record."""
        now = datetime.now(timezone.utc).isoformat()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO repair_jobs (
                    job_id, object_id, source_node_id, target_node_id,
                    reason, status, bytes_transferred, source_sha256,
                    target_sha256, error, created_at, started_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job["job_id"],
                job["object_id"],
                job["source_node_id"],
                job["target_node_id"],
                job.get("reason", "NODE_FAILURE"),
                job.get("status", "QUEUED"),
                job.get("bytes_transferred", 0),
                job.get("source_sha256"),
                job.get("target_sha256"),
                job.get("error"),
                job.get("created_at", now),
                job.get("started_at"),
                job.get("completed_at"),
            ))
            conn.commit()
        return self.get_repair_job(job["job_id"])

    # Whitelist of columns that may be updated on repair_jobs
    _REPAIR_JOB_UPDATABLE_COLUMNS = frozenset({
        "status", "bytes_transferred", "source_sha256", "target_sha256",
        "error", "started_at", "completed_at",
    })

    def update_repair_job(self, job_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Updates fields of an existing repair job. Only whitelisted columns are accepted."""
        if not updates:
            return self.get_repair_job(job_id)
        set_clauses = []
        params = []
        for k, v in updates.items():
            if k not in self._REPAIR_JOB_UPDATABLE_COLUMNS:
                raise ValueError(f"Column '{k}' is not an allowed update target for repair_jobs")
            set_clauses.append(f"{k} = ?")
            params.append(v)
        if not set_clauses:
            return self.get_repair_job(job_id)
        params.append(job_id)

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"UPDATE repair_jobs SET {', '.join(set_clauses)} WHERE job_id = ?", params)
            conn.commit()
        return self.get_repair_job(job_id)

    def get_repair_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a single repair job by ID."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM repair_jobs WHERE job_id = ?", (job_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def list_repair_jobs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Lists recent repair jobs."""
        # Clamp limit to a safe range
        safe_limit = max(1, min(int(limit), 500))
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM repair_jobs ORDER BY created_at DESC LIMIT ?", (safe_limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_all_replicas(self, status_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Retrieves all replica records, optionally filtered by status."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if status_filter:
                cursor.execute("SELECT * FROM object_replicas WHERE status = ? ORDER BY object_id, node_id", (status_filter,))
            else:
                cursor.execute("SELECT * FROM object_replicas ORDER BY object_id, node_id")
            return [dict(row) for row in cursor.fetchall()]

    def get_active_repair_for_object(self, object_id: str, target_node_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Returns currently active (QUEUED or RUNNING) repair job for an object and optional target node."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if target_node_id:
                cursor.execute(
                    "SELECT * FROM repair_jobs WHERE object_id = ? AND target_node_id = ? AND status IN ('QUEUED', 'RUNNING') LIMIT 1",
                    (object_id, target_node_id)
                )
            else:
                cursor.execute(
                    "SELECT * FROM repair_jobs WHERE object_id = ? AND status IN ('QUEUED', 'RUNNING') LIMIT 1",
                    (object_id,)
                )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_repair_summary(self) -> Dict[str, Any]:
        """Calculates repair jobs metrics."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT status, COUNT(*) as count FROM repair_jobs GROUP BY status")
            counts = {row["status"]: row["count"] for row in cursor.fetchall()}
            return {
                "queued": counts.get("QUEUED", 0),
                "running": counts.get("RUNNING", 0),
                "completed": counts.get("COMPLETED", 0),
                "failed": counts.get("FAILED", 0),
                "total": sum(counts.values()),
            }


    # ============================================================
    # Phase 6: Dynamic Node Registration & Rebalance Operations
    # ============================================================

    def register_storage_node(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """Registers or updates a storage node in metadata catalog."""
        now = datetime.now(timezone.utc).isoformat()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO storage_nodes (id, host, port, url, status, last_seen)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                node["id"],
                node["host"],
                node["port"],
                node["url"],
                node.get("status", "HEALTHY"),
                node.get("last_seen", now),
            ))
            conn.commit()
        return node

    def deregister_storage_node(self, node_id: str) -> bool:
        """Removes a storage node from catalog."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM storage_nodes WHERE id = ?", (node_id,))
            conn.commit()
            return cursor.rowcount > 0

    def list_storage_nodes(self) -> List[Dict[str, Any]]:
        """Lists registered storage nodes from catalog."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM storage_nodes ORDER BY id ASC")
            return [dict(row) for row in cursor.fetchall()]

    def create_rebalance_run(self, run_data: Dict[str, Any]) -> Dict[str, Any]:
        """Creates a rebalance run record."""
        now = datetime.now(timezone.utc).isoformat()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO rebalance_runs (
                    run_id, status, started_at, completed_at,
                    total_objects, total_migrations, completed_migrations,
                    failed_migrations, bytes_total, bytes_transferred,
                    error_message, plan_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_data["run_id"],
                run_data.get("status", "PLANNED"),
                run_data.get("started_at"),
                run_data.get("completed_at"),
                run_data.get("total_objects", 0),
                run_data.get("total_migrations", 0),
                run_data.get("completed_migrations", 0),
                run_data.get("failed_migrations", 0),
                run_data.get("bytes_total", 0),
                run_data.get("bytes_transferred", 0),
                run_data.get("error_message"),
                run_data.get("plan_json"),
                run_data.get("created_at", now),
            ))
            conn.commit()
        return self.get_rebalance_run(run_data["run_id"])

    _REBALANCE_RUN_UPDATABLE_COLUMNS = frozenset({
        "status", "started_at", "completed_at", "total_objects",
        "total_migrations", "completed_migrations", "failed_migrations",
        "bytes_total", "bytes_transferred", "error_message", "plan_json",
    })

    def update_rebalance_run(self, run_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Updates fields of an existing rebalance run. Only whitelisted columns are accepted."""
        if not updates:
            return self.get_rebalance_run(run_id)
        set_clauses = []
        params = []
        for k, v in updates.items():
            if k not in self._REBALANCE_RUN_UPDATABLE_COLUMNS:
                raise ValueError(f"Column '{k}' is not an allowed update target for rebalance_runs")
            set_clauses.append(f"{k} = ?")
            params.append(v)
        if not set_clauses:
            return self.get_rebalance_run(run_id)
        params.append(run_id)

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"UPDATE rebalance_runs SET {', '.join(set_clauses)} WHERE run_id = ?", params)
            conn.commit()
        return self.get_rebalance_run(run_id)

    def get_rebalance_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a single rebalance run by ID."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM rebalance_runs WHERE run_id = ?", (run_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_active_rebalance_run(self) -> Optional[Dict[str, Any]]:
        """Returns currently active (PLANNED or RUNNING) rebalance run."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM rebalance_runs WHERE status IN ('PLANNED', 'RUNNING') ORDER BY created_at DESC LIMIT 1"
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def list_rebalance_runs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Lists recent rebalance runs."""
        safe_limit = max(1, min(int(limit), 500))
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM rebalance_runs ORDER BY created_at DESC LIMIT ?", (safe_limit,))
            return [dict(row) for row in cursor.fetchall()]


# Singleton instance
db = MetadataDatabase()
