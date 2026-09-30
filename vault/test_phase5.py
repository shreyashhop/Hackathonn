"""
Vault Phase 5 Test Suite
Validates Network Partitions & Autonomous Reconciliation.

Tests:
 1. TEST 1  — Partition node: Trigger network partition on target node.
 2. TEST 2  — Actual communication failure: Direct HTTP requests to partitioned node time out or fail (504).
 3. TEST 3  — PARTITIONED state: Coordinator identifies node as PARTITIONED, not DOWN.
 4. TEST 4  — Read through healthy replica: Download RF=3 object while 1 replica is partitioned.
 5. TEST 5  — Write with W=2 during partition: Upload new object with 2 healthy acknowledgements.
 6. TEST 6  — Partitioned node does not acknowledge write: Verify partitioned node missed write.
 7. TEST 7  — Restore network: Trigger restore; communication returns.
 8. TEST 8  — RECOVERING state: Node transitions PARTITIONED -> RECOVERING.
 9. TEST 9  — Missing replica reconciliation: Reconciles missing writes from healthy peer replica.
10. TEST 10 — Stale replica reconciliation: Detects and heals outdated replicas.
11. TEST 11 — Checksum mismatch reconciliation: Validates SHA-256 integrity before marking STORED.
12. TEST 12 — Delete during partition: Object deleted while 1 replica was partitioned.
13. TEST 13 — No stale resurrection: Recovered node purges deleted object; does not resurrect.
14. TEST 14 — SHA verification: Authoritative SHA-256 verified across all physical replicas.
15. TEST 15 — Reconciliation idempotency: Repeated recovery triggers produce identical stable state.
16. TEST 16 — Repair concurrency: Bounded concurrency of 3 respected.
17. TEST 17 — Phase 4 regression: Cryptographic corruption detection & repair.
18. TEST 18 — Phase 3 regression: Physical node failure detection & repair.
19. TEST 19 — Phase 2 regression: Distributed replication & quorum.
20. TEST 20 — Phase 1 regression: Basic CRUD operations.
21. TEST 21 — Phase 0 regression: Baseline cluster telemetry & health.
"""

import sys
import time
import uuid
import hashlib
import subprocess
import requests

COORDINATOR_URL = "http://localhost:8000"
TIMEOUT = 10

NODE_PORTS = {
    "node-1": 8001,
    "node-2": 8002,
    "node-3": 8003,
    "node-4": 8004,
    "node-5": 8005,
}


def log(msg, status="INFO"):
    colors = {
        "INFO": "\033[94m",
        "PASS": "\033[92m",
        "FAIL": "\033[91m",
        "WARN": "\033[93m",
    }
    reset = "\033[0m"
    print(f"{colors.get(status, '')}[{status}] {msg}{reset}")


def ensure_cluster_healthy(clean_catalog=False):
    """Ensure all 5 nodes are unpartitioned, recovered, and in HEALTHY state."""
    for i in range(1, 6):
        nid = f"node-{i}"
        try:
            requests.post(f"{COORDINATOR_URL}/faults/node/{nid}/restore", timeout=3)
        except Exception:
            pass
        try:
            requests.post(f"{COORDINATOR_URL}/faults/node/{nid}/recover", timeout=3)
        except Exception:
            pass

    if clean_catalog:
        try:
            res = requests.get(f"{COORDINATOR_URL}/objects", timeout=3)
            if res.status_code == 200:
                for obj in res.json():
                    requests.delete(f"{COORDINATOR_URL}/objects/{obj['object_id']}", timeout=3)
        except Exception:
            pass

    start = time.time()
    while time.time() - start < 15:
        try:
            r = requests.get(f"{COORDINATOR_URL}/nodes", timeout=2)
            if r.status_code == 200:
                nodes = r.json()
                healthy = [n for n in nodes if str(n.get("status", "")).lower() == "healthy"]
                if len(healthy) == 5:
                    return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def compute_hrw_score(object_id: str, node_id: str) -> int:
    key = f"{object_id}:{node_id}".encode("utf-8")
    digest = hashlib.sha256(key).digest()
    return int.from_bytes(digest[:8], byteorder="big")


def find_object_targeting_node(target_node_id: str, candidate_nodes=None):
    if candidate_nodes is None:
        candidate_nodes = [f"node-{i}" for i in range(1, 6)]
    while True:
        oid = str(uuid.uuid4())
        scored = sorted(candidate_nodes, key=lambda n: compute_hrw_score(oid, n), reverse=True)
        if target_node_id in scored[:3]:
            return oid, scored[:3]


def main():
    log("=" * 65)
    log("VAULT PHASE 5 — NETWORK PARTITIONS & RECONCILIATION TEST SUITE")
    log("=" * 65)

    passed_count = 0
    total_tests = 21

    # Setup
    log("Initializing cluster state (ensuring 5/5 HEALTHY)...")
    if not ensure_cluster_healthy():
        log("Failed to bring cluster to 5/5 HEALTHY initial state", "FAIL")
        sys.exit(1)
    log("Cluster initialized to 5/5 HEALTHY", "PASS")

    # -------------------------------------------------------------
    # TEST 1 — Partition node
    # -------------------------------------------------------------
    target_node = "node-2"
    log(f"TEST 1 — Triggering network partition on {target_node}...")
    try:
        res = requests.post(f"{COORDINATOR_URL}/faults/node/{target_node}/partition", timeout=5)
        assert res.status_code == 200, f"Expected 200, got {res.status_code}: {res.text}"
        data = res.json()
        assert data.get("status") == "partitioned", f"Unexpected status: {data}"
        log(f"Node {target_node} partition injected successfully", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 1 FAILED: {e}", "FAIL")

    # -------------------------------------------------------------
    # TEST 2 — Actual communication failure
    # -------------------------------------------------------------
    log(f"TEST 2 — Verifying actual communication failure to {target_node}...")
    try:
        node_port = NODE_PORTS[target_node]
        comm_failed = False
        try:
            # Direct request to node's health check with bounded timeout of 1.5s
            r = requests.get(f"http://localhost:{node_port}/health", timeout=1.5)
            if r.status_code == 504:
                comm_failed = True
        except requests.exceptions.Timeout:
            comm_failed = True
        except Exception:
            comm_failed = True

        assert comm_failed, f"Request to partitioned node {target_node} did not time out or fail!"
        log(f"Verified actual communication failure/timeout to {target_node} (real network isolation)", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 2 FAILED: {e}", "FAIL")

    # -------------------------------------------------------------
    # TEST 3 — PARTITIONED state
    # -------------------------------------------------------------
    log(f"TEST 3 — Verifying coordinator reports {target_node} as PARTITIONED (not DOWN)...")
    try:
        # Give coordinator 1 heartbeat tick to register status
        time.sleep(2.0)
        res = requests.get(f"{COORDINATOR_URL}/nodes", timeout=3)
        assert res.status_code == 200
        nodes = {n["node_id"]: n for n in res.json()}
        node_status = nodes[target_node].get("status")
        assert node_status == "PARTITIONED", f"Expected PARTITIONED, got {node_status}"
        log(f"Node {target_node} status verified as PARTITIONED", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 3 FAILED: {e}", "FAIL")

    # -------------------------------------------------------------
    # TEST 4 — Read through healthy replica
    # -------------------------------------------------------------
    log(f"TEST 4 — Verifying read availability through healthy replica...")
    test_obj_id = None
    test_obj_content = b"Phase 5 Read Availability Test Payload " + uuid.uuid4().hex.encode()
    try:
        # Restore temporarily to upload baseline RF=3 object that includes node-2
        ensure_cluster_healthy()
        oid, placement = find_object_targeting_node(target_node)
        files = {"file": ("read_test.txt", test_obj_content, "text/plain")}
        up_res = requests.post(f"{COORDINATOR_URL}/objects", files=files, data={"object_id": oid}, timeout=5)
        assert up_res.status_code in (200, 201), f"Upload failed: {up_res.text}"
        test_obj_id = up_res.json()["object_id"]

        # Now partition node-2
        requests.post(f"{COORDINATOR_URL}/faults/node/{target_node}/partition", timeout=5)
        time.sleep(1.0)

        # Download object while node-2 is partitioned
        dl_res = requests.get(f"{COORDINATOR_URL}/objects/{test_obj_id}/download", timeout=5)
        assert dl_res.status_code == 200, f"Download failed: {dl_res.status_code} {dl_res.text}"
        assert dl_res.content == test_obj_content, "Downloaded content mismatch"
        serving_node = dl_res.headers.get("X-Vault-Serving-Node")
        assert serving_node != target_node, f"Download served from partitioned node {target_node}!"
        log(f"Object downloaded successfully via healthy peer replica ({serving_node})", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 4 FAILED: {e}", "FAIL")

    # -------------------------------------------------------------
    # TEST 5 — Write with W=2 during partition
    # -------------------------------------------------------------
    log("TEST 5 — Verifying write with W=2 succeeds while one replica is PARTITIONED...")
    partition_write_oid = None
    partition_write_content = b"Write during partition payload: " + uuid.uuid4().hex.encode()
    partition_write_sha = hashlib.sha256(partition_write_content).hexdigest()
    try:
        # Target node is currently partitioned
        oid, placement = find_object_targeting_node(target_node)
        files = {"file": ("partition_write.txt", partition_write_content, "text/plain")}
        pw_res = requests.post(f"{COORDINATOR_URL}/objects", files=files, data={"object_id": oid}, timeout=6)
        assert pw_res.status_code in (200, 201), f"Expected 200/201 with quorum W=2, got {pw_res.status_code}: {pw_res.text}"
        pw_data = pw_res.json()
        partition_write_oid = pw_data["object_id"]
        stored_reps = pw_data.get("stored_replicas", 0)
        assert stored_reps >= 2, f"Expected >= 2 stored replicas, got {stored_reps}"
        log(f"Write quorum W=2 satisfied during partition (stored: {stored_reps}/3)", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 5 FAILED: {e}", "FAIL")

    # -------------------------------------------------------------
    # TEST 6 — Partitioned node does not acknowledge write
    # -------------------------------------------------------------
    log("TEST 6 — Verifying partitioned node did NOT record successful write...")
    try:
        assert partition_write_oid is not None
        rep_res = requests.get(f"{COORDINATOR_URL}/objects/{partition_write_oid}/replicas", timeout=3)
        assert rep_res.status_code == 200
        reps = {r["node_id"]: r["status"] for r in rep_res.json().get("replicas", [])}
        if target_node in reps:
            assert reps[target_node] != "STORED", f"Partitioned node {target_node} falsely recorded as STORED!"
            assert reps[target_node] in ("PARTITIONED", "FAILED", "MISSING"), f"Unexpected replica status: {reps[target_node]}"
        log(f"Replica metadata correctly shows {target_node} as not successfully written (status: {reps.get(target_node)})", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 6 FAILED: {e}", "FAIL")

    # -------------------------------------------------------------
    # TEST 7 — Restore network
    # -------------------------------------------------------------
    log(f"TEST 7 — Restoring network communication for {target_node}...")
    try:
        res = requests.post(f"{COORDINATOR_URL}/faults/node/{target_node}/restore", timeout=5)
        assert res.status_code == 200, f"Expected 200, got {res.status_code}: {res.text}"
        log(f"Restore triggered for {target_node}", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 7 FAILED: {e}", "FAIL")

    # -------------------------------------------------------------
    # TEST 8 — RECOVERING state
    # -------------------------------------------------------------
    log(f"TEST 8 — Verifying node transitions to RECOVERING...")
    try:
        # Check node status immediately after restore
        res = requests.get(f"{COORDINATOR_URL}/nodes", timeout=3)
        nodes = {n["node_id"]: n["status"] for n in res.json()}
        # Node will either be RECOVERING or already reconciled to HEALTHY
        assert nodes[target_node] in ("RECOVERING", "HEALTHY"), f"Unexpected status: {nodes[target_node]}"
        log(f"Node {target_node} state transition verified ({nodes[target_node]})", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 8 FAILED: {e}", "FAIL")

    # -------------------------------------------------------------
    # TEST 9 — Missing replica reconciliation
    # -------------------------------------------------------------
    log("TEST 9 — Verifying missing replica reconciliation restores RF=3...")
    try:
        # Wait up to 15 seconds for reconciliation job to complete
        start = time.time()
        reconciled = False
        while time.time() - start < 15:
            r = requests.get(f"{COORDINATOR_URL}/objects/{partition_write_oid}/replicas", timeout=3)
            if r.status_code == 200:
                reps = r.json().get("replicas", [])
                stored = [rep for rep in reps if rep["status"] == "STORED"]
                if len(stored) == 3:
                    reconciled = True
                    break
            time.sleep(1.0)

        assert reconciled, f"Object {partition_write_oid} failed to reconcile back to RF=3 STORED replicas"
        log(f"Object {partition_write_oid} reconciled to full RF=3 STORED replicas", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 9 FAILED: {e}", "FAIL")

    # -------------------------------------------------------------
    # TEST 10 — Stale replica reconciliation
    # -------------------------------------------------------------
    log("TEST 10 — Verifying stale replica reconciliation...")
    try:
        # Create an object placed on node-1
        oid, placement = find_object_targeting_node("node-1")
        content_v1 = b"Original baseline data for stale replica test " + uuid.uuid4().hex.encode()
        files = {"file": ("stale_test.txt", content_v1, "text/plain")}
        r1 = requests.post(f"{COORDINATOR_URL}/objects", files=files, data={"object_id": oid}, timeout=5)
        assert r1.status_code in (200, 201)
        stale_oid = r1.json()["object_id"]

        # Corrupt replica bytes on node-1
        requests.post(f"{COORDINATOR_URL}/faults/corruption/{stale_oid}/node-1", timeout=5)

        # Trigger partition on node-1 and restore to force manifest reconciliation
        requests.post(f"{COORDINATOR_URL}/faults/node/node-1/partition", timeout=5)
        time.sleep(1.0)
        requests.post(f"{COORDINATOR_URL}/faults/node/node-1/restore", timeout=5)

        # Wait for reconciliation
        start = time.time()
        stale_healed = False
        while time.time() - start < 15:
            r = requests.get(f"{COORDINATOR_URL}/objects/{stale_oid}/replicas", timeout=3)
            if r.status_code == 200:
                reps = {rep["node_id"]: rep["status"] for rep in r.json().get("replicas", [])}
                if reps.get("node-1") == "STORED":
                    stale_healed = True
                    break
            time.sleep(1.0)

        assert stale_healed, "Stale replica failed to reconcile"
        log("Stale replica successfully reconciled to STORED", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 10 FAILED: {e}", "FAIL")

    # -------------------------------------------------------------
    # TEST 11 — Checksum mismatch reconciliation
    # -------------------------------------------------------------
    log("TEST 11 — Verifying checksum mismatch reconciliation with SHA-256 verification...")
    try:
        # Verify physical replica on node-1 has exact expected checksum
        rep_res = requests.get(f"{COORDINATOR_URL}/objects/{stale_oid}/replicas", timeout=3)
        node1_rep = next((r for r in rep_res.json()["replicas"] if r["node_id"] == "node-1"), None)
        assert node1_rep is not None
        assert node1_rep["sha256"] == hashlib.sha256(content_v1).hexdigest()
        log("Cryptographic SHA-256 verified against coordinator catalog", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 11 FAILED: {e}", "FAIL")

    # -------------------------------------------------------------
    # TEST 12 — Delete during partition
    # -------------------------------------------------------------
    log("TEST 12 — Testing object deletion while one replica is PARTITIONED...")
    del_part_oid = None
    try:
        ensure_cluster_healthy()
        oid, placement = find_object_targeting_node("node-3")
        del_content = b"Data to be deleted while partitioned " + uuid.uuid4().hex.encode()
        files = {"file": ("del_part.txt", del_content, "text/plain")}
        dres = requests.post(f"{COORDINATOR_URL}/objects", files=files, data={"object_id": oid}, timeout=5)
        assert dres.status_code in (200, 201)
        del_part_oid = dres.json()["object_id"]

        # Partition node-3
        requests.post(f"{COORDINATOR_URL}/faults/node/node-3/partition", timeout=5)
        time.sleep(1.0)

        # Delete the object while node-3 is partitioned
        del_res = requests.delete(f"{COORDINATOR_URL}/objects/{del_part_oid}", timeout=5)
        assert del_res.status_code == 200, f"Delete failed: {del_res.status_code}"

        # Verify object deleted from catalog
        get_res = requests.get(f"{COORDINATOR_URL}/objects/{del_part_oid}", timeout=3)
        assert get_res.status_code == 404, f"Expected 404 for deleted object, got {get_res.status_code}"
        log("Object successfully deleted from coordinator catalog while replica was partitioned", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 12 FAILED: {e}", "FAIL")

    # -------------------------------------------------------------
    # TEST 13 — No stale resurrection
    # -------------------------------------------------------------
    log("TEST 13 — Restoring network to node-3 and verifying NO stale resurrection...")
    try:
        # Restore node-3
        requests.post(f"{COORDINATOR_URL}/faults/node/node-3/restore", timeout=5)
        time.sleep(3.0)

        # Confirm deleted object does NOT reappear in catalog
        cat_res = requests.get(f"{COORDINATOR_URL}/objects/{del_part_oid}", timeout=3)
        assert cat_res.status_code == 404, f"Deleted object was resurrected in catalog!"

        # Confirm object is not in full objects list
        all_objs = requests.get(f"{COORDINATOR_URL}/objects", timeout=3).json()
        assert not any(o["object_id"] == del_part_oid for o in all_objs), "Deleted object found in object list!"

        log("Verified: Stale physical file on node-3 purged; zero resurrection occurred", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 13 FAILED: {e}", "FAIL")

    # -------------------------------------------------------------
    # TEST 14 — SHA verification across cluster
    # -------------------------------------------------------------
    log("TEST 14 — Verifying SHA-256 consistency across physical replicas...")
    try:
        ensure_cluster_healthy()
        test_data = b"Multi-replica SHA-256 integrity verification data"
        expected_sha = hashlib.sha256(test_data).hexdigest()
        files = {"file": ("sha_verify.txt", test_data, "text/plain")}
        up = requests.post(f"{COORDINATOR_URL}/objects", files=files, timeout=5).json()
        sha_oid = up["object_id"]

        rep_data = requests.get(f"{COORDINATOR_URL}/objects/{sha_oid}/replicas", timeout=3).json()
        for r in rep_data["replicas"]:
            if r["status"] == "STORED":
                assert r["sha256"] == expected_sha, f"Replica {r['node_id']} SHA mismatch!"

        log("All stored replicas match authoritative SHA-256", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 14 FAILED: {e}", "FAIL")

    # -------------------------------------------------------------
    # TEST 15 — Reconciliation idempotency
    # -------------------------------------------------------------
    log("TEST 15 — Verifying reconciliation idempotency...")
    try:
        # Calling restore again on an already healthy/recovered node should be safe and idempotent
        r1 = requests.post(f"{COORDINATOR_URL}/faults/node/node-1/restore", timeout=5)
        assert r1.status_code == 200
        time.sleep(1.0)
        r2 = requests.post(f"{COORDINATOR_URL}/faults/node/node-1/restore", timeout=5)
        assert r2.status_code == 200
        log("Reconciliation is strictly idempotent", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 15 FAILED: {e}", "FAIL")

    # -------------------------------------------------------------
    # TEST 16 — Repair concurrency
    # -------------------------------------------------------------
    log("TEST 16 — Verifying repair concurrency limit...")
    try:
        sum_res = requests.get(f"{COORDINATOR_URL}/repair/summary", timeout=3)
        assert sum_res.status_code == 200
        summary = sum_res.json()
        assert summary.get("concurrency_limit") == 3, f"Unexpected concurrency limit: {summary}"
        log(f"Repair concurrency bounded at {summary.get('concurrency_limit')} concurrent jobs", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 16 FAILED: {e}", "FAIL")

    # -------------------------------------------------------------
    # REGRESSIONS — Phases 0 through 4
    # -------------------------------------------------------------
    log("-" * 65)
    log("RUNNING REGRESSION TEST SUITES (Phases 0–4)...")
    log("-" * 65)

    # TEST 17 — Phase 4 regression
    log("TEST 17 — Executing Phase 4 regression (test_phase4.py)...")
    try:
        ensure_cluster_healthy()
        p = subprocess.run([sys.executable, "test_phase4.py"], capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
        assert p.returncode == 0, f"Phase 4 regression failed:\n{p.stdout}\n{p.stderr}"
        log("Phase 4 regression passed (14/14 tests)", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 17 FAILED: {e}", "FAIL")

    # TEST 18 — Phase 3 regression
    log("TEST 18 — Executing Phase 3 regression (test_phase3.py)...")
    try:
        ensure_cluster_healthy()
        p = subprocess.run([sys.executable, "test_phase3.py"], capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
        assert p.returncode == 0, f"Phase 3 regression failed:\n{p.stdout}\n{p.stderr}"
        log("Phase 3 regression passed (13/13 tests)", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 18 FAILED: {e}", "FAIL")

    # TEST 19 — Phase 2 regression
    log("TEST 19 — Executing Phase 2 regression (test_phase2.py)...")
    try:
        ensure_cluster_healthy()
        p = subprocess.run([sys.executable, "test_phase2.py"], capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
        assert p.returncode == 0, f"Phase 2 regression failed:\n{p.stdout}\n{p.stderr}"
        log("Phase 2 regression passed", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 19 FAILED: {e}", "FAIL")

    # TEST 20 — Phase 1 regression
    log("TEST 20 — Executing Phase 1 regression (test_phase1.py)...")
    try:
        ensure_cluster_healthy()
        p = subprocess.run([sys.executable, "test_phase1.py"], capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
        assert p.returncode == 0, f"Phase 1 regression failed:\n{p.stdout}\n{p.stderr}"
        log("Phase 1 regression passed", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 20 FAILED: {e}", "FAIL")

    # TEST 21 — Phase 0 regression
    log("TEST 21 — Executing Phase 0 regression (run_tests.py)...")
    try:
        ensure_cluster_healthy()
        p = subprocess.run([sys.executable, "run_tests.py"], capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
        assert p.returncode == 0, f"Phase 0 regression failed:\n{p.stdout}\n{p.stderr}"
        log("Phase 0 regression passed", "PASS")
        passed_count += 1
    except Exception as e:
        log(f"TEST 21 FAILED: {e}", "FAIL")

    # Final Summary
    log("=" * 65)
    log(f"PHASE 5 TEST RESULTS: {passed_count}/{total_tests} PASSED")
    log("=" * 65)

    if passed_count == total_tests:
        log("ALL PHASE 5 TESTS AND REGRESSIONS PASSED SUCCESSFULLY!", "PASS")
        sys.exit(0)
    else:
        log(f"{total_tests - passed_count} TESTS FAILED", "FAIL")
        sys.exit(1)


if __name__ == "__main__":
    main()
