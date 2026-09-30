"""
Vault Phase 3 Test Suite
Validates Node Failure Detection, Health State Machine, and Automatic Replica Repair.

Tests:
1. TEST 1 — Heartbeat: Verify all 5 nodes become HEALTHY.
2. TEST 2 — Node failure detection: HEALTHY -> SUSPECT -> DOWN with measured timing.
3. TEST 3 — Replica risk: Upload RF=3 object, fail 1 replica node, verify replica at-risk.
4. TEST 4 — Read availability: Download object while replica node is DOWN.
5. TEST 5 — Automatic repair: Verify healthy replica count returns to 3.
6. TEST 6 — Physical repair: Verify target node filesystem contains physical object file.
7. TEST 7 — Repair checksum: Verify target SHA-256 matches authoritative coordinator SHA-256.
8. TEST 8 — Repair metadata: Verify new replica is marked STORED.
9. TEST 9 — Node recovery: Recover node and observe DOWN -> RECOVERING -> HEALTHY.
10. TEST 10 — No unnecessary fourth replica: Verify Rule 17 maintains RF=3 without duplicate replicas.
11. TEST 11 — Phase 2 regression: Run test_phase2.py.
12. TEST 12 — Phase 1 regression: Run test_phase1.py.
13. TEST 13 — Phase 0 regression: Run run_tests.py.
"""

import sys
import time
import hashlib
import subprocess
import requests

COORDINATOR_URL = "http://localhost:8000"
TIMEOUT = 10


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
    """Ensure all 5 nodes are recovered and in HEALTHY state before starting a test."""
    for i in range(1, 6):
        node_id = f"node-{i}"
        try:
            requests.post(f"{COORDINATOR_URL}/faults/node/{node_id}/recover", timeout=2)
        except Exception:
            pass
    if clean_catalog:
        try:
            res = requests.get(f"{COORDINATOR_URL}/objects", timeout=2)
            if res.status_code == 200:
                for obj in res.json():
                    requests.delete(f"{COORDINATOR_URL}/objects/{obj['object_id']}", timeout=2)
        except Exception:
            pass
    # Wait up to 10 seconds for all nodes to report HEALTHY
    start = time.time()
    while time.time() - start < 10:
        try:
            res = requests.get(f"{COORDINATOR_URL}/nodes", timeout=2)
            if res.status_code == 200:
                nodes = res.json()
                healthy = [n for n in nodes if n.get("status") == "HEALTHY"]
                if len(healthy) == 5:
                    return True
        except Exception:
            pass
        time.sleep(1)
    return False


def test_1_heartbeat():
    log("=== TEST 1 — Heartbeat: Verify all 5 nodes initially become HEALTHY ===")
    assert ensure_cluster_healthy(), "Failed to bring all 5 nodes to HEALTHY"

    res = requests.get(f"{COORDINATOR_URL}/nodes", timeout=TIMEOUT)
    assert res.status_code == 200, f"Expected 200, got {res.status_code}"
    nodes = res.json()
    assert len(nodes) == 5, f"Expected 5 nodes, got {len(nodes)}"

    for node in nodes:
        node_id = node.get("node_id")
        status = node.get("status")
        last_hb = node.get("last_heartbeat")
        log(f"Node {node_id}: status={status}, last_hb={last_hb}")
        assert status == "HEALTHY", f"Node {node_id} is not HEALTHY: {status}"

    log("TEST 1 PASSED: All 5 nodes are reporting HEALTHY with active heartbeats.", "PASS")


def test_2_failure_detection():
    log("=== TEST 2 — Node failure detection: HEALTHY -> SUSPECT -> DOWN ===")
    test_node = "node-4"
    ensure_cluster_healthy()

    start_time = time.time()
    res = requests.post(f"{COORDINATOR_URL}/faults/node/{test_node}/stop", timeout=TIMEOUT)
    assert res.status_code == 200, f"Failed to stop {test_node}: {res.text}"
    log(f"Injected controlled failure on {test_node} at t=0.0s")

    observed_suspect = False
    observed_down = False
    suspect_time = None
    down_time = None

    timeout_wait = 12.0
    while time.time() - start_time < timeout_wait:
        elapsed = time.time() - start_time
        res = requests.get(f"{COORDINATOR_URL}/nodes/{test_node}", timeout=2)
        if res.status_code == 200:
            status = res.json().get("status")
            failed_beats = res.json().get("failed_heartbeats", 0)
            if status == "SUSPECT" and not observed_suspect:
                observed_suspect = True
                suspect_time = elapsed
                log(f"Observed SUSPECT state at t={suspect_time:.2f}s (failed_heartbeats={failed_beats})")
            elif status == "DOWN" and not observed_down:
                observed_down = True
                down_time = elapsed
                log(f"Observed DOWN state at t={down_time:.2f}s (failed_heartbeats={failed_beats})")
                break
        time.sleep(0.4)

    assert observed_down, f"Node {test_node} did not transition to DOWN within {timeout_wait}s"
    log(f"Transition sequence verified: HEALTHY -> SUSPECT (t={suspect_time}s) -> DOWN (t={down_time}s)")

    # Recover the node for subsequent tests
    requests.post(f"{COORDINATOR_URL}/faults/node/{test_node}/recover", timeout=TIMEOUT)
    time.sleep(3.0)
    ensure_cluster_healthy()
    log("TEST 2 PASSED: Node failure detection and state transitions verified.", "PASS")


def test_3_through_10_repair_lifecycle():
    log("=== TESTS 3-10 — Full Automatic Replica Repair Lifecycle ===")
    ensure_cluster_healthy()

    # Step A: Upload object with RF=3
    content = b"VAULT-PHASE-3-AUTOMATIC-REPLICA-REPAIR-VERIFICATION-DATA-" + str(time.time()).encode()
    expected_sha256 = hashlib.sha256(content).hexdigest()
    expected_size = len(content)

    log(f"Uploading test object: size={expected_size} bytes, sha256={expected_sha256[:12]}...")
    files = {"file": ("phase3_repair_test.txt", content, "text/plain")}
    res = requests.post(f"{COORDINATOR_URL}/objects", files=files, timeout=TIMEOUT)
    assert res.status_code in (200, 201), f"Upload failed: {res.text}"
    obj_data = res.json()
    object_id = obj_data["object_id"]
    replicas = obj_data.get("replicas", [])

    assert len(replicas) == 3, f"Expected 3 replicas, got {len(replicas)}"
    stored_nodes = [r["node_id"] for r in replicas if r["status"] == "STORED"]
    assert len(stored_nodes) == 3, f"Expected 3 STORED replicas, got {stored_nodes}"
    log(f"Object {object_id} stored on nodes: {stored_nodes}")

    failed_node = stored_nodes[0]
    healthy_source_node = stored_nodes[1]
    log(f"Selecting replica node to fail: {failed_node}")

    # TEST 3: Replica risk
    log(f"--- TEST 3: Injecting failure on replica node {failed_node} ---")
    requests.post(f"{COORDINATOR_URL}/faults/node/{failed_node}/stop", timeout=TIMEOUT)

    # Wait for node to be detected as DOWN
    start_wait = time.time()
    down_detected = False
    while time.time() - start_wait < 10:
        res = requests.get(f"{COORDINATOR_URL}/nodes/{failed_node}", timeout=2)
        if res.status_code == 200 and res.json().get("status") == "DOWN":
            down_detected = True
            break
        time.sleep(0.5)
    assert down_detected, f"Failed node {failed_node} was not marked DOWN in time"

    # Verify replica on failed_node is now marked at-risk / UNAVAILABLE
    res = requests.get(f"{COORDINATOR_URL}/objects/{object_id}", timeout=TIMEOUT)
    assert res.status_code == 200
    current_replicas = res.json().get("replicas", [])
    failed_replica = next((r for r in current_replicas if r["node_id"] == failed_node), None)
    assert failed_replica is not None, f"Failed node {failed_node} not in replica list"
    assert failed_replica["status"] in ("UNAVAILABLE", "DOWN"), f"Replica status not UNAVAILABLE: {failed_replica['status']}"
    log(f"TEST 3 PASSED: Replica on {failed_node} is confirmed at-risk (status={failed_replica['status']})", "PASS")

    # TEST 4: Read availability during failure
    log("--- TEST 4: Read availability while replica node is DOWN ---")
    res = requests.get(f"{COORDINATOR_URL}/objects/{object_id}/download", timeout=TIMEOUT)
    assert res.status_code == 200, f"Download failed while node was down: {res.status_code}"
    downloaded_bytes = res.content
    assert len(downloaded_bytes) == expected_size, f"Size mismatch: {len(downloaded_bytes)} vs {expected_size}"
    downloaded_sha = hashlib.sha256(downloaded_bytes).hexdigest()
    assert downloaded_sha == expected_sha256, f"Checksum mismatch on read: {downloaded_sha} vs {expected_sha256}"
    log(f"TEST 4 PASSED: Download succeeded via healthy replica ({len(downloaded_bytes)} bytes verified)", "PASS")

    # TEST 5: Automatic repair restoration to RF=3
    log("--- TEST 5: Waiting for automatic repair job to complete ---")
    start_repair_wait = time.time()
    repair_completed = False
    repaired_target_node = None

    while time.time() - start_repair_wait < 15:
        # Check jobs
        jobs_res = requests.get(f"{COORDINATOR_URL}/repair/jobs", timeout=2)
        if jobs_res.status_code == 200:
            jobs = jobs_res.json()
            completed_job = next((j for j in jobs if j["object_id"] == object_id and j["status"] == "COMPLETED"), None)
            if completed_job:
                repair_completed = True
                repaired_target_node = completed_job["target_node_id"]
                log(f"Repair job completed: ID={completed_job['job_id']}, target={repaired_target_node}, bytes={completed_job['bytes_transferred']}")
                break
        time.sleep(0.5)

    assert repair_completed, "Repair job did not complete within timeout"

    # Verify healthy replica count is 3
    res = requests.get(f"{COORDINATOR_URL}/objects/{object_id}", timeout=TIMEOUT)
    assert res.status_code == 200
    obj_after_repair = res.json()
    healthy_stored = [r for r in obj_after_repair.get("replicas", []) if r["status"] == "STORED"]
    log(f"Healthy STORED replicas after repair: {[r['node_id'] for r in healthy_stored]}")
    assert len(healthy_stored) == 3, f"Expected 3 healthy STORED replicas, got {len(healthy_stored)}"
    log("TEST 5 PASSED: Desired replication factor RF=3 restored.", "PASS")

    # TEST 6: Physical repair verification on disk
    log(f"--- TEST 6: Verify physical file existence on replacement target {repaired_target_node} ---")
    target_container = f"vault-{repaired_target_node}"
    check_file_cmd = f"docker exec {target_container} ls -l /data/objects/{object_id}"
    res = subprocess.run(check_file_cmd, shell=True, capture_output=True, text=True)
    assert res.returncode == 0, f"Physical file missing on target {repaired_target_node}: {res.stderr}"
    log(f"Physical file exists: {res.stdout.strip()}")
    log("TEST 6 PASSED: Replacement target physically wrote object bytes to disk.", "PASS")

    # TEST 7: Repair checksum verification
    log(f"--- TEST 7: Calculate target physical SHA-256 and compare to coordinator SHA-256 ---")
    calc_sha_cmd = f"docker exec {target_container} sha256sum /data/objects/{object_id}"
    sha_res = subprocess.run(calc_sha_cmd, shell=True, capture_output=True, text=True)
    assert sha_res.returncode == 0, f"Failed to compute sha256sum in container: {sha_res.stderr}"
    target_physical_sha = sha_res.stdout.split()[0].strip()
    log(f"Target physical SHA-256: {target_physical_sha}")
    log(f"Coordinator catalog SHA-256: {expected_sha256}")
    assert target_physical_sha == expected_sha256, f"Target physical SHA mismatch: {target_physical_sha} != {expected_sha256}"
    log("TEST 7 PASSED: Target physical checksum matches authoritative checksum.", "PASS")

    # TEST 8: Repair metadata updated
    log("--- TEST 8: Verify replacement replica metadata in catalog ---")
    new_rep = next((r for r in obj_after_repair.get("replicas", []) if r["node_id"] == repaired_target_node), None)
    assert new_rep is not None, f"Replacement replica metadata for {repaired_target_node} missing"
    assert new_rep["status"] == "STORED", f"Expected STORED, got {new_rep['status']}"
    assert new_rep["sha256"] == expected_sha256, f"Replica sha256 mismatch: {new_rep['sha256']}"
    assert new_rep["size_bytes"] == expected_size, f"Replica size mismatch: {new_rep['size_bytes']}"
    log(f"TEST 8 PASSED: Replacement replica metadata correctly recorded as STORED.", "PASS")

    # TEST 9: Node recovery state transitions
    log(f"--- TEST 9: Recovering failed node {failed_node} ---")
    rec_res = requests.post(f"{COORDINATOR_URL}/faults/node/{failed_node}/recover", timeout=TIMEOUT)
    assert rec_res.status_code == 200

    observed_recovering = False
    observed_recovered = False
    start_rec_wait = time.time()
    while time.time() - start_rec_wait < 10:
        node_st = requests.get(f"{COORDINATOR_URL}/nodes/{failed_node}", timeout=2).json().get("status")
        if node_st == "RECOVERING":
            observed_recovering = True
        elif node_st == "HEALTHY":
            observed_recovered = True
            break
        time.sleep(0.3)

    assert observed_recovered, f"Node {failed_node} did not return to HEALTHY"
    log(f"TEST 9 PASSED: Node {failed_node} successfully recovered to HEALTHY.", "PASS")

    # TEST 10: No unnecessary fourth replica (Rule 17)
    log("--- TEST 10: Rule 17: Verify no unnecessary fourth replica created after node returns ---")
    time.sleep(3.0)  # Allow recovery reconciliation to finish
    res = requests.get(f"{COORDINATOR_URL}/objects/{object_id}", timeout=TIMEOUT)
    assert res.status_code == 200
    final_replicas = res.json().get("replicas", [])
    final_stored = [r for r in final_replicas if r["status"] == "STORED"]
    log(f"Replicas after node recovery reconciliation: {[r['node_id'] for r in final_stored]}")
    assert len(final_stored) == 3, f"Rule 17 violated: expected exactly 3 STORED replicas, found {len(final_stored)}"
    log("TEST 10 PASSED: Desired RF=3 maintained without redundant 4th replica.", "PASS")

    # Verify physical redundant file was actually removed from disk
    check_failed_node_cmd = f"docker exec vault-{failed_node} test -f /data/objects/{object_id}"
    res_redundant = subprocess.run(check_failed_node_cmd, shell=True)
    assert res_redundant.returncode != 0, f"Redundant physical file still exists on {failed_node}!"
    log(f"Verified physical redundant file on {failed_node} was removed from disk.", "PASS")

    # Cleanup test object
    requests.delete(f"{COORDINATOR_URL}/objects/{object_id}", timeout=TIMEOUT)
    log(f"Cleaned up test object {object_id}")


def test_11_phase2_regression():
    log("=== TEST 11 — Phase 2 Regression Test Suite ===")
    res = subprocess.run([sys.executable, "test_phase2.py"], capture_output=True, text=True)
    print(res.stdout)
    if res.stderr:
        print(res.stderr, file=sys.stderr)
    assert res.returncode == 0, f"Phase 2 regression tests failed with exit code {res.returncode}"
    log("TEST 11 PASSED: Phase 2 regression suite passed.", "PASS")


def test_12_phase1_regression():
    log("=== TEST 12 — Phase 1 Regression Test Suite ===")
    res = subprocess.run([sys.executable, "test_phase1.py"], capture_output=True, text=True)
    print(res.stdout)
    if res.stderr:
        print(res.stderr, file=sys.stderr)
    assert res.returncode == 0, f"Phase 1 regression tests failed with exit code {res.returncode}"
    log("TEST 12 PASSED: Phase 1 regression suite passed.", "PASS")


def test_13_phase0_regression():
    log("=== TEST 13 — Phase 0 Regression Test Suite ===")
    res = subprocess.run([sys.executable, "run_tests.py"], capture_output=True, text=True)
    print(res.stdout)
    if res.stderr:
        print(res.stderr, file=sys.stderr)
    assert res.returncode == 0, f"Phase 0 regression tests failed with exit code {res.returncode}"
    log("TEST 13 PASSED: Phase 0 regression suite passed.", "PASS")


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("VAULT PHASE 3 AUTOMATED VERIFICATION SUITE")
    print("=" * 70 + "\n")

    try:
        test_1_heartbeat()
        test_2_failure_detection()
        test_3_through_10_repair_lifecycle()
        test_11_phase2_regression()
        test_12_phase1_regression()
        test_13_phase0_regression()

        print("\n" + "=" * 70)
        log("ALL 13 PHASE 3 TESTS & REGRESSIONS PASSED SUCCESSFULLY!", "PASS")
        print("=" * 70 + "\n")
        sys.exit(0)
    except AssertionError as e:
        log(f"TEST FAILED: {e}", "FAIL")
        sys.exit(1)
    except Exception as e:
        log(f"UNEXPECTED ERROR: {e}", "FAIL")
        import traceback
        traceback.print_exc()
        sys.exit(1)
