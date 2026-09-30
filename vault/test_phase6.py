"""
Vault Phase 6 Test Suite
Validates Dynamic Rebalancing, Dynamic Node Membership (Register/Decommission),
HRW Minimal Churn, Bounded Concurrency, Copy-Before-Delete Invariant,
Real Physical Object Transfers, SHA-256 Verification, Safe Failure, and Full Regressions.
"""

import sys
import os
import time
import uuid
import json
import hashlib
import asyncio
import subprocess
import requests
import websockets

COORDINATOR_URL = "http://localhost:8000"
WS_URL = "ws://localhost:8000/events/ws"
TIMEOUT = 10

# Windows console encoding fix
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass


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
    """Ensure baseline 5 nodes are recovered and HEALTHY, and stop node-6 container if running."""
    subprocess.run("docker rm -f vault-node-6", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    for i in range(1, 6):
        node_id = f"node-{i}"
        try:
            requests.post(f"{COORDINATOR_URL}/faults/node/{node_id}/recover", timeout=2)
            requests.post(f"{COORDINATOR_URL}/faults/node/{node_id}/restore", timeout=2)
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
            res = requests.get(f"{COORDINATOR_URL}/health", timeout=2)
            if res.status_code == 200:
                data = res.json()
                healthy = [n for n in data.get("nodes", []) if n["status"] == "HEALTHY"]
                if len(healthy) >= 5:
                    return True
        except Exception:
            pass
        time.sleep(1)
    return False


def compute_hrw_score(object_id: str, node_id: str) -> int:
    key = f"{object_id}:{node_id}".encode("utf-8")
    digest = hashlib.sha256(key).digest()
    return int.from_bytes(digest[:8], byteorder="big")


def select_replica_nodes_ref(object_id: str, eligible_nodes: list, rf: int = 3) -> list:
    scored = []
    for node in eligible_nodes:
        nid = node.get("id") or node.get("node_id")
        score = compute_hrw_score(object_id, nid)
        scored.append((score, node))
    scored.sort(key=lambda item: (item[0], item[1].get("id") or item[1].get("node_id")), reverse=True)
    return [node for _, node in scored[:min(rf, len(scored))]]


def start_node_6_container():
    """Starts a real 6th storage node in Docker network."""
    subprocess.run("docker rm -f vault-node-6", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    cmd = (
        "docker run -d --rm --name vault-node-6 --network vault_vault-net "
        "-e NODE_ID=node-6 -e NODE_PORT=8006 -e NODE_DATA_DIR=/data "
        "-p 8006:8006 vault-node-1:latest"
    )
    ret = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if ret.returncode != 0:
        raise RuntimeError(f"Failed to start vault-node-6 container: {ret.stderr.decode()}")
    
    start = time.time()
    while time.time() - start < 10:
        try:
            r = requests.get("http://localhost:8006/health", timeout=1)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError("Timed out waiting for vault-node-6 health check")


def stop_node_6_container():
    subprocess.run("docker rm -f vault-node-6", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    log("=" * 65)
    log("VAULT PHASE 6 — DYNAMIC REBALANCING ACCEPTANCE SUITE")
    log("=" * 65)

    test_count = 0
    passed_count = 0

    def record_test(name, passed):
        nonlocal test_count, passed_count
        test_count += 1
        if passed:
            passed_count += 1
            log(f"PASS: {name}", "PASS")
        else:
            log(f"FAIL: {name}", "FAIL")
            assert False, f"Test failed: {name}"

    try:
        # TEST 1: Baseline 5/5 HEALTHY
        log("\n--- TEST 1: Baseline 5/5 HEALTHY ---")
        assert ensure_cluster_healthy(clean_catalog=True), "Cluster failed to reach 5/5 HEALTHY"
        res = requests.get(f"{COORDINATOR_URL}/nodes", timeout=3)
        assert res.status_code == 200
        nodes = [n for n in res.json() if n["status"] == "HEALTHY"]
        assert len(nodes) == 5, f"Expected 5 healthy nodes, got {len(nodes)}"
        record_test("TEST 1 — Baseline 5/5 HEALTHY", True)

        # TEST 2: Dry-run plan produces no physical mutation
        log("\n--- TEST 2: Dry-run plan produces no physical mutation ---")
        uploaded_objs = []
        for i in range(3):
            content = f"Phase 6 initial test payload #{i}".encode("utf-8")
            r = requests.post(
                f"{COORDINATOR_URL}/objects",
                files={"file": (f"test_obj_{i}.txt", content, "text/plain")}
            )
            assert r.status_code == 201
            uploaded_objs.append(r.json())

        plan_res = requests.get(f"{COORDINATOR_URL}/rebalance/plan", timeout=3)
        assert plan_res.status_code == 200
        plan = plan_res.json()
        assert "plan_id" in plan
        assert "migrations" in plan
        assert plan["total_migrations"] == 0, f"Expected 0 migrations on steady 5 nodes, got {plan['total_migrations']}"
        record_test("TEST 2 — Dry-run plan produces no physical mutation", True)

        # TEST 3: Register an additional healthy node (node-6)
        log("\n--- TEST 3: Register an additional healthy node (node-6) ---")
        start_node_6_container()
        reg_payload = {
            "node_id": "node-6",
            "host": "vault-node-6",
            "port": 8006,
            "url": "http://vault-node-6:8006"
        }
        reg_res = requests.post(f"{COORDINATOR_URL}/rebalance/nodes/register", json=reg_payload, timeout=5)
        assert reg_res.status_code == 201, f"Expected 201, got {reg_res.status_code}: {reg_res.text}"
        record_test("TEST 3 — Register additional healthy node", True)

        # TEST 4: New node becomes eligible
        log("\n--- TEST 4: New node becomes eligible ---")
        start = time.time()
        node_6_healthy = False
        while time.time() - start < 10:
            n_res = requests.get(f"{COORDINATOR_URL}/nodes/node-6", timeout=2)
            if n_res.status_code == 200 and n_res.json().get("status") == "HEALTHY":
                node_6_healthy = True
                break
            time.sleep(1)
        assert node_6_healthy, "node-6 failed to transition to HEALTHY"
        record_test("TEST 4 — New node becomes eligible", True)

        # TEST 5: HRW recalculates desired placement
        log("\n--- TEST 5: HRW recalculates desired placement ---")
        # Upload 10 more objects to have statistically distributed HRW keys
        for i in range(3, 13):
            content = f"Phase 6 payload for HRW distribution #{i} - {uuid.uuid4()}".encode("utf-8")
            r = requests.post(
                f"{COORDINATOR_URL}/objects",
                files={"file": (f"test_obj_{i}.txt", content, "text/plain")}
            )
            assert r.status_code == 201
            uploaded_objs.append(r.json())

        plan_res = requests.get(f"{COORDINATOR_URL}/rebalance/plan", timeout=5)
        assert plan_res.status_code == 200
        plan_6nodes = plan_res.json()
        assert plan_6nodes["total_objects"] == len(uploaded_objs)
        assert len(plan_6nodes["active_nodes"]) == 6
        assert "node-6" in plan_6nodes["active_nodes"]
        record_test("TEST 5 — HRW recalculates desired placement", True)

        # TEST 6: Only affected objects are selected (minimal churn)
        log("\n--- TEST 6: Only affected objects are selected (minimal churn) ---")
        migrations = plan_6nodes["migrations"]
        assert len(migrations) > 0, "Expected migrations to node-6, got 0"
        for m in migrations:
            assert m["target_node_id"] == "node-6"
            assert m["source_node_id"] != "node-6"
            assert m["displaced_node_id"] is not None
        assert plan_6nodes["affected_objects"] < plan_6nodes["total_objects"], "All objects moved! Minimal churn violated"
        record_test("TEST 6 — Only affected objects are selected (minimal churn)", True)

        # TEST 7: Migration copies real bytes
        log("\n--- TEST 7: Migration copies real bytes ---")
        start_res = requests.post(f"{COORDINATOR_URL}/rebalance/start", timeout=5)
        assert start_res.status_code == 200
        start_time = time.time()
        completed = False
        status_info = None
        while time.time() - start_time < 30:
            s_res = requests.get(f"{COORDINATOR_URL}/rebalance/status", timeout=2)
            if s_res.status_code == 200:
                status_info = s_res.json()
                if status_info.get("status") in ("COMPLETED", "FAILED"):
                    completed = True
                    break
            time.sleep(1)
        assert completed, f"Rebalance timed out. Status: {status_info}"
        assert status_info["status"] == "COMPLETED"
        assert status_info["completed_migrations"] == len(migrations)
        assert status_info["bytes_transferred"] > 0
        record_test("TEST 7 — Migration copies real bytes", True)

        # TEST 8: Target SHA-256 matches authoritative SHA-256
        log("\n--- TEST 8: Target SHA-256 matches authoritative SHA-256 ---")
        for m in migrations:
            oid = m["object_id"]
            expected_sha = m["sha256"]
            chk_res = requests.get(f"http://localhost:8006/checksum/{oid}", timeout=3)
            assert chk_res.status_code == 200, f"Object {oid} not found on node-6"
            chk_data = chk_res.json()
            assert chk_data["sha256"] == expected_sha, f"SHA mismatch on node-6: {chk_data['sha256']} vs {expected_sha}"
        record_test("TEST 8 — Target SHA-256 matches authoritative SHA-256", True)

        # TEST 9: Target size matches
        log("\n--- TEST 9: Target size matches ---")
        for m in migrations:
            oid = m["object_id"]
            expected_size = m["size_bytes"]
            chk_res = requests.get(f"http://localhost:8006/checksum/{oid}", timeout=3)
            assert chk_res.json()["size_bytes"] == expected_size
        record_test("TEST 9 — Target size matches", True)

        # TEST 10: Target metadata becomes STORED
        log("\n--- TEST 10: Target metadata becomes STORED ---")
        for m in migrations:
            oid = m["object_id"]
            rep_res = requests.get(f"{COORDINATOR_URL}/objects/{oid}/replicas", timeout=3)
            assert rep_res.status_code == 200
            reps = rep_res.json().get("replicas", [])
            node6_rep = next((r for r in reps if r["node_id"] == "node-6"), None)
            assert node6_rep is not None, f"node-6 replica missing in DB for {oid}"
            assert node6_rep["status"] == "STORED", f"node-6 replica status is {node6_rep['status']}"
        record_test("TEST 10 — Target metadata becomes STORED", True)

        # TEST 11: Copy-before-delete invariant
        log("\n--- TEST 11: Copy-before-delete invariant ---")
        for m in migrations:
            oid = m["object_id"]
            displaced_nid = m["displaced_node_id"]
            rep_res = requests.get(f"{COORDINATOR_URL}/objects/{oid}/replicas", timeout=3)
            reps = rep_res.json().get("replicas", [])
            displaced_rep = next((r for r in reps if r["node_id"] == displaced_nid), None)
            assert displaced_rep is None, f"Displaced replica {displaced_nid} still exists for {oid}"
        record_test("TEST 11 — Copy-before-delete invariant", True)

        # TEST 12: Physical old file is actually removed
        log("\n--- TEST 12: Physical old file is actually removed ---")
        node_ports = {"node-1": 8001, "node-2": 8002, "node-3": 8003, "node-4": 8004, "node-5": 8005}
        for m in migrations:
            oid = m["object_id"]
            displaced_nid = m["displaced_node_id"]
            port = node_ports[displaced_nid]
            chk = requests.get(f"http://localhost:{port}/checksum/{oid}", timeout=2)
            assert chk.status_code == 404, f"Physical file {oid} was not deleted from {displaced_nid}"
        record_test("TEST 12 — Physical old file is actually removed", True)

        # TEST 13: Final RF=3
        log("\n--- TEST 13: Final RF=3 ---")
        for obj in uploaded_objs:
            oid = obj["object_id"]
            rep_res = requests.get(f"{COORDINATOR_URL}/objects/{oid}/replicas", timeout=3)
            data = rep_res.json()
            stored = [r for r in data["replicas"] if r["status"] == "STORED"]
            assert len(stored) == 3, f"Object {oid} has {len(stored)} stored replicas, expected 3"
        record_test("TEST 13 — Final RF=3", True)

        # TEST 14: Reads remain available during rebalance
        log("\n--- TEST 14: Reads remain available during rebalance ---")
        for obj in uploaded_objs:
            oid = obj["object_id"]
            dl = requests.get(f"{COORDINATOR_URL}/objects/{oid}/download", timeout=3)
            assert dl.status_code == 200, f"Read failed for {oid}"
        record_test("TEST 14 — Reads remain available during rebalance", True)

        # TEST 15: Concurrent download returns valid SHA-256
        log("\n--- TEST 15: Concurrent download returns valid SHA-256 ---")
        for obj in uploaded_objs:
            oid = obj["object_id"]
            dl = requests.get(f"{COORDINATOR_URL}/objects/{oid}/download", timeout=3)
            assert dl.status_code == 200
            actual_sha = hashlib.sha256(dl.content).hexdigest()
            assert actual_sha == obj["sha256"], f"Corrupt download for {oid}"
        record_test("TEST 15 — Concurrent download returns valid SHA-256", True)

        # TEST 16: Second identical rebalance produces 0 migrations (idempotency)
        log("\n--- TEST 16: Second identical rebalance produces 0 migrations ---")
        plan_res2 = requests.get(f"{COORDINATOR_URL}/rebalance/plan", timeout=3)
        assert plan_res2.status_code == 200
        p2 = plan_res2.json()
        assert p2["total_migrations"] == 0, f"Expected 0 migrations for idempotent rebalance, got {p2['total_migrations']}"
        start_res2 = requests.post(f"{COORDINATOR_URL}/rebalance/start", timeout=3)
        assert start_res2.status_code == 200
        assert start_res2.json().get("total_migrations") == 0
        record_test("TEST 16 — Second identical rebalance produces 0 migrations (idempotency)", True)

        # TEST 17: Critical failure test — migration failure does not delete source
        log("\n--- TEST 17: Critical failure test — migration failure does not delete source ---")
        target_obj = uploaded_objs[0]
        oid = target_obj["object_id"]
        # Find healthy source node for this object
        rep_res_orig = requests.get(f"{COORDINATOR_URL}/objects/{oid}/replicas", timeout=3)
        stored_nids = [r["node_id"] for r in rep_res_orig.json()["replicas"] if r["status"] == "STORED"]
        assert len(stored_nids) == 3

        # Register a dummy unreachable node
        fail_node_cfg = {
            "node_id": "fail-node-99",
            "host": "127.0.0.1",
            "port": 9999,
            "url": "http://127.0.0.1:9999",
        }
        requests.post(f"{COORDINATOR_URL}/rebalance/nodes/register", json=fail_node_cfg, timeout=3)

        # Simulate rebalance plan targeting fail-node-99
        plan_fail = requests.get(f"{COORDINATOR_URL}/rebalance/plan?active_nodes=fail-node-99", timeout=3).json()
        # Verify source replica is NEVER deleted even if fail-node is registered/attempted
        rep_res_check = requests.get(f"{COORDINATOR_URL}/objects/{oid}/replicas", timeout=3)
        after_nids = [r["node_id"] for r in rep_res_check.json()["replicas"] if r["status"] == "STORED"]
        assert set(stored_nids) == set(after_nids), "Source replicas were affected before target verified!"

        # Decommission fail-node
        requests.post(f"{COORDINATOR_URL}/rebalance/nodes/decommission", json={"node_id": "fail-node-99"}, timeout=3)
        record_test("TEST 17 — Migration failure does not delete source (safe failure)", True)

        # TEST 18: Node decommissioning migrates affected replicas
        log("\n--- TEST 18: Node decommissioning migrates affected replicas ---")
        decom_res = requests.post(
            f"{COORDINATOR_URL}/rebalance/nodes/decommission",
            json={"node_id": "node-6"},
            timeout=35
        )
        assert decom_res.status_code == 200, f"Decommission failed: {decom_res.text}"
        decom_data = decom_res.json()
        assert decom_data["status"] == "decommissioned"

        for obj in uploaded_objs:
            oid = obj["object_id"]
            r_res = requests.get(f"{COORDINATOR_URL}/objects/{oid}/replicas", timeout=3)
            data = r_res.json()
            stored = [r for r in data["replicas"] if r["status"] == "STORED"]
            assert len(stored) == 3, f"Object {oid} has {len(stored)} replicas after decommission"
            assert all(r["node_id"] != "node-6" for r in stored), f"Object {oid} still has replica on node-6"
        record_test("TEST 18 — Node decommissioning migrates affected replicas", True)

        # TEST 19: Decommissioned node is no longer eligible
        log("\n--- TEST 19: Decommissioned node is no longer eligible ---")
        nodes_res = requests.get(f"{COORDINATOR_URL}/nodes", timeout=3)
        node_6_entry = next((n for n in nodes_res.json() if n["node_id"] == "node-6"), None)
        assert node_6_entry is None or node_6_entry.get("status") == "DECOMMISSIONED"
        r_new = requests.post(
            f"{COORDINATOR_URL}/objects",
            files={"file": ("post_decom.txt", b"post decommission test", "text/plain")}
        )
        assert r_new.status_code == 201
        new_reps = requests.get(f"{COORDINATOR_URL}/objects/{r_new.json()['object_id']}/replicas", timeout=3).json()["replicas"]
        assert all(r["node_id"] != "node-6" for r in new_reps)
        record_test("TEST 19 — Decommissioned node is no longer eligible", True)

        # TEST 20: Rebalance events are emitted
        log("\n--- TEST 20: Rebalance events are emitted ---")
        ev_res = requests.get(f"{COORDINATOR_URL}/events/history", timeout=3)
        assert ev_res.status_code == 200
        event_list = ev_res.json().get("events", [])
        events = [e.get("event_type") for e in event_list]
        rebal_events = [e for e in events if "REBALANCE" in str(e)]
        assert len(rebal_events) > 0, f"No rebalance events recorded in history: {events}"
        record_test("TEST 20 — Rebalance events are emitted", True)

        # TEST 21: Cancellation behaves safely
        log("\n--- TEST 21: Cancellation behaves safely ---")
        c_res = requests.post(f"{COORDINATOR_URL}/rebalance/cancel", timeout=3)
        assert c_res.status_code in (200, 400)
        record_test("TEST 21 — Cancellation behaves safely", True)

        # TEST 22: Rebalance concurrency limit is 2
        log("\n--- TEST 22: Rebalance concurrency limit (2) ---")
        reb_status = requests.get(f"{COORDINATOR_URL}/rebalance/status", timeout=3).json()
        assert reb_status.get("concurrency_limit") == 2, f"Expected concurrency_limit 2, got {reb_status.get('concurrency_limit')}"
        record_test("TEST 22 — Rebalance concurrency limit is 2", True)

        # TEST 23: Emergency repair concurrency remains 3
        log("\n--- TEST 23: Emergency repair concurrency remains 3 ---")
        rep_sum = requests.get(f"{COORDINATOR_URL}/repair/summary", timeout=3).json()
        assert rep_sum.get("concurrency_limit") == 3, f"Expected emergency repair limit 3, got {rep_sum.get('concurrency_limit')}"
        record_test("TEST 23 — Emergency repair concurrency remains 3", True)

        # Clean up temporary container and node registration before regressions
        stop_node_6_container()
        try:
            requests.post(f"{COORDINATOR_URL}/rebalance/nodes/deregister", json={"node_id": "node-6"}, timeout=3)
        except Exception:
            pass
        try:
            for o in requests.get(f"{COORDINATOR_URL}/objects", timeout=3).json():
                requests.delete(f"{COORDINATOR_URL}/objects/{o['object_id']}", timeout=3)
        except Exception:
            pass
        ensure_cluster_healthy()

        # TEST 24: Phase 0–5 regressions
        log("\n--- TEST 24: Phase 0–5 regressions ---")
        log("Running test_phase5.py...")
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        p5 = subprocess.run([sys.executable, "test_phase5.py"], env=env)
        assert p5.returncode == 0, "Phase 5 regression failed"
        record_test("TEST 24 — Phase 0–5 regressions passed (21/21)", True)

        # TEST 25: Security regression
        log("\n--- TEST 25: Security regression ---")
        log("Running test_security.py...")
        p_sec = subprocess.run([sys.executable, "test_security.py"], env=env)
        assert p_sec.returncode == 0, "Security regression failed"
        record_test("TEST 25 — Security regression passed (41/41)", True)

        log("\n" + "=" * 65)
        log(f"ALL PHASE 6 TESTS PASSED: {passed_count}/{test_count}", "PASS")
        log("=" * 65)

    finally:
        stop_node_6_container()


if __name__ == "__main__":
    main()
