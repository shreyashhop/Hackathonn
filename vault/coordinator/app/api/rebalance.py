import os
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, status, Query
from pydantic import BaseModel, Field

from ..core.rebalance_manager import rebalance_mgr
from ..core.health_monitor import health_monitor
from ..models import (
    NodeRegisterRequest,
    NodeDecommissionRequest,
    RebalancePlan,
    RebalanceStatus,
)

router = APIRouter(prefix="/rebalance", tags=["Dynamic Rebalancing"])


@router.get("/plan", response_model=Dict[str, Any])
async def get_rebalance_plan(
    active_nodes: Optional[str] = Query(None, description="Comma-separated list of active node IDs to simulate")
):
    """
    Dry-run rebalance plan calculation using Rendezvous Hashing (HRW).
    Identifies which replicas must move with minimal cluster churn.
    Performs zero physical mutations.
    """
    try:
        active_nids = [n.strip() for n in active_nodes.split(",") if n.strip()] if active_nodes else None
        plan = rebalance_mgr.generate_plan(active_node_ids=active_nids)
        return plan
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate rebalancing plan"
        )


@router.post("/start", response_model=Dict[str, Any])
async def start_rebalance():
    """
    Starts asynchronous dynamic rebalancing execution.
    Transfers physical bytes, verifies SHA-256 and size, marks STORED,
    and safely prunes displaced replicas with copy-before-delete safety.
    """
    try:
        result = await rebalance_mgr.start_rebalance()
        if result.get("status") == "already_running":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A rebalance run is already in progress"
            )
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to initiate rebalance migration"
        )


@router.get("/status", response_model=Dict[str, Any])
async def get_rebalance_status():
    """Returns current rebalance execution state, progress, and transfer metrics."""
    try:
        status_data = rebalance_mgr.get_status()
        status_data["concurrency_limit"] = rebalance_mgr._concurrency_limit
        return status_data
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve rebalance status"
        )


@router.post("/cancel", response_model=Dict[str, Any])
async def cancel_rebalance():
    """Gracefully cancels active rebalance migration."""
    try:
        return await rebalance_mgr.cancel_rebalance()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to cancel rebalance"
        )


@router.post("/nodes/register", status_code=status.HTTP_201_CREATED, response_model=Dict[str, Any])
async def register_node(req: NodeRegisterRequest):
    """
    Dynamically registers an additional storage node into the cluster topology.
    Enters active heartbeat checks and recalculates HRW placement availability.
    """
    try:
        host = req.host.strip()
        # In Docker, auto-resolve localhost/127.0.0.1 to host.docker.internal for reachability
        is_docker = os.path.exists("/.dockerenv") or os.getenv("RUNNING_IN_DOCKER") == "true"
        if is_docker and host in ("localhost", "127.0.0.1"):
            target_host = "host.docker.internal"
        else:
            target_host = host

        url = req.url or f"http://{target_host}:{req.port}"

        node_cfg = {
            "id": req.node_id,
            "node_id": req.node_id,
            "host": target_host,
            "port": req.port,
            "url": url,
        }
        registered = await rebalance_mgr.register_node(node_cfg)
        return {
            "status": "registered",
            "message": f"Storage node {req.node_id} successfully registered into cluster",
            "node": registered,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to register storage node"
        )


@router.post("/nodes/decommission", response_model=Dict[str, Any])
async def decommission_node(req: NodeDecommissionRequest):
    """
    Safely decommissions a storage node from the cluster.
    Evacuates all hosted replicas to healthy peer nodes before removing from topology.
    """
    try:
        node_rec = health_monitor.get_node(req.node_id)
        if not node_rec:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Node {req.node_id} not found in cluster configuration"
            )
        return await rebalance_mgr.decommission_node(req.node_id)
    except HTTPException:
        raise
    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(ve)
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to decommission storage node"
        )


@router.post("/nodes/deregister", response_model=Dict[str, Any])
async def deregister_node(req: NodeDecommissionRequest):
    """
    Directly deregisters and purges a node from cluster tracking.
    """
    try:
        health_monitor.deregister_node(req.node_id, purge=True)
        return {"status": "deregistered", "node_id": req.node_id}
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to deregister node"
        )
