from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from datetime import datetime


class CapacityInfo(BaseModel):
    total_bytes: int = 0
    used_bytes: int = 0
    free_bytes: int = 0
    total_gb: float = 0.0
    used_gb: float = 0.0
    free_gb: float = 0.0
    usage_percent: float = 0.0
    error: Optional[str] = None


class NodeStatus(BaseModel):
    node_id: str
    host: str
    port: int
    url: str
    status: str = "unknown"  # healthy, degraded, offline, unknown
    latency_ms: Optional[float] = None
    capacity: Optional[CapacityInfo] = None
    uptime_seconds: Optional[float] = None
    objects_count: int = 0
    last_seen: Optional[str] = None
    error: Optional[str] = None


class CoordinatorHealth(BaseModel):
    service: str = "Vault Coordinator"
    status: str = "healthy"
    version: str = "0.1.0"
    uptime_seconds: float
    timestamp: str
    nodes_total: int
    nodes_healthy: int
    nodes: List[Dict[str, Any]]


class EventMessage(BaseModel):
    event_type: str
    timestamp: str
    data: Dict[str, Any]


# ============================================================
# Phase 6: Dynamic Rebalancing Models
# ============================================================

class NodeRegisterRequest(BaseModel):
    node_id: str = Field(..., pattern=r'^[a-zA-Z0-9_\-]+$', description="Alphanumeric identifier for the storage node")
    host: str = Field(..., description="Hostname or IP of the storage node")
    port: int = Field(..., ge=1024, le=65535, description="Port number of the storage node")
    url: Optional[str] = Field(None, description="Optional full URL override")


class NodeDecommissionRequest(BaseModel):
    node_id: str = Field(..., pattern=r'^[a-zA-Z0-9_\-]+$', description="ID of node to decommission")


class RebalanceMigrationStep(BaseModel):
    object_id: str
    object_name: str
    source_node_id: str
    target_node_id: str
    displaced_node_id: Optional[str] = None
    size_bytes: int
    sha256: str
    reason: str


class RebalancePlan(BaseModel):
    plan_id: str
    timestamp: str
    total_objects: int
    affected_objects: int
    total_migrations: int
    bytes_total: int
    migrations: List[RebalanceMigrationStep]
    source_distribution: Dict[str, int]
    target_distribution: Dict[str, int]
    active_nodes: List[str]


class RebalanceStatus(BaseModel):
    run_id: Optional[str] = None
    status: str  # IDLE, PLANNED, RUNNING, COMPLETED, FAILED, CANCELLED
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    total_objects: int = 0
    total_migrations: int = 0
    completed_migrations: int = 0
    failed_migrations: int = 0
    bytes_total: int = 0
    bytes_transferred: int = 0
    progress_percent: float = 0.0
    error_message: Optional[str] = None
    active_migrations: List[Dict[str, Any]] = []

