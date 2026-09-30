import os
import re
import time
import asyncio
import traceback
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from .core.config import settings
from .core.events import manager
from .core.health_monitor import health_monitor
from .core.repair_manager import repair_mgr
from .core.integrity_scanner import integrity_scanner
from .core.rebalance_manager import rebalance_mgr
from .api.nodes import router as nodes_router, check_all_nodes
from .api.events import router as events_router
from .api.objects import router as objects_router
from .api.replication import router as replication_router
from .api.faults import router as faults_router
from .api.repair import router as repair_router
from .api.integrity import router as integrity_router
from .api.rebalance import router as rebalance_router
from .db.database import db

START_TIME = time.time()

# Maximum upload file size (50 MB)
MAX_UPLOAD_SIZE = int(os.getenv("MAX_UPLOAD_SIZE", str(50 * 1024 * 1024)))

# Allowed CORS origins — restrict to known frontend hosts
CORS_ORIGINS = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:3000,http://127.0.0.1:3000,http://localhost:8000,http://127.0.0.1:8000",
).split(",")


# ---------- Regex to scrub filesystem paths from error messages ----------
_PATH_PATTERN = re.compile(
    r'(?:[A-Za-z]:)?(?:[/\\]+[\w. @-]+){2,}',
)


def _sanitize_error(detail: str) -> str:
    """Strip filesystem paths and internal details from error messages."""
    return _PATH_PATTERN.sub("[path redacted]", str(detail))


# ---------- Security Headers Middleware ----------

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Adds security-related HTTP headers to every response."""

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Cache-Control"] = "no-store"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "connect-src 'self' ws: wss: http: https:; "
            "frame-ancestors 'none'"
        )
        # Remove Server header if present
        if "Server" in response.headers:
            del response.headers["Server"]
        return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[Coordinator] Initializing Vault Distributed Object Storage Coordinator...")
    print(f"[Coordinator] Configured storage nodes: {[n['id'] for n in settings.STORAGE_NODES]}")
    # Start active health monitor, repair worker, integrity scanner, and rebalance manager
    health_monitor.start()
    repair_mgr.start()
    integrity_scanner.start()
    rebalance_mgr.start()
    yield
    print("[Coordinator] Shutting down...")
    await health_monitor.stop()
    await repair_mgr.stop()
    await integrity_scanner.stop()
    await rebalance_mgr.stop()


app = FastAPI(
    title="Vault Coordinator",
    description="Coordinator for Vault Distributed Fault-Tolerant Object Storage",
    version="0.1.0",
    lifespan=lifespan,
    # Disable interactive API docs in Docker production to reduce attack surface
    docs_url="/docs" if os.getenv("RUNNING_IN_DOCKER") != "true" else None,
    redoc_url="/redoc" if os.getenv("RUNNING_IN_DOCKER") != "true" else None,
)

# Security headers middleware (outermost — runs last on response, first on headers)
app.add_middleware(SecurityHeadersMiddleware)

# CORS — restricted origins instead of wildcard
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
)


# ---------- Global Exception Handler — prevent info leakage ----------

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Catch-all handler that prevents internal details from leaking to clients."""
    # Log the full traceback server-side for debugging
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content={"detail": "An internal error occurred. Please try again later."},
    )


# Include API Routers
app.include_router(nodes_router)
app.include_router(events_router)
app.include_router(objects_router)
app.include_router(replication_router)
app.include_router(faults_router)
app.include_router(repair_router)
app.include_router(integrity_router)
app.include_router(rebalance_router)


@app.get("/")
def root():
    return {
        "service": "Vault Distributed Object Storage Coordinator",
        "version": "0.1.0",
        "status": "online",
    }


@app.get("/health")
async def health():
    nodes = await check_all_nodes()
    healthy_count = sum(1 for n in nodes if n["status"].lower() == "healthy")
    uptime = round(time.time() - START_TIME, 2)

    return {
        "service": "Vault Coordinator",
        "status": "healthy",
        "version": "0.1.0",
        "uptime_seconds": uptime,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "nodes_total": len(nodes),
        "nodes_healthy": healthy_count,
        "cluster_state": "HEALTHY" if healthy_count == len(nodes) else ("DEGRADED" if healthy_count > 0 else "CRITICAL"),
        "nodes": nodes,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host=settings.COORDINATOR_HOST, port=settings.COORDINATOR_PORT, reload=False)
