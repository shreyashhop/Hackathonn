import os
from typing import List, Dict, Any


def get_default_nodes() -> List[Dict[str, Any]]:
    """
    Parses storage nodes from environment or defaults to the 5 standard nodes.

    Supported STORAGE_NODES formats:
      - node-1:8001,node-2:8002,...
      - http://localhost:8001,http://localhost:8002,...
      - node-1@render-host:8000,node-2@render-host:8000,...
    """
    env_nodes = os.getenv("STORAGE_NODES", "").strip()
    is_docker = os.path.exists("/.dockerenv") or os.getenv("RUNNING_IN_DOCKER") == "true"

    if env_nodes:
        nodes = []
        entries = [e.strip() for e in env_nodes.split(",") if e.strip()]

        for idx, entry in enumerate(entries, 1):
            explicit_id = None

            if "@" in entry:
                explicit_id, entry = entry.split("@", 1)
                explicit_id = explicit_id.strip()

            clean_entry = entry.replace("http://", "").replace("https://", "").rstrip("/")

            if ":" in clean_entry:
                host, port_str = clean_entry.rsplit(":", 1)
                port = int(port_str)
            else:
                host = clean_entry
                port = 8000 + idx

            node_id = explicit_id or (
                f"node-{idx}" if host in ["localhost", "127.0.0.1"] else host
            )

            nodes.append({
                "id": node_id,
                "host": host,
                "port": port,
                "url": f"http://{host}:{port}",
            })

        return nodes

    default_host = "node" if is_docker else "localhost"
    nodes = []

    for i in range(1, 6):
        host = f"node-{i}" if is_docker else "localhost"
        port = 8000 + i

        nodes.append({
            "id": f"node-{i}",
            "host": host,
            "port": port,
            "url": f"http://{host}:{port}",
        })

    return nodes


class Settings:
    COORDINATOR_PORT: int = int(os.getenv("PORT", "8000"))
    COORDINATOR_HOST: str = os.getenv("HOST", "0.0.0.0")
    STORAGE_NODES: List[Dict[str, Any]] = get_default_nodes()
    HEALTH_CHECK_INTERVAL_SECONDS: int = int(os.getenv("HEALTH_CHECK_INTERVAL", "5"))
    DATA_DIR: str = os.getenv("COORDINATOR_DATA_DIR", "./data/coordinator")
    REPLICATION_FACTOR: int = int(os.getenv("REPLICATION_FACTOR", "3"))
    WRITE_QUORUM: int = int(os.getenv("WRITE_QUORUM", "2"))


settings = Settings()
