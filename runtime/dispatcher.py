"""
Dispatcher — 决定"谁来执行"。

职责：
  根据 Worker 的执行环境（local/docker/ssh/cloud/remote），
  将 Task 路由到正确的 Executor 后端。

Scheduler 决定"什么时候执行"。
Dispatcher 决定"谁来执行"。
这是两个职责。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from .queue import TaskSnapshot
from .registry import CapabilityInfo


class Backend(Enum):
    LOCAL = "local"
    DOCKER = "docker"
    SSH = "ssh"
    CLOUD = "cloud"
    REMOTE = "remote"


@dataclass
class WorkerEndpoint:
    """Worker 的执行端点"""
    worker_name: str
    backend: Backend = Backend.LOCAL
    address: str = "localhost"
    capabilities: List[str] = field(default_factory=list)
    handler: Optional[Callable] = None  # 本地 worker 的执行函数
    lease_timeout_ms: int = 30000


class Dispatcher:
    """
    任务分发器。
    Scheduler → Dispatcher → Executor → Worker
    """

    def __init__(self) -> None:
        self._endpoints: Dict[str, WorkerEndpoint] = {}

    def register(self, endpoint: WorkerEndpoint) -> None:
        """注册 Worker 端点"""
        self._endpoints[endpoint.worker_name] = endpoint

    def unregister(self, name: str) -> None:
        self._endpoints.pop(name, None)

    def get(self, name: str) -> Optional[WorkerEndpoint]:
        return self._endpoints.get(name)

    def route(self, capability: str) -> List[WorkerEndpoint]:
        """按 capability 找到所有可用的端点"""
        return [
            ep for ep in self._endpoints.values()
            if capability in ep.capabilities or any(cap.startswith(capability) for cap in ep.capabilities)
        ]

    def route_one(self, capability: str) -> Optional[WorkerEndpoint]:
        """按 capability 返回最佳端点（简单策略：第一个匹配）"""
        candidates = self.route(capability)
        # 优先级：local > docker > ssh > cloud > remote
        priority = {Backend.LOCAL: 0, Backend.DOCKER: 1, Backend.SSH: 2, Backend.CLOUD: 3, Backend.REMOTE: 4}
        candidates.sort(key=lambda ep: priority.get(ep.backend, 99))
        return candidates[0] if candidates else None

    def list_all(self) -> List[WorkerEndpoint]:
        return list(self._endpoints.values())
