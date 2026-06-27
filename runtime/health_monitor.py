"""
Health Monitor — Runtime 健康检查。

监控：
  - Worker 存活状态
  - 过期 Lease 回收
  - Queue 积压
  - Event Bus 状态
  - Tool 可用性
  - LLM 可用性
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from .event_bus import EventBus, Event, EventPriority


class HealthStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ComponentHealth:
    """组件健康状态"""
    name: str
    status: HealthStatus = HealthStatus.UNKNOWN
    last_check: float = field(default_factory=time.time)
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkerHeartbeat:
    worker_name: str
    last_seen: float
    status: HealthStatus = HealthStatus.UNKNOWN


class HealthMonitor:
    """运行时健康监控"""

    def __init__(
        self,
        event_bus: EventBus,
        check_interval_sec: float = 5.0,
        worker_timeout_sec: float = 30.0,
    ) -> None:
        self._event_bus = event_bus
        self._check_interval = check_interval_sec
        self._worker_timeout = worker_timeout_sec
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()

        self._components: Dict[str, ComponentHealth] = {}
        self._workers: Dict[str, WorkerHeartbeat] = {}
        self._checkers: Dict[str, Callable[[], tuple[HealthStatus, str]]] = {}
        self._executor_ref: Any = None  # 弱引用 Executor 以检查 lease

    def register_component(self, name: str, checker: Callable[[], tuple[HealthStatus, str]]) -> None:
        """注册组件健康检查"""
        self._checkers[name] = checker
        self._components[name] = ComponentHealth(name=name)

    def register_worker(self, worker_name: str) -> None:
        """注册 Worker 心跳追踪"""
        with self._lock:
            self._workers[worker_name] = WorkerHeartbeat(
                worker_name=worker_name,
                last_seen=time.time(),
                status=HealthStatus.HEALTHY,
            )

    def worker_pulse(self, worker_name: str) -> None:
        """Worker 发送心跳"""
        with self._lock:
            if worker_name in self._workers:
                self._workers[worker_name].last_seen = time.time()
                self._workers[worker_name].status = HealthStatus.HEALTHY

    def set_executor(self, executor: Any) -> None:
        """注入 Executor 引用，用于 lease 过期检查"""
        self._executor_ref = executor

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="HealthMonitor")
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        while self._running:
            try:
                self._check()
            except Exception:
                pass
            time.sleep(self._check_interval)

    def _check(self) -> None:
        with self._lock:
            # 检查注册的组件
            for name, checker in self._checkers.items():
                try:
                    status, msg = checker()
                    comp = self._components[name]
                    old_status = comp.status
                    comp.status = status
                    comp.message = msg
                    comp.last_check = time.time()
                    if old_status != status:
                        self._event_bus.publish(Event(
                            name="system.health_change",
                            data={"component": name, "status": status.value, "message": msg},
                            priority=EventPriority.HIGH,
                            source="HealthMonitor"
                        ))
                except Exception as e:
                    self._components[name].status = HealthStatus.UNHEALTHY
                    self._components[name].message = str(e)

            # 检查 Worker 超时
            now = time.time()
            for worker_name, hb in list(self._workers.items()):
                if now - hb.last_seen > self._worker_timeout and hb.status != HealthStatus.UNHEALTHY:
                    hb.status = HealthStatus.UNHEALTHY
                    self._event_bus.publish(Event(
                        name="worker.unhealthy",
                        data={"worker": worker_name, "last_seen": hb.last_seen},
                        priority=EventPriority.HIGH,
                        source="HealthMonitor"
                    ))

            # 检查过期 Lease
            if self._executor_ref:
                expired = self._executor_ref.check_expired_leases()
                for lease in expired:
                    self._event_bus.publish(Event(
                        name="lease.expired",
                        data={"task_id": lease.task_id, "worker": lease.worker_name},
                        priority=EventPriority.HIGH,
                        source="HealthMonitor"
                    ))

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            overall = HealthStatus.HEALTHY
            components = {}
            for name, comp in self._components.items():
                components[name] = {
                    "status": comp.status.value,
                    "message": comp.message,
                    "last_check": comp.last_check,
                }
                if comp.status == HealthStatus.UNHEALTHY:
                    overall = HealthStatus.UNHEALTHY
                elif comp.status == HealthStatus.DEGRADED and overall == HealthStatus.HEALTHY:
                    overall = HealthStatus.DEGRADED

            workers = {}
            for name, hb in self._workers.items():
                workers[name] = {
                    "status": hb.status.value,
                    "last_seen": hb.last_seen,
                }
                if hb.status == HealthStatus.UNHEALTHY:
                    overall = HealthStatus.DEGRADED

            return {
                "overall": overall.value,
                "timestamp": time.time(),
                "components": components,
                "workers": workers,
            }
