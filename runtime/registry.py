"""
Worker Registry — Worker 注册与发现。

职责：
  - 管理所有已注册 Worker
  - 按 capabilities 自动匹配 Task 到 Worker
  - 支持自动发现 workers/ 目录下的 Worker 模块
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .event_bus import EventBus, Event


@dataclass
class WorkerInfo:
    """Worker 元信息"""
    name: str
    version: str
    capabilities: List[str]
    returns: str
    model: str = "deepseek-chat"
    fallback_model: Optional[str] = None
    timeout_ms: int = 120000
    enabled: bool = True
    handler: Optional[Callable] = None  # 执行函数: (Task) -> Task
    path: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class Registry:
    """Worker 注册中心"""

    def __init__(self, event_bus: EventBus) -> None:
        self._event_bus = event_bus
        self._workers: Dict[str, WorkerInfo] = {}
        # capabilities → worker names 反向索引
        self._cap_index: Dict[str, List[str]] = {}

    def register(self, worker: WorkerInfo) -> None:
        """注册 Worker"""
        if worker.name in self._workers:
            raise ValueError(f"Worker '{worker.name}' already registered")
        self._workers[worker.name] = worker
        for cap in worker.capabilities:
            self._cap_index.setdefault(cap, []).append(worker.name)
        self._event_bus.publish(Event(
            name="worker.registered",
            data={"worker": worker.name, "capabilities": worker.capabilities}
        ))

    def unregister(self, name: str) -> None:
        """注销 Worker"""
        worker = self._workers.pop(name, None)
        if worker:
            for cap in worker.capabilities:
                if cap in self._cap_index:
                    self._cap_index[cap] = [w for w in self._cap_index[cap] if w != name]
        self._event_bus.publish(Event(
            name="worker.unregistered",
            data={"worker": name}
        ))

    def find_by_capability(self, capability: str) -> List[WorkerInfo]:
        """按能力标签查找 Worker"""
        names = self._cap_index.get(capability, [])
        return [self._workers[n] for n in names if n in self._workers and self._workers[n].enabled]

    def find_by_task_type(self, task_type: str) -> List[WorkerInfo]:
        """按 Task type 查找匹配的 Worker（capability 匹配）"""
        return self.find_by_capability(task_type)

    def match(self, task_type: str) -> Optional[WorkerInfo]:
        """为 Task 匹配最佳 Worker（返回第一个匹配的）"""
        candidates = self.find_by_task_type(task_type)
        return candidates[0] if candidates else None

    def get(self, name: str) -> Optional[WorkerInfo]:
        return self._workers.get(name)

    def list_all(self) -> List[WorkerInfo]:
        return list(self._workers.values())

    def auto_discover(self, base_path: str = "workers") -> int:
        """自动发现 workers/ 目录下的 Worker 模块"""
        count = 0
        full_path = os.path.abspath(base_path)
        if not os.path.isdir(full_path):
            return 0

        sys.path.insert(0, os.path.dirname(full_path))

        for item in os.listdir(full_path):
            item_path = os.path.join(full_path, item)
            # 目录形式的 Worker（如 workers/ScriptWorker/）
            if os.path.isdir(item_path):
                worker_file = os.path.join(item_path, "__init__.py")
                if os.path.exists(worker_file):
                    try:
                        mod_name = f"workers.{item}"
                        mod = importlib.import_module(mod_name)
                        if hasattr(mod, "register"):
                            mod.register(self)
                            count += 1
                    except Exception as e:
                        self._event_bus.publish(Event(
                            name="system.error",
                            data={"message": f"Failed to load worker '{item}': {e}"}
                        ))
        return count

    def __len__(self) -> int:
        return len(self._workers)
