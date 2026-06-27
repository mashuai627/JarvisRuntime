"""
Worker Registry v3 — Capability-based。

不再注册"ScriptWorker"，注册 "generate_script"。
一个 Capability 可以有多个 Worker 实现。
Scheduler 通过 capability 匹配，自动选择最佳 Worker。
"""

from __future__ import annotations

import importlib
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .event_bus import EventBus, Event


@dataclass
class CapabilityInfo:
    """能力信息"""
    name: str  # generate_script / generate_image / generate_video
    version: str = "1.0.0"
    description: str = ""


@dataclass
class WorkerInfo:
    """Worker 信息"""
    name: str  # ScriptWorker / ImageWorker / VideoWorker
    version: str = "1.0.0"
    capabilities: List[str] = field(default_factory=list)
    returns: str = "{task_id}.json"
    model: str = "deepseek-chat"
    enabled: bool = True
    handler: Optional[Callable] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class Registry:
    """注册中心 v3 — Capability 驱动"""

    def __init__(self, event_bus: EventBus) -> None:
        self._event_bus = event_bus
        self._capabilities: Dict[str, CapabilityInfo] = {}       # capability name → info
        self._cap_workers: Dict[str, List[str]] = {}             # capability → worker names
        self._workers: Dict[str, WorkerInfo] = {}                # worker name → info

    def register_capability(self, capability: CapabilityInfo) -> None:
        """注册一个能力"""
        if capability.name not in self._capabilities:
            self._capabilities[capability.name] = capability
            self._cap_workers[capability.name] = []
            self._event_bus.publish(Event(
                name="capability.registered",
                data={"capability": capability.name}
            ))

    def register_worker(self, worker: WorkerInfo) -> None:
        """注册一个 Worker 并关联其 capabilities"""
        if worker.name in self._workers:
            raise ValueError(f"Worker '{worker.name}' already registered")

        self._workers[worker.name] = worker

        for cap in worker.capabilities:
            if cap not in self._cap_workers:
                self._cap_workers[cap] = []
            self._cap_workers[cap].append(worker.name)

        self._event_bus.publish(Event(
            name="worker.registered",
            data={"worker": worker.name, "capabilities": worker.capabilities}
        ))

    def unregister_worker(self, name: str) -> None:
        worker = self._workers.pop(name, None)
        if worker:
            for cap in worker.capabilities:
                if cap in self._cap_workers:
                    self._cap_workers[cap] = [w for w in self._cap_workers[cap] if w != name]
        self._event_bus.publish(Event(name="worker.unregistered", data={"worker": name}))

    def find_workers_for(self, capability: str) -> List[WorkerInfo]:
        """找到提供某个 capability 的所有 Worker"""
        names = self._cap_workers.get(capability, [])
        return [self._workers[n] for n in names if n in self._workers and self._workers[n].enabled]

    def match(self, capability: str) -> Optional[WorkerInfo]:
        """为 capability 匹配最佳 Worker（优先第一个注册的 enabled worker）"""
        candidates = self.find_workers_for(capability)
        return candidates[0] if candidates else None

    def get_worker(self, name: str) -> Optional[WorkerInfo]:
        return self._workers.get(name)

    def get_capability(self, name: str) -> Optional[CapabilityInfo]:
        return self._capabilities.get(name)

    def list_capabilities(self) -> List[CapabilityInfo]:
        return list(self._capabilities.values())

    def list_workers(self) -> List[WorkerInfo]:
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
