"""
Task Queue v3 — Snapshot-based。

不再直接修改 Task。
每次状态变更只追加不可变快照。
用于 Undo / Replay / Debug。
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional


class Priority(Enum):
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4

    def weight(self) -> int:
        """数值越小优先级越高"""
        return {
            Priority.CRITICAL: 0,
            Priority.HIGH: 10,
            Priority.NORMAL: 50,
            Priority.LOW: 80,
            Priority.BACKGROUND: 100,
        }[self]


@dataclass
class TaskSnapshot:
    """任务快照——不可变"""
    task_id: str
    version: int = 0
    type: str = ""
    state: str = ""  # created / queued / running / waiting / reviewing / completed / failed / cancelled
    status_detail: str = ""  # waiting 原因说明（"等待 ComfyUI" / "等待人工确认"）
    input: Dict[str, Any] = field(default_factory=dict)
    output: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None
    worker: Optional[str] = None
    parent: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)
    children: List[str] = field(default_factory=list)
    priority: str = "NORMAL"
    timeout_ms: int = 600000
    tags: List[str] = field(default_factory=list)
    project: Optional[str] = None
    created_at: Optional[str] = None
    queued_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    updated_at: str = field(default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%S"))

    def to_dict(self) -> dict:
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None}

    @classmethod
    def from_dict(cls, data: dict) -> "TaskSnapshot":
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in data.items() if k in valid_fields})


class TaskQueue:
    """任务队列 v3 — 快照模式"""

    def __init__(self, storage_path: str = "runtime/queue.json") -> None:
        self._storage_path = storage_path
        self._lock = threading.RLock()
        self._snapshots: Dict[str, List[TaskSnapshot]] = {}  # task_id → versions
        self._load()

    def _load(self) -> None:
        if os.path.exists(self._storage_path):
            try:
                with open(self._storage_path, "r") as f:
                    data = json.load(f)
                for sdata in data.get("tasks", []):
                    snap = TaskSnapshot.from_dict(sdata)
                    self._snapshots.setdefault(snap.task_id, []).append(snap)
            except Exception:
                pass

    def _persist(self) -> None:
        os.makedirs(os.path.dirname(self._storage_path), exist_ok=True)
        all_snaps = []
        for snaps in self._snapshots.values():
            all_snaps.extend(snaps)
        with open(self._storage_path, "w") as f:
            json.dump({"tasks": [s.to_dict() for s in all_snaps]}, f, indent=2, ensure_ascii=False)

    def snapshot(self, task: TaskSnapshot) -> TaskSnapshot:
        """追加快照（不可变追加）"""
        with self._lock:
            task.version = len(self._snapshots.get(task.task_id, [])) + 1
            self._snapshots.setdefault(task.task_id, []).append(task)
            self._persist()
        return task

    def get_latest(self, task_id: str) -> Optional[TaskSnapshot]:
        """获取最新快照"""
        with self._lock:
            snaps = self._snapshots.get(task_id, [])
            return snaps[-1] if snaps else None

    def get_history(self, task_id: str) -> List[TaskSnapshot]:
        """获取完整快照历史"""
        with self._lock:
            return list(self._snapshots.get(task_id, []))

    def get_at_version(self, task_id: str, version: int) -> Optional[TaskSnapshot]:
        """获取指定版本的快照"""
        with self._lock:
            snaps = self._snapshots.get(task_id, [])
            for s in snaps:
                if s.version == version:
                    return s
            return None

    def get_ready(self) -> List[TaskSnapshot]:
        """获取所有可执行的任务（queued 且依赖已满足）"""
        with self._lock:
            ready = []
            for task_id, snaps in self._snapshots.items():
                latest = snaps[-1]
                if latest.state != "queued":
                    continue
                if self._dependencies_satisfied(latest):
                    ready.append(latest)
            # 按优先级排序（数值越小越优先）
            priority_order = {"CRITICAL": 0, "HIGH": 1, "NORMAL": 2, "LOW": 3, "BACKGROUND": 4}
            ready.sort(key=lambda t: priority_order.get(t.priority, 2))
            return ready

    def _dependencies_satisfied(self, task: TaskSnapshot) -> bool:
        """检查依赖是否全部完成"""
        for dep_id in task.dependencies:
            dep = self.get_latest(dep_id)
            if dep is None or dep.state != "completed":
                return False
        if task.parent:
            parent = self.get_latest(task.parent)
            if parent is None or parent.state != "completed":
                return False
        return True

    def get_all(self, state: Optional[str] = None) -> List[TaskSnapshot]:
        with self._lock:
            result = []
            for snaps in self._snapshots.values():
                latest = snaps[-1]
                if state is None or latest.state == state:
                    result.append(latest)
            return result

    def count(self, state: Optional[str] = None) -> int:
        return len(self.get_all(state))

    def size(self) -> int:
        with self._lock:
            return len(self._snapshots)
