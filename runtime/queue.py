"""
Task Queue — 任务持久化存储。

职责：
  - 存储所有 Task（created/queued/waiting 状态）
  - 支持优先级排序
  - 支持 DAG 依赖检查
  - 不负责调度（由 Scheduler 负责）
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

from .event_bus import EventBus, Event, EventPriority


@dataclass
class Task:
    """任务数据对象"""
    id: str
    type: str
    status: str = "created"
    input: Dict[str, Any] = field(default_factory=dict)
    output: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None
    worker: Optional[str] = None
    parent: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)
    children: List[str] = field(default_factory=list)
    created_at: Optional[str] = None
    queued_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    retry_count: int = 0
    priority: int = 0
    timeout_ms: int = 600000
    tags: List[str] = field(default_factory=list)
    project: Optional[str] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None}

    @classmethod
    def from_dict(cls, data: dict) -> "Task":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class TaskQueue:
    """任务队列（内存 + JSON 持久化）"""

    def __init__(self, event_bus: EventBus, storage_path: str = "runtime/queue.json") -> None:
        self._event_bus = event_bus
        self._storage_path = storage_path
        self._lock = threading.RLock()
        self._tasks: Dict[str, Task] = {}
        self._load()

    def _load(self) -> None:
        """从磁盘恢复队列"""
        if os.path.exists(self._storage_path):
            try:
                with open(self._storage_path, "r") as f:
                    data = json.load(f)
                for task_data in data.get("tasks", []):
                    task = Task.from_dict(task_data)
                    self._tasks[task.id] = task
            except Exception:
                pass

    def _persist(self) -> None:
        """持久化到磁盘"""
        os.makedirs(os.path.dirname(self._storage_path), exist_ok=True)
        with open(self._storage_path, "w") as f:
            json.dump({"tasks": [t.to_dict() for t in self._tasks.values()]}, f, indent=2, ensure_ascii=False)

    def add(self, task: Task) -> Task:
        """添加任务到队列"""
        with self._lock:
            import datetime
            task.created_at = task.created_at or datetime.datetime.now().isoformat()
            task.status = "created"
            self._tasks[task.id] = task
            self._persist()
        self._event_bus.publish(Event(name="task.created", data={"task_id": task.id}))
        return task

    def enqueue(self, task_id: str) -> bool:
        """将任务标记为 queued（进入待调度状态）"""
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False
            task.status = "queued"
            import datetime
            task.queued_at = datetime.datetime.now().isoformat()
            self._persist()
        self._event_bus.publish(Event(name="task.queued", data={"task_id": task_id}))
        return True

    def get(self, task_id: str) -> Optional[Task]:
        with self._lock:
            return self._tasks.get(task_id)

    def update_status(self, task_id: str, status: str, **extra) -> bool:
        """更新任务状态"""
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False
            task.status = status
            for k, v in extra.items():
                if hasattr(task, k):
                    setattr(task, k, v)
            self._persist()
        return True

    def get_ready(self) -> List[Task]:
        """获取所有可执行的任务（queued 且依赖已满足）"""
        with self._lock:
            ready = []
            for task in self._tasks.values():
                if task.status != "queued":
                    continue
                if self._dependencies_satisfied(task):
                    ready.append(task)
            # 按优先级降序
            ready.sort(key=lambda t: t.priority, reverse=True)
            return ready

    def _dependencies_satisfied(self, task: Task) -> bool:
        """检查依赖是否全部完成"""
        for dep_id in task.dependencies:
            dep = self._tasks.get(dep_id)
            if dep is None or dep.status != "completed":
                return False
        # 检查 parent
        if task.parent:
            parent = self._tasks.get(task.parent)
            if parent is None or parent.status != "completed":
                return False
        return True

    def get_all(self, status: Optional[str] = None) -> List[Task]:
        with self._lock:
            if status:
                return [t for t in self._tasks.values() if t.status == status]
            return list(self._tasks.values())

    def count(self, status: Optional[str] = None) -> int:
        return len(self.get_all(status))

    def size(self) -> int:
        with self._lock:
            return len(self._tasks)
