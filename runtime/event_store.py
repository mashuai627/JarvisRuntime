"""
Event Store — Event Sourcing 核心。

原理：
  Task 永远是 {"id":"TASK001"}，没有 mutable 字段。
  真实状态通过回放事件流计算得出。
  Runtime 任何时候都是 Replay → State。

存储：
  追加写（append-only），不可变。
  支持按 task_id 回放、按时间范围查询。

好处：
  Debug 超级容易，Undo/Replay 天然支持。
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Generator, List, Optional


@dataclass
class StoredEvent:
    """存储事件——不可变追加记录"""
    event_id: str = field(default_factory=lambda: f"evt-{uuid.uuid4().hex[:8]}")
    task_id: str = ""
    event_type: str = ""  # task.created / task.queued / task.started / task.completed 等
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    version: int = 0  # 单调递增，每个 task 的事件序号

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "StoredEvent":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class EventStore:
    """
    事件存储——追加写（append-only），不可变。
    每个 Task 的事件独立版本链。
    """

    def __init__(self, storage_path: str = "runtime/event_store.jsonl") -> None:
        self._storage_path = storage_path
        self._lock = threading.RLock()
        self._events: List[StoredEvent] = []
        self._task_events: Dict[str, List[StoredEvent]] = defaultdict(list)
        self._task_versions: Dict[str, int] = defaultdict(int)
        self._loaded = False
        self._load()

    def _load(self) -> None:
        """从磁盘加载事件流"""
        if not os.path.exists(self._storage_path):
            return
        with open(self._storage_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    event = StoredEvent.from_dict(data)
                    self._events.append(event)
                    self._task_events[event.task_id].append(event)
                    self._task_versions[event.task_id] = max(
                        self._task_versions[event.task_id], event.version
                    )
                except Exception:
                    pass
        self._loaded = True

    def append(self, event: StoredEvent) -> None:
        """追加事件到存储"""
        with self._lock:
            self._task_versions[event.task_id] = self._task_versions.get(event.task_id, 0) + 1
            event.version = self._task_versions[event.task_id]
            self._events.append(event)
            self._task_events[event.task_id].append(event)
            self._persist(event)

    def _persist(self, event: StoredEvent) -> None:
        """追加一行到 JSONL 文件"""
        os.makedirs(os.path.dirname(self._storage_path), exist_ok=True)
        with open(self._storage_path, "a") as f:
            f.write(json.dumps(event.to_dict(), ensure_ascii=False) + "\n")

    def wire_to_bus(self, event_bus: "EventBus") -> None:
        """订阅 EventBus，自动将所有 task.* 事件写入 EventStore"""
        task_events = [
            "task.created", "task.queued", "task.started",
            "task.completed", "task.failed", "task.cancelled",
            "task.review_requested", "task.review_approved", "task.review_rejected",
        ]

        for name in task_events:
            # 闭包捕获 name
            def make_handler(event_name):
                def handler(evt):
                    self.append(StoredEvent(
                        task_id=evt.data.get("task_id", ""),
                        event_type=event_name,
                        data=evt.data,
                    ))
                return handler
            event_bus.subscribe(name, make_handler(name))

    def replay(self, task_id: str) -> List[StoredEvent]:
        """回放某个 Task 的完整事件流"""
        with self._lock:
            return list(self._task_events.get(task_id, []))

    def get_state(self, task_id: str) -> Dict[str, Any]:
        """通过回放事件流计算 Task 当前状态"""
        events = self.replay(task_id)
        if not events:
            return {"id": task_id, "status": "unknown", "version": 0}

        state = {
            "id": task_id,
            "status": "",
            "version": events[-1].version,
            "type": "",
            "worker": "",
            "parent": None,
            "dependencies": [],
            "children": [],
            "input": {},
            "output": None,
            "error": None,
            "created_at": None,
            "queued_at": None,
            "started_at": None,
            "completed_at": None,
        }

        for evt in events:
            state["version"] = evt.version
            state["status"] = evt.event_type.replace("task.", "")
            data = evt.data

            if evt.event_type == "task.created":
                state["type"] = data.get("type", state["type"])
                state["input"] = data.get("input", state["input"])
                state["parent"] = data.get("parent", state["parent"])
                state["dependencies"] = data.get("dependencies", state["dependencies"])
                state["children"] = data.get("children", state["children"])
                state["priority"] = data.get("priority", "NORMAL")
                state["created_at"] = evt.timestamp
            elif evt.event_type == "task.queued":
                state["queued_at"] = evt.timestamp
            elif evt.event_type == "task.started":
                state["worker"] = data.get("worker", state["worker"])
                state["started_at"] = evt.timestamp
            elif evt.event_type == "task.completed":
                state["output"] = data.get("output", state["output"])
                state["completed_at"] = evt.timestamp
            elif evt.event_type == "task.failed":
                state["error"] = data.get("error", state["error"])
                state["completed_at"] = evt.timestamp
            elif evt.event_type == "task.cancelled":
                state["completed_at"] = evt.timestamp

        return state

    def get_task_at_version(self, task_id: str, version: int) -> Dict[str, Any]:
        """回放到指定版本，获取历史状态"""
        events = self.replay(task_id)
        target = [e for e in events if e.version <= version]

        class _Store:
            def __init__(self, evts):
                self._task_events = {task_id: evts}
                self._task_versions = {task_id: len(evts)}

        # 临时构造一个 store 来复用 get_state
        temp_store = _Store(target)
        temp_store.replay = lambda tid: target
        temp_store._task_events = {task_id: target}
        # 直接使用类似逻辑
        return EventStore._compute_state_from_events(task_id, target)

    @staticmethod
    def _compute_state_from_events(task_id: str, events: List[StoredEvent]) -> Dict[str, Any]:
        """纯函数：从事件列表计算状态"""
        if not events:
            return {"id": task_id, "status": "unknown", "version": 0}
        state = {"id": task_id, "status": "", "version": events[-1].version}
        for evt in events:
            state["version"] = evt.version
            state["status"] = evt.event_type.replace("task.", "")
            for k, v in evt.data.items():
                if k == "output" or k == "error":
                    state[k] = v
                elif k not in ("task_id",):
                    state[k] = v
            state[f"{evt.event_type.replace('task.', '')}_at"] = evt.timestamp
        return state

    def history(self, event_type: Optional[str] = None, limit: int = 100) -> List[StoredEvent]:
        """查询全局事件历史"""
        with self._lock:
            if event_type:
                filtered = [e for e in self._events if e.event_type == event_type]
                return filtered[-limit:]
            return self._events[-limit:]

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_events": len(self._events),
                "total_tasks": len(self._task_events),
                "file_size_kb": os.path.getsize(self._storage_path) // 1024 if os.path.exists(self._storage_path) else 0,
            }
