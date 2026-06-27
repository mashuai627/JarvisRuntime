"""
Event Bus — Jarvis Runtime 核心通信基础设施。

所有组件通过发布/订阅事件解耦，禁止直接函数调用。

事件列表（系统预定义）：
  task.created       — 新任务创建
  task.queued        — 任务入队
  task.started       — 任务开始执行
  task.completed     — 任务完成
  task.failed        — 任务失败
  task.cancelled     — 任务取消
  task.review_requested
  task.review_approved
  task.review_rejected
  worker.registered  — Worker 注册
  worker.unregistered
  system.heartbeat
  system.error
"""

from __future__ import annotations

import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List


class EventPriority(Enum):
    LOW = 0
    NORMAL = 5
    HIGH = 10
    CRITICAL = 15


@dataclass
class Event:
    """事件对象"""
    name: str
    data: Dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: f"evt-{uuid.uuid4().hex[:8]}")
    timestamp: float = field(default_factory=time.time)
    priority: EventPriority = EventPriority.NORMAL
    source: str = ""


# 回调类型: (Event) -> None
Subscriber = Callable[[Event], None]


class EventBus:
    """
    内存事件总线（单进程）。
    未来可替换为 Redis/MQTT backend。
    """

    def __init__(self) -> None:
        self._subscribers: Dict[str, List[Subscriber]] = defaultdict(list)
        self._lock = threading.RLock()
        self._history: List[Event] = []
        self._max_history = 1000
        self._running = True

    def subscribe(self, event_name: str, callback: Subscriber) -> None:
        """订阅事件"""
        with self._lock:
            self._subscribers[event_name].append(callback)

    def unsubscribe(self, event_name: str, callback: Subscriber) -> None:
        """取消订阅"""
        with self._lock:
            if event_name in self._subscribers:
                self._subscribers[event_name] = [
                    cb for cb in self._subscribers[event_name] if cb is not callback
                ]

    def publish(self, event: Event) -> None:
        """发布事件（同步通知所有订阅者）"""
        with self._lock:
            self._history.append(event)
            if len(self._history) > self._max_history:
                self._history = self._history[-self._max_history:]

        subscribers = []
        with self._lock:
            subscribers = list(self._subscribers.get(event.name, []))
            # 通配符订阅者 "*"
            subscribers.extend(self._subscribers.get("*", []))

        for callback in subscribers:
            try:
                callback(event)
            except Exception as e:
                # 订阅者异常不应影响其他订阅者
                self.publish(Event(
                    name="system.error",
                    data={"message": f"Subscriber error for {event.name}: {e}", "event_id": event.id},
                    priority=EventPriority.HIGH,
                    source="EventBus"
                ))

    def publish_simple(self, name: str, **data: Any) -> Event:
        """便捷发布"""
        event = Event(name=name, data=data)
        self.publish(event)
        return event

    def history(self, event_name: str | None = None, limit: int = 50) -> List[Event]:
        """获取历史事件"""
        with self._lock:
            if event_name:
                return [e for e in self._history if e.name == event_name][-limit:]
            return self._history[-limit:]

    def subscriber_count(self, event_name: str) -> int:
        with self._lock:
            return len(self._subscribers.get(event_name, []))

    def shutdown(self) -> None:
        self._running = False
