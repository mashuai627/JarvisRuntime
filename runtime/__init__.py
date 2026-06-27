"""
Jarvis Runtime — 事件驱动的 Agent 执行引擎。

Core Components:
  EventBus  — 事件总线，所有组件通过发布/订阅解耦
  TaskQueue — 任务持久化存储，支持 DAG 依赖
  Registry  — Worker 注册与自动发现
  Scheduler — 任务调度，按 capabilities 匹配 Worker

Architecture:
  Skill (Task Graph / Workflow)
    └─ Task (唯一驱动单元)
        └─ EventBus (发布/订阅)
            ├─ Scheduler (调度)
            ├─ Registry (发现 Worker)
            └─ Queue (存储)
"""

from .event_bus import EventBus, Event, EventPriority
from .queue import TaskQueue, Task
from .registry import Registry, WorkerInfo
from .scheduler import Scheduler

__all__ = [
    "EventBus",
    "Event",
    "EventPriority",
    "TaskQueue",
    "Task",
    "Registry",
    "WorkerInfo",
    "Scheduler",
]
