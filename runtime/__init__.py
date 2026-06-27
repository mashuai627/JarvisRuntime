"""
Jarvis Runtime — Event-Driven AI Agent Operating Kernel (v3).

Core Components (6):
  EventBus      — 事件总线 (pub/sub)
  EventStore    — 事件溯源存储 (append-only)
  Queue         — 任务快照队列 (snapshot-based)
  Scheduler     — 调度器 (when to run)
  Dispatcher    — 分发器 (where to run)
  Executor      — 执行器 (timeout/retry/lease/cancel)
  Registry      — 注册中心 (capability-based)
  HealthMonitor — 健康监控
  Metrics       — 可观测性指标
  Command       — 用户层抽象 (Skill → Command → Runtime → Tasks)
"""

from .event_bus import EventBus, Event, EventPriority
from .event_store import EventStore, StoredEvent
from .queue import TaskQueue, TaskSnapshot, Priority
from .scheduler import Scheduler
from .dispatcher import Dispatcher, WorkerEndpoint, Backend
from .executor import Executor, ExecutionResult, ExecutionState, Lease
from .registry import Registry, WorkerInfo, CapabilityInfo
from .health_monitor import HealthMonitor, HealthStatus, ComponentHealth
from .metrics import Metrics
from .command import Command, TaskGraph

__all__ = [
    # Core
    "EventBus", "Event", "EventPriority",
    "EventStore", "StoredEvent",
    "TaskQueue", "TaskSnapshot", "Priority",
    "Scheduler",
    "Dispatcher", "WorkerEndpoint", "Backend",
    "Executor", "ExecutionResult", "ExecutionState", "Lease",
    "Registry", "WorkerInfo", "CapabilityInfo",
    "HealthMonitor", "HealthStatus", "ComponentHealth",
    "Metrics",
    "Command", "TaskGraph",
]
