"""
Scheduler — 任务调度器。

职责（仅限调度，不做其他事）：
  - 从 Queue 取就绪任务
  - 通过 Registry 匹配 Worker
  - 派发任务给 Worker 执行
  - 通过 Event Bus 通知状态变更

Scheduler 不知道 Comic/西游记/图片/视频，只知道 Task。
"""

from __future__ import annotations

import threading
import time
import traceback
from typing import Optional

from .event_bus import EventBus, Event, EventPriority
from .queue import TaskQueue, Task
from .registry import Registry, WorkerInfo


class Scheduler:
    """任务调度器 — 事件驱动"""

    def __init__(
        self,
        event_bus: EventBus,
        queue: TaskQueue,
        registry: Registry,
        max_concurrent: int = 4,
        poll_interval_ms: int = 500,
    ) -> None:
        self._event_bus = event_bus
        self._queue = queue
        self._registry = registry
        self._max_concurrent = max_concurrent
        self._poll_interval_ms = poll_interval_ms
        self._running = False
        self._active_count = 0
        self._lock = threading.RLock()
        self._thread: Optional[threading.Thread] = None

        # 订阅事件
        self._event_bus.subscribe("task.created", self._on_task_created)
        self._event_bus.subscribe("task.completed", self._on_task_completed)
        self._event_bus.subscribe("task.failed", self._on_task_completed)
        self._event_bus.subscribe("task.cancelled", self._on_task_completed)

    def start(self) -> None:
        """启动调度循环"""
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="Scheduler")
        self._thread.start()

    def stop(self) -> None:
        """停止调度"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        """主调度循环"""
        while self._running:
            try:
                self._tick()
            except Exception as e:
                self._event_bus.publish(Event(
                    name="system.error",
                    data={"message": f"Scheduler error: {e}", "traceback": traceback.format_exc()},
                    priority=EventPriority.HIGH,
                    source="Scheduler"
                ))
            time.sleep(self._poll_interval_ms / 1000)

    def _tick(self) -> None:
        """单次调度周期"""
        with self._lock:
            if self._active_count >= self._max_concurrent:
                return

            ready = self._queue.get_ready()
            for task in ready:
                if self._active_count >= self._max_concurrent:
                    break
                self._dispatch(task)

    def _dispatch(self, task: Task) -> None:
        """派发任务给 Worker"""
        worker = self._registry.match(task.type)
        if not worker:
            # 没有匹配的 Worker，任务保持 queued
            return
        if not worker.handler:
            self._event_bus.publish(Event(
                name="system.error",
                data={"message": f"Worker '{worker.name}' has no handler", "task_id": task.id},
                priority=EventPriority.HIGH,
                source="Scheduler"
            ))
            return

        with self._lock:
            self._active_count += 1
        self._queue.update_status(task.id, "running")

        self._event_bus.publish(Event(
            name="task.started",
            data={"task_id": task.id, "worker": worker.name}
        ))

        # 在独立线程中执行
        t = threading.Thread(
            target=self._execute,
            args=(task, worker),
            daemon=True,
            name=f"Worker-{worker.name}-{task.id}"
        )
        t.start()

    def _execute(self, task: Task, worker: WorkerInfo) -> None:
        """执行任务"""
        try:
            result = worker.handler(task)  # type: ignore
            if result.status == "completed":
                task.status = "completed"
                task.output = result.output
                self._queue.update_status(task.id, "completed", output=result.output)
                self._event_bus.publish(Event(
                    name="task.completed",
                    data={"task_id": task.id, "worker": worker.name}
                ))
            elif result.status == "failed":
                task.status = "failed"
                task.error = result.error
                self._queue.update_status(task.id, "failed", error=result.error)
                self._event_bus.publish(Event(
                    name="task.failed",
                    data={"task_id": task.id, "worker": worker.name, "error": result.error}
                ))
            else:
                self._queue.update_status(task.id, result.status)
        except Exception as e:
            self._queue.update_status(task.id, "failed", error={"message": str(e)})
            self._event_bus.publish(Event(
                name="task.failed",
                data={"task_id": task.id, "worker": worker.name, "error": str(e)},
                priority=EventPriority.HIGH,
                source="Scheduler"
            ))
        finally:
            with self._lock:
                self._active_count -= 1

    def _on_task_created(self, event: Event) -> None:
        """task.created → 自动入队"""
        task_id = event.data.get("task_id", "")
        self._queue.enqueue(task_id)

    def _on_task_completed(self, event: Event) -> None:
        """任务完成/失败/取消后，检查是否有子任务可以入队"""
        task_id = event.data.get("task_id", "")
        task = self._queue.get(task_id)
        if task:
            for child_id in task.children:
                child = self._queue.get(child_id)
                if child and child.status == "created":
                    if self._queue._dependencies_satisfied(child):
                        self._queue.enqueue(child_id)
