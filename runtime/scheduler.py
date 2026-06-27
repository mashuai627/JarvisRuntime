"""
Scheduler v3 — Thin Scheduler。

职责（仅限"什么时候可以执行"）：
  - 从 Queue 取就绪任务
  - 检查优先级
  - 检查依赖满足
  - 检查并发限制
  - 通知 Dispatcher

Dispatcher 负责："谁来执行"（不在此文件内）
"""

from __future__ import annotations

import threading
import time
import traceback
from typing import Optional

from .event_bus import EventBus, Event, EventPriority
from .queue import TaskQueue, TaskSnapshot, Priority
from .registry import Registry
from .dispatcher import Dispatcher, WorkerEndpoint
from .executor import Executor, ExecutionResult, ExecutionState
from .metrics import Metrics


class Scheduler:
    """
    瘦调度器。
    只决定"何时执行"，不决定"谁执行"。
    """

    def __init__(
        self,
        event_bus: EventBus,
        queue: TaskQueue,
        registry: Registry,
        dispatcher: Dispatcher,
        executor: Executor,
        metrics: Optional[Metrics] = None,
        max_concurrent: int = 4,
        poll_interval_ms: int = 500,
    ) -> None:
        self._event_bus = event_bus
        self._queue = queue
        self._registry = registry
        self._dispatcher = dispatcher
        self._executor = executor
        self._metrics = metrics or Metrics()
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
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="Scheduler")
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
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
        with self._lock:
            if self._active_count >= self._max_concurrent:
                return

            ready = self._queue.get_ready()
            for task_snap in ready:
                if self._active_count >= self._max_concurrent:
                    break
                self._schedule(task_snap)

    def _schedule(self, task_snap: TaskSnapshot) -> None:
        """调度：匹配 capability → Dispatcher 路由 → Executor 执行"""
        capability = task_snap.type

        # 通过 Registry 匹配 capability
        worker = self._registry.match(capability)
        if not worker or not worker.handler:
            # 无匹配 Worker，保持 queued
            return

        # 通过 Dispatcher 获取端点
        endpoint = self._dispatcher.get(worker.name)
        if not endpoint:
            self._dispatcher.register(WorkerEndpoint(
                worker_name=worker.name,
                capabilities=worker.capabilities,
                handler=worker.handler,
            ))
            endpoint = self._dispatcher.get(worker.name)

        with self._lock:
            self._active_count += 1

        # 更新快照
        self._queue.snapshot(TaskSnapshot(
            task_id=task_snap.task_id,
            type=task_snap.type,
            state="running",
            worker=worker.name,
            priority=task_snap.priority,
        ))

        self._event_bus.publish(Event(
            name="task.started",
            data={"task_id": task_snap.task_id, "worker": worker.name}
        ))

        self._metrics.gauge_set("worker.busy", self._active_count)
        self._metrics.gauge_set("queue.depth", self._queue.count("queued"))

        # 异步执行
        t = threading.Thread(
            target=self._execute,
            args=(task_snap, worker.name, worker.handler),
            daemon=True,
            name=f"Exec-{worker.name}-{task_snap.task_id}"
        )
        t.start()

    def _execute(self, task_snap: TaskSnapshot, worker_name: str, handler) -> None:
        """通过 Executor 执行"""
        start = time.time()

        def wrapped_handler(ts: TaskSnapshot) -> ExecutionResult:
            try:
                result = handler(ts)
                if result.state == ExecutionState.COMPLETED:
                    return ExecutionResult(
                        task_id=ts.task_id,
                        state=ExecutionState.COMPLETED,
                        output=result.output or {},
                        worker_name=worker_name,
                    )
                else:
                    return ExecutionResult(
                        task_id=ts.task_id,
                        state=ExecutionState.FAILED,
                        error=result.error or {"message": "Unknown"},
                        worker_name=worker_name,
                    )
            except Exception as e:
                return ExecutionResult(
                    task_id=ts.task_id,
                    state=ExecutionState.FAILED,
                    error={"message": str(e), "traceback": traceback.format_exc()},
                    worker_name=worker_name,
                )

        result = self._executor.execute(
            task=task_snap,
            worker_handler=wrapped_handler,
            worker_name=worker_name,
            timeout_ms=task_snap.timeout_ms,
        )

        elapsed_ms = (time.time() - start) * 1000
        self._metrics.record_latency("task.execution", elapsed_ms)

        if result.state in (ExecutionState.COMPLETED, ExecutionState.FAILED):
            state = "completed" if result.state == ExecutionState.COMPLETED else "failed"
            self._queue.snapshot(TaskSnapshot(
                task_id=task_snap.task_id,
                type=task_snap.type,
                state=state,
                output=result.output,
                error=result.error,
                worker=worker_name,
                priority=task_snap.priority,
            ))

            event_name = f"task.{state}"
            self._event_bus.publish(Event(
                name=event_name,
                data={"task_id": task_snap.task_id, "worker": worker_name,
                      "output": result.output, "error": result.error}
            ))

        elif result.state == ExecutionState.CANCELLED:
            self._queue.snapshot(TaskSnapshot(
                task_id=task_snap.task_id,
                type=task_snap.type,
                state="cancelled",
                worker=worker_name,
                priority=task_snap.priority,
            ))
            self._event_bus.publish(Event(
                name="task.cancelled",
                data={"task_id": task_snap.task_id}
            ))

        self._metrics.counter_inc("tasks.executed")
        with self._lock:
            self._active_count -= 1
            self._metrics.gauge_set("worker.busy", self._active_count)

    def _on_task_created(self, event: Event) -> None:
        task_id = event.data.get("task_id", "")
        typ = event.data.get("type", "")
        snap = self._queue.get_latest(task_id)
        if snap:
            self._queue.snapshot(TaskSnapshot(
                task_id=task_id,
                type=snap.type,
                state="queued",
                priority=snap.priority,
            ))
        self._event_bus.publish(Event(name="task.queued", data={"task_id": task_id}))

    def _on_task_completed(self, event: Event) -> None:
        """任务完成后，检查是否有子任务可以入队"""
        task_id = event.data.get("task_id", "")
        snap = self._queue.get_latest(task_id)
        if snap:
            for child_id in snap.children:
                child = self._queue.get_latest(child_id)
                if child and child.state == "created":
                    if self._queue._dependencies_satisfied(child):
                        self._queue.snapshot(TaskSnapshot(
                            task_id=child_id,
                            type=child.type,
                            state="queued",
                            priority=child.priority,
                        ))
                        self._event_bus.publish(Event(name="task.queued", data={"task_id": child_id}))
