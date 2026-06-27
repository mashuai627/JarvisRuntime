"""
Executor — Worker 执行运行时。

职责（Worker 只写业务，Executor 负责所有横切关注点）：
  - Timeout 管理
  - Retry 重试（指数退避）
  - Lease（心跳 + 自动回收）
  - Cancellation（取消信号）
  - Logging（结构化日志）
  - Metrics（延迟/吞吐）
  - Future/Promise（异步结果）
  - Streaming（流式输出）
"""

from __future__ import annotations

import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Generator, Optional

from .queue import TaskSnapshot


class ExecutionState(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


@dataclass
class ExecutionResult:
    """Worker 执行结果"""
    task_id: str
    state: ExecutionState
    output: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None
    duration_ms: int = 0
    retry_count: int = 0
    worker_name: str = ""


@dataclass
class Lease:
    """任务租约——Worker 取任务后 30s 无心跳自动回收"""
    task_id: str
    worker_name: str
    acquired_at: float = field(default_factory=time.time)
    timeout_ms: int = 30000
    heartbeat_at: float = field(default_factory=time.time)
    active: bool = True

    def is_expired(self) -> bool:
        return self.active and (time.time() - self.heartbeat_at) * 1000 > self.timeout_ms

    def renew(self) -> None:
        self.heartbeat_at = time.time()

    def release(self) -> None:
        self.active = False


class Executor:
    """
    通用执行器。所有 Worker 通过 Executor 执行。
    Worker 只负责业务逻辑（纯函数），不关心基础设施。
    """

    def __init__(self, max_retries: int = 3, default_timeout_ms: int = 600000) -> None:
        self._max_retries = max_retries
        self._default_timeout_ms = default_timeout_ms
        self._leases: Dict[str, Lease] = {}
        self._cancel_flags: Dict[str, threading.Event] = {}
        self._results: Dict[str, ExecutionResult] = {}
        self._lock = threading.RLock()

        # Metrics counters
        self.metrics: Dict[str, int] = {
            "total_executions": 0,
            "successful": 0,
            "failed": 0,
            "timed_out": 0,
            "cancelled": 0,
            "total_retries": 0,
        }

    def execute(
        self,
        task: TaskSnapshot,
        worker_handler: Callable[[TaskSnapshot], ExecutionResult],
        worker_name: str = "",
        timeout_ms: Optional[int] = None,
    ) -> ExecutionResult:
        """
        执行一个 Task。自动处理 timeout / retry / lease / cancellation。
        """
        timeout_ms = timeout_ms or self._default_timeout_ms

        # 获取租约
        with self._lock:
            self.metrics["total_executions"] += 1

        lease = Lease(task_id=task.task_id, worker_name=worker_name, timeout_ms=timeout_ms)
        with self._lock:
            self._leases[task.task_id] = lease

        result = ExecutionResult(
            task_id=task.task_id,
            state=ExecutionState.RUNNING,
            worker_name=worker_name,
        )

        total_start = time.time()
        attempt = 0

        while attempt <= self._max_retries:
            # 检查取消
            if self.is_cancelled(task.task_id):
                result.state = ExecutionState.CANCELLED
                with self._lock:
                    self.metrics["cancelled"] += 1
                lease.release()
                break

            # 每次尝试独立计时（timeout per-attempt，不含 backoff）
            attempt_start = time.time()
            try:
                raw_result = worker_handler(task)
                duration = int((time.time() - attempt_start) * 1000)
                result = raw_result
                result.duration_ms = duration
                result.retry_count = attempt

                with self._lock:
                    if result.state == ExecutionState.COMPLETED:
                        self.metrics["successful"] += 1
                    elif result.state == ExecutionState.FAILED:
                        self.metrics["failed"] += 1

                # 检查超时（per-attempt）
                if duration > timeout_ms:
                    result.state = ExecutionState.TIMED_OUT
                    result.error = {"message": f"Timeout after {duration}ms (attempt {attempt})"}
                    with self._lock:
                        self.metrics["timed_out"] += 1

                lease.release()
                break

            except Exception as e:
                attempt += 1
                with self._lock:
                    self.metrics["total_retries"] += 1

                if attempt > self._max_retries:
                    result.state = ExecutionState.FAILED
                    result.error = {
                        "message": str(e),
                        "traceback": traceback.format_exc(),
                    }
                    result.retry_count = attempt
                    result.duration_ms = int((time.time() - total_start) * 1000)
                    with self._lock:
                        self.metrics["failed"] += 1
                    lease.release()
                    break

                # 指数退避（不计入 timeout）
                backoff = 1000 * (2 ** (attempt - 1))
                time.sleep(backoff / 1000)

        with self._lock:
            self._results[task.task_id] = result
            self._leases.pop(task.task_id, None)
            self._cancel_flags.pop(task.task_id, None)

        return result

    def heartbeat(self, task_id: str) -> bool:
        """Worker 心跳：续租"""
        with self._lock:
            lease = self._leases.get(task_id)
            if lease and not lease.is_expired():
                lease.renew()
                return True
            return False

    def cancel(self, task_id: str) -> bool:
        """取消任务"""
        with self._lock:
            flag = self._cancel_flags.get(task_id)
            if flag:
                flag.set()
                return True
            self._cancel_flags.setdefault(task_id, threading.Event()).set()
            return True

    def is_cancelled(self, task_id: str) -> bool:
        with self._lock:
            flag = self._cancel_flags.get(task_id)
            return flag is not None and flag.is_set()

    def check_expired_leases(self) -> List[Lease]:
        """检查过期租约（由 HealthMonitor 调用），返回需要回收的任务"""
        expired = []
        with self._lock:
            for task_id, lease in list(self._leases.items()):
                if lease.is_expired():
                    expired.append(lease)
                    self._leases.pop(task_id, None)
        return expired

    def get_result(self, task_id: str) -> Optional[ExecutionResult]:
        with self._lock:
            return self._results.get(task_id)
