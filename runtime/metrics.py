"""
Metrics — Runtime 可观测性。

指标：
  - task_count (created/queued/running/completed/failed)
  - worker_busy / worker_idle
  - event_rate (events/sec)
  - queue_depth
  - latency (avg/p50/p95/p99 ms)
  - token_usage
  - api_cost
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class LatencyRecord:
    """延迟采样"""
    value_ms: float
    timestamp: float = field(default_factory=time.time)


class Metrics:
    """Runtime 指标收集"""

    def __init__(self, max_latency_samples: int = 1000) -> None:
        self._lock = threading.RLock()
        self._max_latency_samples = max_latency_samples
        self._start_time = time.time()

        # Counters
        self._counters: Dict[str, int] = defaultdict(int)

        # Gauges
        self._gauges: Dict[str, float] = {}

        # Latency records
        self._latencies: Dict[str, List[LatencyRecord]] = defaultdict(list)

        # Token / cost tracking
        self._tokens: Dict[str, int] = defaultdict(int)  # model → tokens
        self._costs: Dict[str, float] = defaultdict(float)  # model → cost

    def counter_inc(self, name: str, delta: int = 1) -> None:
        with self._lock:
            self._counters[name] += delta

    def gauge_set(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def record_latency(self, name: str, value_ms: float) -> None:
        with self._lock:
            self._latencies[name].append(LatencyRecord(value_ms=value_ms))
            if len(self._latencies[name]) > self._max_latency_samples:
                self._latencies[name] = self._latencies[name][-self._max_latency_samples:]

    def record_tokens(self, model: str, tokens: int) -> None:
        with self._lock:
            self._tokens[model] += tokens

    def record_cost(self, model: str, cost: float) -> None:
        with self._lock:
            self._costs[model] += cost

    def latency_stats(self, name: str) -> Dict[str, float]:
        with self._lock:
            samples = [r.value_ms for r in self._latencies.get(name, [])]
            if not samples:
                return {"avg": 0, "p50": 0, "p95": 0, "p99": 0, "count": 0}
            sorted_samples = sorted(samples)
            n = len(sorted_samples)
            return {
                "avg": sum(sorted_samples) / n,
                "p50": sorted_samples[int(n * 0.50)],
                "p95": sorted_samples[int(n * 0.95)],
                "p99": sorted_samples[int(n * 0.99)] if n >= 100 else sorted_samples[-1],
                "count": n,
            }

    def snapshot(self) -> Dict[str, Any]:
        """获取当前完整快照"""
        with self._lock:
            uptime = time.time() - self._start_time
            total_events = self._counters.get("events.total", 0)

            return {
                "uptime_sec": uptime,
                "event_rate_per_sec": total_events / uptime if uptime > 0 else 0,
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "latency": {k: self.latency_stats(k) for k in self._latencies},
                "tokens": dict(self._tokens),
                "cost": dict(self._costs),
                "total_cost": sum(self._costs.values()),
            }
