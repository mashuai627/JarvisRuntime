"""
Milestone 1 — Runtime Kernel Integration Test

验证全部核心组件协作：
  EventBus → EventStore → Queue(Snapshot) → Registry(Capability) 
  → Scheduler → Dispatcher → Executor → HealthMonitor → Metrics → Command

Architecture Review 十大要点全覆盖。
"""

import os
import sys
import time
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from runtime import (
    EventBus, EventStore, StoredEvent,
    TaskQueue, TaskSnapshot, Priority,
    Scheduler, Dispatcher, WorkerEndpoint, Backend,
    Executor, ExecutionResult, ExecutionState,
    Registry, WorkerInfo, CapabilityInfo,
    HealthMonitor, HealthStatus,
    Metrics, Command, TaskGraph,
)


def test_event_sourcing():
    """Review 4: Event Sourcing — 状态来自事件回放"""
    print("\n[Test] Event Sourcing")
    store = EventStore(storage_path="runtime/test_store.jsonl")

    task_id = "TASK-ES-01"

    # 追加事件
    store.append(StoredEvent(task_id=task_id, event_type="task.created",
        data={"type": "generate_script", "input": {"prompt": "hello"}}))
    store.append(StoredEvent(task_id=task_id, event_type="task.queued"))
    store.append(StoredEvent(task_id=task_id, event_type="task.started", data={"worker": "ScriptWorker"}))
    store.append(StoredEvent(task_id=task_id, event_type="task.completed",
        data={"output": {"path": "script.txt"}}))

    # 回放计算状态
    state = store.get_state(task_id)
    assert state["status"] == "completed", f"Expected completed, got {state['status']}"
    assert state["output"]["path"] == "script.txt"
    assert state["version"] == 4
    assert state["worker"] == "ScriptWorker"

    # 历史版本
    v2 = store.get_task_at_version(task_id, 2)
    assert "queued" in str(v2.get("status", ""))

    print(f"  [PASS] State from replay: {state['status']} v{state['version']}")
    print(f"  [PASS] History at v2: {v2.get('status')}")

    # 清理
    os.remove("runtime/test_store.jsonl")
    return store


def test_snapshot_queue():
    """Review 3: Snapshot Queue"""
    print("\n[Test] Snapshot Queue")

    q = TaskQueue(storage_path="runtime/test_q.json")

    # CRITICAL 优先级 Task
    s1 = q.snapshot(TaskSnapshot(task_id="TASK-Q01", type="generate_script",
        state="created", priority="CRITICAL"))
    s1b = q.snapshot(TaskSnapshot(task_id="TASK-Q01", type="generate_script",
        state="queued", priority="CRITICAL"))

    # NORMAL 优先级 Task
    q.snapshot(TaskSnapshot(task_id="TASK-Q02", type="generate_image",
        state="created", priority="NORMAL"))
    q.snapshot(TaskSnapshot(task_id="TASK-Q02", type="generate_image",
        state="queued", priority="NORMAL"))

    # 快照历史
    history = q.get_history("TASK-Q01")
    assert len(history) == 2, f"Expected 2 snapshots, got {len(history)}"
    assert history[0].version == 1
    assert history[1].version == 2

    # 版本查询
    v1 = q.get_at_version("TASK-Q01", 1)
    assert v1.state == "created"

    # 就绪队列按优先级排序
    ready = q.get_ready()
    assert ready[0].task_id == "TASK-Q01", f"CRITICAL should be first, got {ready[0].task_id}"
    assert ready[1].task_id == "TASK-Q02"

    print(f"  [PASS] History length: {len(history)}")
    print(f"  [PASS] Ready order: {[r.task_id for r in ready]} (CRITICAL first)")

    os.remove("runtime/test_q.json")
    return q


def test_capability_registry():
    """Review 5: Capability Registry"""
    print("\n[Test] Capability Registry")

    bus = EventBus()
    reg = Registry(bus)

    reg.register_capability(CapabilityInfo(name="generate_script", description="Generate story scripts"))
    reg.register_capability(CapabilityInfo(name="generate_image", description="Generate images via SD/ComfyUI"))
    reg.register_capability(CapabilityInfo(name="generate_video", description="Composite video from images"))

    # 一个 capability 可以有多个 Worker
    reg.register_worker(WorkerInfo(
        name="ScriptWorkerV1", capabilities=["generate_script"],
        handler=lambda t: t))
    reg.register_worker(WorkerInfo(
        name="ScriptWorkerV2", capabilities=["generate_script"],
        handler=lambda t: t))
    reg.register_worker(WorkerInfo(
        name="ImageWorker", capabilities=["generate_image", "stable_diffusion"],
        handler=lambda t: t))

    # 一对多匹配
    script_workers = reg.find_workers_for("generate_script")
    assert len(script_workers) == 2, f"Expected 2 workers, got {len(script_workers)}"
    assert {w.name for w in script_workers} == {"ScriptWorkerV1", "ScriptWorkerV2"}

    # match 返回第一个
    best = reg.match("generate_script")
    assert best.name == "ScriptWorkerV1"

    # ImageWorker 也响应 stable_diffusion
    sd_workers = reg.find_workers_for("stable_diffusion")
    assert len(sd_workers) == 1

    print(f"  [PASS] generate_script → {[w.name for w in script_workers]}")
    print(f"  [PASS] match → {best.name}")
    return bus, reg


def test_executor_retry_timeout():
    """Review 2: Executor with timeout/retry"""
    print("\n[Test] Executor — timeout & retry")
    exec = Executor(max_retries=2, default_timeout_ms=5000)

    # 测试：立即成功的 worker
    def success_worker(ts):
        return ExecutionResult(task_id=ts.task_id, state=ExecutionState.COMPLETED,
                               output={"result": "ok"})

    snap = TaskSnapshot(task_id="TASK-EX01", type="test")
    result = exec.execute(snap, success_worker, worker_name="TestWorker")
    assert result.state == ExecutionState.COMPLETED
    assert result.output["result"] == "ok"

    # 测试：失败后重试
    call_count = [0]
    def flaky_worker(ts):
        call_count[0] += 1
        if call_count[0] < 3:
            raise RuntimeError(f"Temporary error call={call_count[0]}")
        return ExecutionResult(task_id=ts.task_id, state=ExecutionState.COMPLETED,
                               output={"retries": call_count[0]})

    snap2 = TaskSnapshot(task_id="TASK-EX02", type="test")
    result2 = exec.execute(snap2, flaky_worker, worker_name="FlakyWorker")
    assert result2.state == ExecutionState.COMPLETED, f"Expected COMPLETED, got {result2.state}: {result2.error}"
    assert call_count[0] == 3, f"Expected 3 calls (2 retries + success), got {call_count[0]}"

    print(f"  [PASS] Success: {result.state.value}")
    print(f"  [PASS] Flaky worker recovered after {call_count[0]} attempts")

    return exec


def test_lease_and_cancel():
    """Review 8: Lease + Cancellation（在重试间隙检查取消信号）"""
    print("\n[Test] Lease & Cancel")

    exec = Executor(max_retries=5)

    call_count = [0]
    def always_fails(ts):
        call_count[0] += 1
        raise RuntimeError(f"Fail #{call_count[0]}")

    snap = TaskSnapshot(task_id="TASK-LEASE", type="test", timeout_ms=10000)

    import threading
    def run_and_cancel():
        time.sleep(2)  # 等待几次重试
        exec.cancel("TASK-LEASE")

    cancel_thread = threading.Thread(target=run_and_cancel, daemon=True)
    cancel_thread.start()

    result = exec.execute(snap, always_fails, worker_name="FailingWorker")
    cancel_thread.join()

    assert result.state == ExecutionState.CANCELLED, f"Expected CANCELLED, got {result.state}"
    # 取消前已经执行了几次（重试期间）
    assert call_count[0] >= 2

    print(f"  [PASS] Cancelled after {call_count[0]} attempts: {result.state.value}")


def test_health_monitor():
    """Review 6: Health Monitor"""
    print("\n[Test] Health Monitor")

    bus = EventBus()
    hm = HealthMonitor(bus, check_interval_sec=0.5, worker_timeout_sec=1.0)

    def healthy_check():
        return HealthStatus.HEALTHY, "All good"

    hm.register_component("event_bus", healthy_check)
    hm.register_worker("ScriptWorker")
    hm.register_worker("ImageWorker")

    hm.start()
    time.sleep(1)

    status = hm.get_status()
    assert status["overall"] == "healthy"
    assert status["components"]["event_bus"]["status"] == "healthy"

    # Worker 超时
    time.sleep(2)
    status2 = hm.get_status()
    assert status2["workers"]["ScriptWorker"]["status"] == "unhealthy"
    assert status2["overall"] == "degraded"

    hm.stop()
    print(f"  [PASS] Overall: {status['overall']} → {status2['overall']}")


def test_metrics():
    """Review 9: Metrics"""
    print("\n[Test] Metrics")

    m = Metrics()
    m.counter_inc("tasks.created", 10)
    m.counter_inc("tasks.completed", 8)
    m.counter_inc("tasks.failed", 2)
    m.gauge_set("worker.busy", 3)
    m.gauge_set("queue.depth", 15)
    m.record_latency("task.execution", 120)
    m.record_latency("task.execution", 250)
    m.record_latency("task.execution", 80)
    m.record_tokens("deepseek-chat", 15000)
    m.record_cost("deepseek-chat", 0.03)

    snap = m.snapshot()
    assert snap["counters"]["tasks.created"] == 10
    assert snap["gauges"]["worker.busy"] == 3
    assert snap["tokens"]["deepseek-chat"] == 15000
    assert abs(snap["total_cost"] - 0.03) < 0.001
    assert snap["latency"]["task.execution"]["count"] == 3
    assert 80 <= snap["latency"]["task.execution"]["p50"] <= 250

    print(f"  [PASS] Counters: {snap['counters']}")
    print(f"  [PASS] Cost: ${snap['total_cost']:.2f}")
    print(f"  [PASS] Latency p50: {snap['latency']['task.execution']['p50']}ms")


def test_command_and_dag():
    """Review 10: Command → TaskGraph"""
    print("\n[Test] Command → TaskGraph")

    # 模拟 Comic Skill 拆解 Command
    cmd = Command(
        id="CMD-0001",
        type="generate_episode",
        project="journey_to_the_west",
        params={"episode": 10},
        priority="HIGH",
    )

    # Skill 生成 TaskGraph
    tasks = [
        {"id": "TASK-A01", "type": "generate_script", "input": {"episode": 10}},
        {"id": "TASK-A02", "type": "generate_storyboard", "input": {"episode": 10}},
        {"id": "TASK-A03", "type": "generate_image", "input": {"scene": 1}},
        {"id": "TASK-A04", "type": "generate_image", "input": {"scene": 2}},
        {"id": "TASK-A05", "type": "generate_video", "input": {"episode": 10}},
    ]
    edges = [
        ("TASK-A01", "TASK-A02"),
        ("TASK-A02", "TASK-A03"),
        ("TASK-A02", "TASK-A04"),
        ("TASK-A03", "TASK-A05"),
        ("TASK-A04", "TASK-A05"),
    ]

    graph = TaskGraph(command_id=cmd.id, tasks=tasks, edges=edges)
    order = graph.topological_order()

    # 验证拓扑序
    assert order.index("TASK-A01") < order.index("TASK-A02")  # TASK-A01 → TASK-A02
    assert order.index("TASK-A02") < order.index("TASK-A03")  # TASK-A02 → TASK-A03
    assert order.index("TASK-A02") < order.index("TASK-A04")  # TASK-A02 → TASK-A04
    assert order.index("TASK-A03") < order.index("TASK-A05")  # TASK-A03 → TASK-A05
    assert order.index("TASK-A04") < order.index("TASK-A05")  # TASK-A04 → TASK-A05

    print(f"  [PASS] TaskGraph: {len(tasks)} tasks, {len(edges)} edges")
    print(f"  [PASS] Topological order: {' → '.join(order)}")


def test_full_pipeline():
    """完整管线：Command → EventStore → Queue → Scheduler → Dispatcher → Executor"""
    print("\n[Test] Full Pipeline (Event-Driven)")

    # 初始化所有组件
    bus = EventBus()
    store = EventStore(storage_path="runtime/test_full.jsonl")
    store.wire_to_bus(bus)  # 自动记录所有 task.* 事件
    q = TaskQueue(storage_path="runtime/test_full_q.json")
    reg = Registry(bus)
    disp = Dispatcher()
    exec = Executor(max_retries=1, default_timeout_ms=5000)
    mets = Metrics()

    # 注册 capability
    reg.register_capability(CapabilityInfo(name="generate_script"))
    reg.register_capability(CapabilityInfo(name="generate_image"))

    # 注册 Worker
    def script_handler(ts):
        # Worker 只返回状态，不处理基础设施
        return ExecutionResult(
            task_id=ts.task_id,
            state=ExecutionState.COMPLETED,
            output={"path": f"outputs/scripts/{ts.task_id}.txt"},
        )

    def image_handler(ts):
        return ExecutionResult(
            task_id=ts.task_id,
            state=ExecutionState.COMPLETED,
            output={"path": f"outputs/images/{ts.task_id}.png"},
        )

    reg.register_worker(WorkerInfo(
        name="ScriptWorker", capabilities=["generate_script"], handler=script_handler))
    reg.register_worker(WorkerInfo(
        name="ImageWorker", capabilities=["generate_image"], handler=image_handler))

    # 注册 Dispatcher 端点
    disp.register(WorkerEndpoint(
        worker_name="ScriptWorker", backend=Backend.LOCAL,
        capabilities=["generate_script"], handler=script_handler))
    disp.register(WorkerEndpoint(
        worker_name="ImageWorker", backend=Backend.LOCAL,
        capabilities=["generate_image"], handler=image_handler))

    # 创建 Scheduler
    scheduler = Scheduler(bus, q, reg, disp, exec, metrics=mets, max_concurrent=4)

    # 创建 Task（通过 snapshot）
    task = q.snapshot(TaskSnapshot(
        task_id="TASK-F01",
        type="generate_script",
        state="created",
        input={"prompt": "Hello world"},
        priority="HIGH",
    ))

    # 事件溯源自动记录（通过 EventBus → EventStore）
    bus.publish_simple("task.created", task_id=task.task_id, type=task.type, input=task.input)

    # 入队
    q.snapshot(TaskSnapshot(task_id=task.task_id, type=task.type, state="queued", priority="HIGH"))
    bus.publish_simple("task.queued", task_id=task.task_id)

    # 启动调度
    scheduler.start()
    time.sleep(3)
    scheduler.stop()

    # 验证结果
    final_snap = q.get_latest("TASK-F01")
    assert final_snap.state == "completed", f"Expected completed, got {final_snap.state}"
    assert final_snap.output["path"].endswith(".txt")

    # 事件溯源验证
    final_state = store.get_state("TASK-F01")
    assert final_state["status"] == "completed"

    # Metrics 验证
    snap = mets.snapshot()
    assert snap["counters"]["tasks.executed"] >= 1

    print(f"  [PASS] Task completed: {final_snap.state}")
    print(f"  [PASS] EventStore state: {final_state['status']}")
    print(f"  [PASS] Metrics: {snap['counters']['tasks.executed']} tasks executed")
    print(f"  [PASS] Latency: {snap['latency'].get('task.execution', {}).get('avg', 0):.0f}ms avg")

    # 清理
    os.remove("runtime/test_full.jsonl")
    os.remove("runtime/test_full_q.json")


def main():
    print("=" * 60)
    print("Jarvis Runtime v3 — Milestone 1 Integration Test")
    print("=" * 60)

    test_event_sourcing()
    test_snapshot_queue()
    test_capability_registry()
    test_executor_retry_timeout()
    test_lease_and_cancel()
    test_health_monitor()
    test_metrics()
    test_command_and_dag()
    test_full_pipeline()

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED — Runtime Kernel Verified")
    print("=" * 60)


if __name__ == "__main__":
    main()
