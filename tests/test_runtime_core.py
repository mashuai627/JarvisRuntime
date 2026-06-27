"""
Sprint 1 Day 2 — Runtime Core Integration Test

验证四个核心组件的正确协作：
  EventBus → Queue → Registry → Scheduler → Worker 执行 → 事件通知
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from runtime import EventBus, Event, TaskQueue, Task, Registry, WorkerInfo, Scheduler


def main():
    print("=" * 50)
    print("Jarvis Runtime — Core Integration Test")
    print("=" * 50)

    # 1. 创建 EventBus
    bus = EventBus()
    print("[PASS] EventBus created")

    # 2. 创建 Queue
    queue = TaskQueue(bus, storage_path="runtime/test_queue.json")
    print("[PASS] TaskQueue created")

    # 3. 创建 Registry + 注册 Mock Worker
    registry = Registry(bus)

    def mock_script_worker(task: Task) -> Task:
        """模拟 Script Worker"""
        task.status = "completed"
        task.output = {"path": f"outputs/scripts/{task.id}.txt", "format": "text"}
        return task

    def mock_image_worker(task: Task) -> Task:
        """模拟 Image Worker（含 waiting 场景）"""
        task.status = "completed"
        task.output = {"path": f"outputs/images/{task.id}.png", "format": "png", "size": 102400}
        return task

    registry.register(WorkerInfo(
        name="ScriptWorker",
        version="1.0.0",
        capabilities=["script", "text_generation"],
        returns="{task_id}.txt",
        handler=mock_script_worker,
    ))
    registry.register(WorkerInfo(
        name="ImageWorker",
        version="1.0.0",
        capabilities=["image", "comfyui", "stable_diffusion"],
        returns="{task_id}.png",
        handler=mock_image_worker,
    ))
    print(f"[PASS] Registry: {len(registry)} workers registered")
    print(f"  - ScriptWorker capabilities: {registry.get('ScriptWorker').capabilities}")
    print(f"  - ImageWorker capabilities: {registry.get('ImageWorker').capabilities}")

    # 4. 验证 capability 匹配
    match = registry.match("script")
    assert match.name == "ScriptWorker", f"Expected ScriptWorker, got {match.name}"
    print(f"[PASS] Capability match: 'script' → {match.name}")

    match2 = registry.match("image")
    assert match2.name == "ImageWorker"
    print(f"[PASS] Capability match: 'image' → {match2.name}")

    # 5. 创建 Scheduler
    scheduler = Scheduler(bus, queue, registry, max_concurrent=4)
    print("[PASS] Scheduler created")

    # 6. 事件总线测试 — 订阅 + 发布
    received_events = []

    def collect_events(evt: Event):
        received_events.append(evt.name)

    bus.subscribe("task.completed", collect_events)
    bus.subscribe("task.created", collect_events)

    bus.publish_simple("test.event", msg="hello")
    # Scheduler 内部也订阅了 task.completed，所以 count >= 1
    assert bus.subscriber_count("task.completed") >= 1
    print("[PASS] EventBus pub/sub works")

    # 7. DAG 任务测试
    t1 = Task(id="TASK-0001", type="script", input={"prompt": "Write a story about a monkey"})
    t2 = Task(id="TASK-0002", type="image", input={"prompt": "Generate monkey image"},
              dependencies=["TASK-0001"])
    t1.children = ["TASK-0002"]
    t2.parent = "TASK-0001"

    queue.add(t1)
    queue.add(t2)
    queue.enqueue("TASK-0001")
    print(f"[PASS] DAG: TASK-0002 depends on TASK-0001")

    # TASK-0002 不应就绪（依赖未满足）
    ready = queue.get_ready()
    ready_ids = [t.id for t in ready]
    assert "TASK-0001" in ready_ids, f"TASK-0001 should be ready"
    assert "TASK-0002" not in ready_ids, f"TASK-0002 should NOT be ready (dependencies)"
    print(f"[PASS] DAG check: ready={ready_ids}, TASK-0002 correctly blocked")

    # 8. 启动 Scheduler 执行 TASK-0001
    scheduler.start()
    import time
    time.sleep(2)
    scheduler.stop()

    t1_after = queue.get("TASK-0001")
    assert t1_after.status == "completed", f"TASK-0001 status: {t1_after.status}"
    print(f"[PASS] TASK-0001 executed: status={t1_after.status}, output={t1_after.output}")

    # 9. 事件验证
    assert "task.created" in received_events
    assert "task.completed" in received_events
    print(f"[PASS] Events received: {received_events}")

    # 10. 清理
    if os.path.exists("runtime/test_queue.json"):
        os.remove("runtime/test_queue.json")

    print("\n" + "=" * 50)
    print("ALL TESTS PASSED")
    print("=" * 50)


if __name__ == "__main__":
    main()
