# Jarvis Runtime

> **An Event-Driven AI Agent Operating Kernel**
>
> 承载漫剧、编程、办公、研究等任意 Skill 的 AI Agent 运行时内核。

---

## 设计哲学

- **Protocol First** — 先定协议，再写代码
- **Event Sourcing** — 状态来自事件回放，不存可变状态
- **Command → Task** — 用户说 Command，Runtime 拆 Task
- **Capability 驱动** — 注册能力而非 Worker，一个能力可多 Worker 实现
- **抽象不绑定** — Tool 不绑定 MCP，Worker 不绑定执行环境

---

## 架构

```
Skills (Task Graph)
  │
  ▼
Command ──► Runtime Kernel ──► Workers
              │
              ├─ EventBus      (pub/sub)
              ├─ EventStore    (event sourcing)
              ├─ Queue         (task snapshots)
              ├─ Scheduler     (when to run)
              ├─ Dispatcher    (where to run)
              ├─ Executor      (timeout/retry/cancel)
              ├─ Registry      (capability matching)
              ├─ HealthMonitor (liveness checks)
              └─ Metrics       (observability)
```

---

## Milestones

| Milestone | 内容 | 状态 |
|-----------|------|------|
| M1 | Runtime Kernel (6 核心组件) | 🚧 Day 2 |
| M2 | Worker SDK | 待启动 |
| M3 | Comic Skill v1 | 待启动 |

---

## 目录

```
JarvisRuntime/
├── docs/ARCHITECTURE.md
├── specs/          # 8 JSON Schema
├── runtime/        # Kernel
│   ├── event_bus.py
│   ├── event_store.py
│   ├── queue.py
│   ├── scheduler.py
│   ├── dispatcher.py
│   ├── executor.py
│   ├── registry.py
│   ├── health_monitor.py
│   ├── metrics.py
│   └── command.py
├── workers/
├── skills/
├── agents/
├── memory/
├── projects/
├── outputs/
└── tests/
```
