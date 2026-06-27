# Jarvis Runtime Architecture (v2)

> **Single Source of Truth** — 本文档是 Jarvis Runtime 的**唯一权威架构定义**。
> 任何代码、文档、讨论与此不一致时，以此为准。
>
> **Chief Architect Review 10/10** — 2026-06-28

---

## 1. 设计哲学

### 1.1 核心原则

| 原则 | 说明 |
|------|------|
| **Protocol First** | 协议定义在前，代码实现在后。所有组件先定 Schema，再写实现 |
| **Event Driven** | 组件间通过 Event Bus 发布/订阅通信，禁止直接函数调用 |
| **抽象不绑定** | Tool 不绑定 MCP；Runtime 不知业务；Skill 只是 Task Graph |
| **事实与 Prompt 分离** | Memory 只存事实（角色/世界/资产）；Prompt 永远即时生成 |
| **Hash 一致性** | 所有 Asset 基于 SHA256 hash 校验，不依赖文件名 |

### 1.2 禁止事项

- ❌ Worker 输出自然语言（只输出 JSON）
- ❌ Runtime 知道 Comic/西游记/图片/视频
- ❌ Tool 绑定 MCP
- ❌ Queue/Scheduler/Registry 职责交叉
- ❌ Memory 存 Prompt
- ❌ 组件间直接函数调用

---

## 2. 五层架构

```
┌─────────────────────────────────┐
│  Skills (Task Graph / Workflow) │  ← 业务层：Comic/Coding/Office...
├─────────────────────────────────┤
│  Workers (协议执行体)            │  ← capabilities 驱动
├─────────────────────────────────┤
│  Runtime Core                    │
│  ┌───────────────────────────┐  │
│  │  EventBus (事件总线)       │  │  ← 核心通信基础设施
│  │  Scheduler (调度器)        │  │  ← 派发 Task 到 Worker
│  │  Registry (注册中心)       │  │  ← Worker 注册与能力发现
│  │  Queue (任务队列)          │  │  ← 任务持久化 + DAG 依赖
│  └───────────────────────────┘  │
├─────────────────────────────────┤
│  Protocol (8 个 Schema)          │  ← JSON Schema draft-2020-12
├─────────────────────────────────┤
│  Adapters (MCP/Local/Docker/SSH) │  ← Tool 后端适配器
└─────────────────────────────────┘
```

---

## 3. 八个核心协议 (v2)

### 3.1 Task — 系统唯一驱动单元

- **状态生命周期**: `created → queued → running → waiting↔reviewing → completed/failed/cancelled`
- **waiting 场景**: 等待 ComfyUI / API / 人工确认 / 外部服务
- **reviewing 场景**: 等待人工审核
- **DAG 支持**: `dependencies[]`, `children[]`, `parent`
- **Worker 不写死**: 通过 `type` 字段 → Registry `capabilities` 自动匹配

### 3.2 Worker — 协议执行体

- **capabilities**: `["script", "image", "video", ...]` — Registry 自动匹配
- **Worker 禁止自然语言输出**，只能用结构化 JSON
- 每个 Worker 声明 `returns`（输出格式）、`model`（模型）、`events`（发布/订阅）

### 3.3 Tool — 抽象工具协议

- **不绑定 MCP**。工具标识格式 `namespace.action`
- 后端适配器：`mcp / local / docker / ssh / http`
- 上层只看到 Tool，下层适配器负责转发

### 3.4 Memory — 事实记忆

- **只存事实**: character / world / asset_catalog / episode / location / relationship / timeline / rule
- **不存 Prompt**。Prompt 永远即时生成
- 所有 Memory 按 project 组织

### 3.5 Asset — 统一资源协议

- **必含 `hash`**（SHA256），用于一致性校验和去重
- 支持版本标识（如 `MonkeyKing_v1`）
- 记录 `generated_by`（生成 Worker）和 `source_task_id`

### 3.6 Runtime — 核心配置

- Runtime 只知 Task，不知业务
- 配置：EventBus backend（memory/redis/mqtt）、Queue 参数、Scheduler 策略、Registry 路径、Adapters

### 3.7 Script — 故事/剧本

### 3.8 Storyboard — 分镜

---

## 4. Runtime Core 四大组件

### 4.1 Event Bus

```
Skill
  │ task.created
  ▼
EventBus ─── publish ───► Scheduler.on_task_created()
  │
  │ task.started
  ▼
Worker ─── 执行完毕 ───► publish(task.completed)
  │
  │ Review 订阅 task.completed ─► 继续流程
  ▼
```

- **backend**: memory（默认）/ redis / mqtt
- 所有组件通过 `subscribe(name, callback)` 解耦
- 异常隔离：单个订阅者崩溃不影响其他订阅者

### 4.2 Queue

- 只负责**任务存储**
- 支持 DAG 依赖检查（`_dependencies_satisfied`）
- 持久化：JSON（可替换为 SQLite）
- 接口：`add / enqueue / get / get_ready / update_status`

### 4.3 Registry

- 只负责**Worker 注册与发现**
- `capabilities` 反向索引：`{"script": ["ScriptWorker", ...]}`
- `match(task_type)` — 自动匹配
- `auto_discover(path)` — 扫描 workers/ 目录

### 4.4 Scheduler

- 只负责**派发**
- 从 Queue 取就绪任务 → Registry 匹配 Worker → 执行
- 订阅 `task.created` 自动入队
- 订阅 `task.completed` 自动唤醒子任务 (DAG)
- 线程池执行，`max_concurrent` 控制并发

---

## 5. 目录结构

```
JarvisRuntime/
├── docs/
│   └── ARCHITECTURE.md         ← 本文档（唯一权威）
├── specs/                      ← 8 个 JSON Schema
│   ├── task.schema.json
│   ├── worker.schema.json
│   ├── tool.schema.json
│   ├── memory.schema.json
│   ├── asset.schema.json
│   ├── runtime.schema.json
│   ├── script.schema.json
│   └── storyboard.schema.json
├── runtime/                    ← Runtime Core
│   ├── __init__.py
│   ├── event_bus.py            ← 事件总线
│   ├── queue.py                ← 任务队列
│   ├── registry.py             ← Worker 注册中心
│   ├── scheduler.py            ← 任务调度器
│   └── queue.json              ← 持久化文件（runtime）
├── workers/                    ← Worker 实现（待填充）
├── agents/
├── memory/
├── skills/
├── projects/
├── outputs/
├── logs/
└── tests/
    └── test_runtime_core.py    ← 集成测试
```

---

## 6. 技术决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 协议格式 | JSON Schema draft-2020-12 | 国际标准，跨语言验证 |
| 事件总线 | 内存（可替换 Redis/MQTT） | 单进程起步，接口统一 |
| 任务存储 | JSON 文件 | 零依赖，可替换 SQLite |
| 并发模型 | threading + 线程池 | Python 标准库，I/O 密集场景 |
| Worker 匹配 | capabilities 反向索引 | O(1) 查找，自动发现 |
| Tool 抽象 | namespace.action | 不绑定实现，后端可插拔 |

---

## 7. Sprint 开发原则

每个 Sprint 的验收标准：

| Review 维度 | 标准 |
|-------------|------|
| Architecture | 架构是否能支撑 50 个 Worker？ |
| Protocol | 协议是否允许扩展而不破坏兼容性？ |
| Extensibility | 增加新 Worker 是否需要改 Runtime？ |
| Failure | 单组件崩溃是否影响其他组件？ |
| Testability | 每个组件是否可以独立测试？ |

---

## 8. Changelog

### v2 (2026-06-28) — Chief Architect Review
- Task: 生命周期改为 8 状态 + DAG 支持 (dependencies/children/parent)
- Worker: 增加 capabilities 字段
- Tool: 抽象化，不绑定 MCP，增加适配器层
- Memory: 只存事实，不存 Prompt
- Asset: 增加 hash 字段
- Runtime: 新增 EventBus / Queue / Scheduler / Registry 四组件
- 确立事件驱动架构

### v1 (2026-06-27) — Sprint 1 Day 1
- 初始协议：8 个 Schema
- 五层架构定义
- Task/Worker/Tool/Memory/Asset/Runtime/Script/Storyboard
