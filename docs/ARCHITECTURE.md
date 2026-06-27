# Jarvis Runtime Architecture

> **Single Source of Truth**
> 本文档是 Jarvis Runtime 项目的唯一架构真相。
> 任何 Worker、Skill、Runtime 功能的新增或修改，**必须先符合本文档定义的协议，再开始开发。**

---

## 1. 项目定位

Jarvis Runtime 是一个**模型无关**的本地 AI 执行宿主。它不绑定任何单一 LLM，而是通过统一协议调度 DeepSeek、OpenAI、Claude、Gemini、Qwen 等模型，执行文件、Shell、Git、Python、浏览器等本地操作。

**核心理念**：Protocol → Runtime → Worker，而不是 Runtime → Worker → Prompt。

---

## 2. 架构分层

```
┌──────────────────────────────────────────┐
│              Skills Layer                │  ← Comic / Coding / Office ...
│         (项目级 Skill 组合)               │
├──────────────────────────────────────────┤
│              Workers Layer               │  ← ScriptWorker / ImageWorker ...
│    (每个 Worker 实现一个 accepts 类型)     │
├──────────────────────────────────────────┤
│             Runtime Layer                │  ← Task Queue / Scheduler / Logger
│    (任务调度、状态管理、错误处理)           │
├──────────────────────────────────────────┤
│             Protocol Layer               │  ← task / worker / tool / memory ...
│    (所有 Schema 定义，不可变核心)          │
├──────────────────────────────────────────┤
│              MCP Layer                   │  ← Filesystem / Shell / Git / Python
│    (本地执行能力，由 MCP Server 提供)      │
└──────────────────────────────────────────┘
```

---

## 3. 八个核心协议

| # | 协议 | Schema 文件 | 职责 |
|---|------|-----------|------|
| 1 | Task Protocol | `specs/task.schema.json` | 系统唯一驱动单元，替代 Prompt |
| 2 | Worker Protocol | `specs/worker.schema.json` | 所有 Worker 统一接口 |
| 3 | Tool Protocol | `specs/tool.schema.json` | MCP 工具统一调用格式 |
| 4 | Memory Protocol | `specs/memory.schema.json` | 角色/世界观/风格持久化 |
| 5 | Asset Protocol | `specs/asset.schema.json` | 所有产出物统一管理 |
| 6 | Runtime Protocol | `specs/runtime.schema.json` | Task Queue / Scheduler / Logger |
| 7 | Script Protocol | `specs/script.schema.json` | 剧本输出结构 |
| 8 | Storyboard Protocol | `specs/storyboard.schema.json` | 分镜输出结构 |

---

## 4. Task 生命周期

```
pending → queued → running → finished → review_pending → finished
                   ↘ failed   ↘ rejected
```

- **pending**: 任务创建，等待入队
- **queued**: 已进入 Task Queue，等待 Worker 接管
- **running**: Worker 正在执行
- **finished**: 执行成功，output 已写入
- **failed**: 执行失败，error 字段说明原因
- **review_pending**: 等待 ReviewWorker 审核
- **rejected**: 审核不通过，需重新执行

---

## 5. Worker 规范

### 5.1 强制规则

1. **禁止自然语言输出**。Worker 只能返回结构化 JSON。
2. **必须声明 accepts**。Worker 必须明确自己能处理的 task type 列表。
3. **必须声明 returns**。Worker 必须声明输出文件名模板。
4. **输出必须可验证**。所有 Worker 输出必须能通过对应的 Schema 校验。

### 5.2 Worker 输出格式

```json
{
  "task_id": "TASK-0001",
  "status": "finished",
  "output": {
    "path": "projects/xiyouji/ep01/script.json",
    "format": "script.schema.json",
    "size": 4096,
    "checksum": "sha256:abc123"
  },
  "duration_ms": 3200
}
```

---

## 6. 目录结构

```text
JarvisRuntime/
├── specs/               # 协议定义（不可变核心）
│   ├── task.schema.json
│   ├── worker.schema.json
│   ├── tool.schema.json
│   ├── memory.schema.json
│   ├── asset.schema.json
│   ├── runtime.schema.json
│   ├── script.schema.json
│   └── storyboard.schema.json
├── runtime/             # Runtime 实现
│   ├── scheduler.py
│   ├── task_queue.py
│   └── logger.py
├── workers/             # Worker 实现
│   ├── ScriptWorker/
│   ├── StoryboardWorker/
│   ├── ImageWorker/
│   ├── VideoWorker/
│   ├── AudioWorker/
│   └── ReviewWorker/
├── agents/              # AI Agent 配置
├── memory/              # 记忆存储
├── skills/              # Skill 定义
├── projects/            # 用户项目
│   └── {project_name}/
│       ├── project.json
│       ├── ep01/
│       └── assets/
├── outputs/             # 最终成品
├── logs/                # 运行日志
├── tests/               # 测试
└── docs/                # 文档
    └── ARCHITECTURE.md
```

---

## 7. 技术决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 包管理器 | uv | 速度最快，Rust 实现，与 Python 生态兼容 |
| 配置格式 | JSON | 所有协议统一 JSON Schema，可自动校验 |
| 任务驱动 | Task 替代 Prompt | 消除自然语言歧义，所有 Worker 输入输出可结构化 |
| 模型无关 | 多模型适配 | DeepSeek/OpenAI/Claude/Gemini/Qwen 均可接入 |
| Worker 隔离 | 每个 Worker 独立目录 | 避免依赖污染，方便单独测试和替换 |
| MCP | 本地执行能力 | Filesystem/Shell/Git/Python 通过 MCP 统一暴露 |

---

## 8. 开发原则

1. **Protocol First**: 先定义 Schema，再写代码
2. **Worker Silent**: Worker 不说话，只输出 JSON
3. **Review Required**: 关键产出必须经过 ReviewWorker
4. **Asset Traceable**: 每个 Asset 记录生成者 Worker 和源 Task ID
5. **No Model Lock-in**: 任何 Worker 应支持至少 2 个模型后端

---

*最后更新：2026-06-28*
*Chief Architect: Jarvis Runtime Team*
