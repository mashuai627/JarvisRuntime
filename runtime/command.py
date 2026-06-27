"""
Command — 用户层抽象。

Command 是用户与 Runtime 的接口。
Skill 发 Command，Runtime 拆 Task。

示例：
  用户: "生成西游记第10集"
  → Command(type="generate_episode", project="journey_to_the_west", episode=10)
  → Runtime 拆为：
    TASK-0100: generate_script
    TASK-0101: generate_storyboard (depends on TASK-0100)
    TASK-0102: generate_image (depends on TASK-0101)
    TASK-0103: generate_video (depends on TASK-0102)

Command 属于用户层，Task 属于 Runtime 层。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class Command:
    """用户指令"""
    id: str
    type: str  # generate_episode / analyze_code / summarize_doc / ...
    project: str = ""
    params: Dict[str, Any] = field(default_factory=dict)
    priority: str = "NORMAL"  # CRITICAL / HIGH / NORMAL / LOW / BACKGROUND

    def to_task_graph(self) -> "TaskGraph":
        """由 Skill 实现：将 Command 转换为 Task DAG"""
        raise NotImplementedError("Must be implemented by Skill")


@dataclass
class TaskGraph:
    """Task DAG — Skill 的输出"""
    command_id: str
    tasks: List[Dict[str, Any]] = field(default_factory=list)
    edges: List[tuple[str, str]] = field(default_factory=list)  # (from_task_id, to_task_id)

    def topological_order(self) -> List[str]:
        """拓扑排序"""
        in_degree: Dict[str, int] = {}
        adj: Dict[str, List[str]] = {}

        for task in self.tasks:
            tid = task["id"]
            in_degree[tid] = 0
            adj[tid] = []

        for src, dst in self.edges:
            adj.setdefault(src, []).append(dst)
            in_degree[dst] = in_degree.get(dst, 0) + 1
            in_degree.setdefault(src, 0)

        # Kahn's algorithm
        from collections import deque
        queue = deque([tid for tid, deg in in_degree.items() if deg == 0])
        result = []

        while queue:
            node = queue.popleft()
            result.append(node)
            for neighbor in adj.get(node, []):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return result


# 内置 Command 类型
COMMAND_TYPES = [
    "generate_episode",    # 生成一集动画
    "analyze_code",        # 代码分析
    "summarize_doc",       # 文档摘要
    "transcribe_audio",    # 音频转文字
    "translate_text",      # 文本翻译
    "generate_report",     # 生成报告
    "custom",              # 自定义
]
