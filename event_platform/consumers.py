"""消费者投影模块：消费者固定所需字段 ID 与类型，按各自视图读取事件。

- 注册/升级时，消费者固定的 {字段 ID: 类型} 会与**所有仍接受写入的版本**核对
  （不只可见版本）：过渡期内任何旧版有效事件都必须能投影出类型相符的值，
  否则升级本身就会制造"接入成功、投影空值"的矛盾，必须在升级处拒绝；
- 投影以字段 ID 为键 —— 显示名称重命名对消费者完全透明；
- 投影日志按事件逐条追加，由接入模块在验证通过后一次性提交。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Dict, List

from .model import CompatibilityError, FieldType, SchemaError
from .registry import ConsumerRequirements, SchemaRegistry, find_transition_problems


@dataclass(frozen=True)
class ProjectedEvent:
    """某个消费者视角下的一条事件。"""

    event_id: str
    schema_version: int  # 事件写入时声明并验证的版本
    data: Dict[int, Any]  # 以字段 ID 为键的投影数据


@dataclass(frozen=True)
class _PlainRequirements:
    """把一组待注册的 {字段 ID: 类型} 包装成注册中心检查所需的消费者形状。"""

    consumer_id: str
    requirements: Dict[int, FieldType]


class Consumer:
    def __init__(self, consumer_id: str, requirements: Dict[int, FieldType]):
        self.consumer_id = consumer_id
        self.requirements: Dict[int, FieldType] = dict(requirements)
        self.active = True
        self.projection_log: List[ProjectedEvent] = []


class ConsumerManager:
    def __init__(self, lock: threading.RLock, registry: SchemaRegistry):
        self._lock = lock
        self._registry = registry
        self._consumers: Dict[str, Consumer] = {}

    # ---------- 内部 ----------

    def _require(self, consumer_id: str) -> Consumer:
        try:
            return self._consumers[consumer_id]
        except KeyError:
            raise SchemaError(f"消费者 {consumer_id!r} 不存在") from None

    def _validate_requirements(self, requirements: Dict[int, FieldType]) -> None:
        """固定的字段与类型必须在整个过渡期内都可满足。

        不只核对当前可见版本：注册/升级后，任何仍接受写入的旧版本事件都可能到达，
        它们投影到该消费者时也必须与其固定类型相符（缺字段时要么能由可见版本的
        默认值补齐，要么消费者固定的本就是允许缺席的可选字段）。否则升级动作本身
        就会制造"接入成功但投影空值"的矛盾，必须在此拒绝。
        """
        if not requirements:
            raise SchemaError("消费者至少需要固定一个字段")
        illegal = [
            f"字段 {fid} 固定的类型非法: {pinned!r}"
            for fid, pinned in requirements.items()
            if not isinstance(pinned, FieldType)
        ]
        if illegal:
            raise SchemaError("；".join(illegal))
        current = self._registry.current()
        view: ConsumerRequirements = _PlainRequirements("(new)", requirements)
        problems = find_transition_problems(
            current, self._registry.accepting_versions(), [view]
        )
        if problems:
            raise CompatibilityError("；".join(problems))

    # ---------- 生命周期 ----------

    def register(self, consumer_id: str, requirements: Dict[int, FieldType]) -> Consumer:
        with self._lock:
            if consumer_id in self._consumers:
                raise SchemaError(f"消费者 {consumer_id!r} 已注册")
            self._validate_requirements(requirements)
            c = Consumer(consumer_id, requirements)
            self._consumers[consumer_id] = c
            return c

    def upgrade(self, consumer_id: str, new_requirements: Dict[int, FieldType]) -> None:
        """消费者升级：原子替换其固定的字段视图（按全部接受写入版本校验）。"""
        with self._lock:
            c = self._require(consumer_id)
            if not c.active:
                raise SchemaError(f"消费者 {consumer_id!r} 已注销，不能升级")
            self._validate_requirements(new_requirements)
            c.requirements = dict(new_requirements)

    def deregister(self, consumer_id: str) -> None:
        """注销后不再阻塞发布/回退，也不再接收投影。"""
        with self._lock:
            self._require(consumer_id).active = False

    # ---------- 投影 ----------

    def active_views(self) -> List[Consumer]:
        """仍活跃的消费者（注册中心据此做发布/回退检查）。"""
        with self._lock:
            return [c for c in self._consumers.values() if c.active]

    def commit_projections(
        self, event_id: str, schema_version: int, projections: Dict[str, Dict[int, Any]]
    ) -> None:
        """一次性提交全部活跃消费者的投影。

        调用方（接入模块）必须已把全部投影构建完毕；这里只做追加，
        与事件入库在同一把锁内完成，保证不留下部分投影。
        """
        with self._lock:
            for cid, data in projections.items():
                self._consumers[cid].projection_log.append(
                    ProjectedEvent(event_id, schema_version, data)
                )

    def projections_of(self, consumer_id: str) -> List[ProjectedEvent]:
        with self._lock:
            return list(self._require(consumer_id).projection_log)
