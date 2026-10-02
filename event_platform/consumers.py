"""消费者投影模块：消费者固定所需字段 ID 与类型，按各自视图读取事件。

- 注册/升级时，消费者固定的 {字段 ID: 类型} 会与当前可见版本核对；
- 投影以字段 ID 为键 —— 显示名称重命名对消费者完全透明；
- 投影日志按事件逐条追加，由接入模块在验证通过后一次性提交。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .model import FieldType, SchemaError
from .registry import SchemaRegistry


@dataclass(frozen=True)
class ProjectedEvent:
    """某个消费者视角下的一条事件。"""

    event_id: str
    schema_version: int  # 事件写入时声明并验证的版本
    data: Dict[int, Any]  # 以字段 ID 为键的投影数据


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
        """固定的字段与类型必须能对上当前可见版本。"""
        if not requirements:
            raise SchemaError("消费者至少需要固定一个字段")
        current = self._registry.current()
        problems = []
        for fid, pinned in requirements.items():
            if not isinstance(pinned, FieldType):
                problems.append(f"字段 {fid} 固定的类型非法: {pinned!r}")
                continue
            f = current.fields.get(fid)
            if f is None:
                problems.append(f"字段 {fid} 在当前版本 v{current.version} 中不存在")
            elif f.type is not pinned:
                problems.append(
                    f"字段 {fid} 固定类型 {pinned.value} 与当前类型 {f.type.value} 不符"
                )
        if problems:
            raise SchemaError("；".join(problems))

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
        """消费者升级：原子替换其固定的字段视图（按当前可见版本校验）。"""
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
