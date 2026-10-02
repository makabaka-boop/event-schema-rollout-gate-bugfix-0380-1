"""接入模块：按生产者声明的版本验证事件，验证通过后入库并生成投影。

不变量：
- 事件先按声明的原版本验证（不是当前可见版本），过渡期内旧版本仍可写；
- 无效事件进入隔离区，事件库与所有消费者投影都不留痕迹；
- 投影按消费者固定的 {字段 ID: 类型} 解析：事件版本里有的值直接取值；
  事件版本缺失但可见版本为可选字段的槽位，用可见版本默认值补齐
  （无默认值则为 None）；必需字段无法补齐说明绕过了闸门，隔离该事件；
- 投影先为全部活跃消费者构建完毕，再与入库在同一临界区内一次提交，
  任何中途失败都不会留下部分投影。
"""
from __future__ import annotations

import enum
import threading
from typing import Any, Dict, List, Optional, Tuple

from .consumers import ConsumerManager
from .model import FieldType, SchemaVersion
from .registry import SchemaRegistry


class IngestStatus(enum.Enum):
    ACCEPTED = "accepted"
    QUARANTINED = "quarantined"


class StoredEvent:
    def __init__(self, event_id: str, producer_id: str, schema_version: int, payload: Dict[int, Any]):
        self.event_id = event_id
        self.producer_id = producer_id
        self.schema_version = schema_version
        self.payload = payload  # 已套用默认值的规范化负载

    def __repr__(self) -> str:  # pragma: no cover
        return f"StoredEvent({self.event_id!r}, v{self.schema_version}, {self.payload!r})"


class QuarantineRecord:
    def __init__(self, event_id: str, producer_id: str, schema_version: int,
                 payload: Dict[int, Any], reason: str):
        self.event_id = event_id
        self.producer_id = producer_id
        self.schema_version = schema_version
        self.payload = payload
        self.reason = reason

    def __repr__(self) -> str:  # pragma: no cover
        return f"QuarantineRecord({self.event_id!r}, v{self.schema_version}, {self.reason!r})"


class IngestResult:
    def __init__(self, status: IngestStatus, event_id: str, reason: Optional[str] = None):
        self.status = status
        self.event_id = event_id
        self.reason = reason

    @property
    def accepted(self) -> bool:
        return self.status is IngestStatus.ACCEPTED

    def __repr__(self) -> str:  # pragma: no cover
        return f"IngestResult({self.status.value}, {self.event_id!r}, {self.reason!r})"


def resolve_projection_view(
    sv: SchemaVersion,
    requirements: Dict[int, FieldType],
    normalized: Dict[int, Any],
    visible: SchemaVersion,
) -> Dict[int, Any]:
    """为一个消费者解析投影，保证投影值与它固定的类型不矛盾。

    事件按声明版本 ``sv`` 验证，``normalized`` 里只有该版本认识的值/默认值；
    消费者可能已升级去读更新版本（``visible``）才有的字段。对每个固定字段：

    1. 事件声明版本里就有 -> 直接取值（注册/发布/升级三重闸门保证类型已相符）；
    2. 事件声明版本里没有：
       - 可见版本提供非空默认值 -> 用默认值补齐（可选字段跨版本默认值）；
       - 可见版本中该字段为无默认值的可选字段 -> 投影 None（允许缺席）；
       - 可见版本中为必需字段 -> 不可能到达：闸门会拒绝这种注册/发布，
         此处作为防御性兜底抛出 TypeError 以触发隔离，绝不入库一个矛盾投影。
    """
    data: Dict[int, Any] = build_projection(requirements, normalized)
    for fid, pinned in requirements.items():
        if fid in normalized:
            value = data[fid]
            if value is not None and not pinned.accepts(value):
                raise TypeError(
                    f"字段 {fid} 的值 {value!r} 与消费者固定类型 {pinned.value} 不兼容"
                )
            continue
        visible_field = visible.fields.get(fid)
        if visible_field is not None and visible_field.default is not None:
            data[fid] = visible_field.default
        elif visible_field is not None and not visible_field.required:
            data[fid] = None
        else:
            # 不应发生（见闸门）；抛出后由接入临界区隔离该事件
            raise TypeError(
                f"字段 {fid} 在事件声明版本 v{sv.version} 中缺失，"
                f"且可见版本 v{visible.version} 中无默认值可补齐"
            )
    return data


def build_projection(requirements: Dict[int, Any], normalized: Dict[int, Any]) -> Dict[int, Any]:
    """按消费者固定的字段 ID 抽取投影（以 ID 为键，重命名透明）。

    保留供单版本场景直接使用；跨版本投影请用 :func:`resolve_projection_view`。
    """
    return {fid: normalized.get(fid) for fid in requirements}


class IngestionEngine:
    def __init__(self, lock: threading.RLock, registry: SchemaRegistry, consumers: ConsumerManager):
        self._lock = lock
        self._registry = registry
        self._consumers = consumers
        self._store: List[StoredEvent] = []
        self._quarantined: List[QuarantineRecord] = []
        self._seen_ids: set[str] = set()

    # ---------- 查询 ----------

    @property
    def stored_events(self) -> Tuple[StoredEvent, ...]:
        with self._lock:
            return tuple(self._store)

    @property
    def quarantined_events(self) -> Tuple[QuarantineRecord, ...]:
        with self._lock:
            return tuple(self._quarantined)

    # ---------- 验证 ----------

    @staticmethod
    def validate_event(sv: SchemaVersion, payload: Dict[int, Any]) -> Tuple[List[str], Dict[int, Any]]:
        """按声明版本验证负载。返回 (错误列表, 套用默认值后的规范化负载)。"""
        errors: List[str] = []
        normalized: Dict[int, Any] = {}
        for key in payload:
            if isinstance(key, bool) or not isinstance(key, int) or key not in sv.fields:
                errors.append(f"版本 v{sv.version} 中不存在字段 {key!r}")
        for fid, f in sv.fields.items():
            if fid in payload:
                value = payload[fid]
                if not f.type.accepts(value):
                    errors.append(
                        f"字段 {fid}({f.name}) 期望 {f.type.value}，实际值 {value!r}"
                    )
                else:
                    normalized[fid] = value
            elif f.required:
                errors.append(f"缺少必需字段 {fid}({f.name})")
            elif f.default is not None:
                normalized[fid] = f.default
        return errors, normalized

    # ---------- 接入 ----------

    def ingest(self, event_id: str, producer_id: str, schema_version: int,
               payload: Dict[int, Any]) -> IngestResult:
        with self._lock:
            if event_id in self._seen_ids:
                raise ValueError(f"重复事件 ID: {event_id!r}")
            self._seen_ids.add(event_id)  # 事件 ID 只消费一次，无论接受还是隔离

            sv = self._registry.get_version(schema_version)
            if sv is None or not self._registry.is_accepting(schema_version):
                return self._quarantine(
                    event_id, producer_id, schema_version, payload,
                    f"模式版本 v{schema_version} 不存在或已弃用",
                )

            errors, normalized = self.validate_event(sv, payload)
            if errors:
                return self._quarantine(
                    event_id, producer_id, schema_version, payload, "；".join(errors)
                )

            # 先为全部活跃消费者构建投影，全部成功后才提交 —— 不留部分投影。
            # 可见版本用于把旧版事件缺失、但新版可选字段带默认值的槽位补齐。
            visible = self._registry.current()
            try:
                projections = {
                    view.consumer_id: resolve_projection_view(
                        sv, view.requirements, normalized, visible
                    )
                    for view in self._consumers.active_views()
                }
            except Exception as exc:
                return self._quarantine(
                    event_id, producer_id, schema_version, payload,
                    f"投影构建失败: {exc}",
                )

            # 原子提交：入库与所有消费者投影在同一临界区内完成
            self._store.append(StoredEvent(event_id, producer_id, schema_version, normalized))
            self._consumers.commit_projections(event_id, schema_version, projections)
            return IngestResult(IngestStatus.ACCEPTED, event_id)

    # ---------- 内部 ----------

    def _quarantine(self, event_id: str, producer_id: str, schema_version: int,
                    payload: Dict[int, Any], reason: str) -> IngestResult:
        self._quarantined.append(
            QuarantineRecord(event_id, producer_id, schema_version, dict(payload), reason)
        )
        return IngestResult(IngestStatus.QUARANTINED, event_id, reason)
