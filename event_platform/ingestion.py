"""接入模块：按生产者声明的版本验证事件，验证通过后入库并生成投影。

不变量：
- 事件先按声明的原版本验证（不是当前可见版本），过渡期内旧版本仍可写；
- 无效事件进入隔离区，事件库与所有消费者投影都不留痕迹；
- 投影先为全部活跃消费者构建完毕，再与入库在同一临界区内一次提交，
  任何中途失败都不会留下部分投影。
"""
from __future__ import annotations

import enum
import threading
from typing import Any, Dict, List, Optional, Tuple

from .consumers import ConsumerManager
from .model import SchemaVersion
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


def build_projection(requirements: Dict[int, Any], normalized: Dict[int, Any]) -> Dict[int, Any]:
    """按消费者固定的字段 ID 抽取投影（以 ID 为键，重命名透明）。"""
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

            # 先为全部活跃消费者构建投影，全部成功后才提交 —— 不留部分投影
            try:
                projections = {
                    view.consumer_id: build_projection(view.requirements, normalized)
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
