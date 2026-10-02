"""平台门面：把注册、接入、投影三个模块接到同一把锁上。

跨模块操作（发布时检查消费者、入库时生成投影）都在共享的 RLock
临界区内完成，保证对外呈现的状态永远一致。
"""
from __future__ import annotations

import threading

from .consumers import ConsumerManager
from .ingestion import IngestResult, IngestionEngine
from .registry import Proposal, SchemaRegistry


class EventPlatform:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.registry = SchemaRegistry(self._lock)
        self.consumers = ConsumerManager(self._lock, self.registry)
        self.ingestion = IngestionEngine(self._lock, self.registry, self.consumers)

    # ---------- 模式生命周期（自动带上仍活跃的消费者做检查） ----------

    def begin_proposal(self) -> Proposal:
        return self.registry.begin_proposal()

    def publish(self, proposal: Proposal) -> int:
        with self._lock:
            return self.registry.publish(proposal, self.consumers.active_views())

    def rollback(self, target_version: int) -> int:
        with self._lock:
            return self.registry.rollback(target_version, self.consumers.active_views())

    # ---------- 接入 ----------

    def ingest(self, event_id: str, producer_id: str, schema_version: int, payload) -> IngestResult:
        return self.ingestion.ingest(event_id, producer_id, schema_version, payload)
