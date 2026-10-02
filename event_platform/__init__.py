"""事件模式平台：模式注册 + 事件接入 + 消费者投影。"""
from .consumers import Consumer, ConsumerManager, ProjectedEvent
from .ingestion import (
    IngestionEngine,
    IngestResult,
    IngestStatus,
    QuarantineRecord,
    StoredEvent,
)
from .model import (
    CompatibilityError,
    Field,
    FieldType,
    SchemaError,
    SchemaVersion,
    ValidationError,
)
from .platform import EventPlatform
from .registry import Proposal, SchemaRegistry, find_compatibility_problems

__all__ = [
    "CompatibilityError",
    "Consumer",
    "ConsumerManager",
    "EventPlatform",
    "Field",
    "FieldType",
    "IngestionEngine",
    "IngestResult",
    "IngestStatus",
    "ProjectedEvent",
    "Proposal",
    "QuarantineRecord",
    "SchemaError",
    "SchemaRegistry",
    "SchemaVersion",
    "StoredEvent",
    "ValidationError",
    "find_compatibility_problems",
]
