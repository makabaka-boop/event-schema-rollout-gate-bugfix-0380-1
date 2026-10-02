"""基础数据模型：字段类型、字段定义与模式版本。

核心不变量：字段身份由稳定数字 ID 决定；显示名称仅用于展示，
重命名显示名称不改变字段身份。
"""
from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Any, Dict


class SchemaError(Exception):
    """模式相关错误的基类。"""


class ValidationError(SchemaError):
    """字段或模式自身不合法（默认值与类型不符、ID 非法等）。"""


class CompatibilityError(SchemaError):
    """新模式/回退目标与仍活跃消费者固定的字段不兼容。"""


class FieldType(enum.Enum):
    INT = "int"
    FLOAT = "float"
    STRING = "string"
    BOOL = "bool"

    def accepts(self, value: Any) -> bool:
        # 注意：Python 中 bool 是 int 的子类，必须单独排除
        if self is FieldType.BOOL:
            return isinstance(value, bool)
        if self is FieldType.INT:
            return isinstance(value, int) and not isinstance(value, bool)
        if self is FieldType.FLOAT:
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        if self is FieldType.STRING:
            return isinstance(value, str)
        return False  # pragma: no cover


@dataclass(frozen=True)
class Field:
    """一个字段：稳定数字 ID + 显示名称 + 类型 + 是否必需 + 默认值。"""

    field_id: int
    name: str
    type: FieldType
    required: bool = False
    default: Any = None

    def __post_init__(self) -> None:
        if isinstance(self.field_id, bool) or not isinstance(self.field_id, int) or self.field_id < 0:
            raise ValidationError(f"字段 ID 必须是非负整数: {self.field_id!r}")
        if not isinstance(self.name, str) or not self.name:
            raise ValidationError("显示名称不能为空")
        if not isinstance(self.type, FieldType):
            raise ValidationError(f"未知字段类型: {self.type!r}")
        if self.required and self.default is not None:
            raise ValidationError(f"必需字段 {self.name!r} 不应声明默认值")
        if self.default is not None and not self.type.accepts(self.default):
            raise ValidationError(
                f"字段 {self.name!r} 的默认值 {self.default!r} 与类型 {self.type.value} 不匹配"
            )


@dataclass(frozen=True)
class SchemaVersion:
    """一个已发布的模式版本。fields 以字段 ID 为键。"""

    version: int
    fields: Dict[int, Field]

    def __post_init__(self) -> None:
        if self.version < 1:
            raise ValidationError(f"版本号必须 >= 1: {self.version}")
        if not self.fields:
            raise ValidationError("模式至少需要一个字段")
        for fid, f in self.fields.items():
            if fid != f.field_id:
                raise ValidationError(f"字段键 {fid} 与字段自身 ID {f.field_id} 不一致")
