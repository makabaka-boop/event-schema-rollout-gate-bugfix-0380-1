"""模式注册模块：版本管理、管理员提案、发布与回退。

发布/回退前必须对**所有仍接受写入的版本 × 所有仍活跃消费者**做过渡期检查：
- 删除仍被需要的字段 -> 拒绝
- 字段类型在任一接受写入的版本中与消费者固定类型不一致（跨版本类型漂移）-> 拒绝
- 可见版本中的必需字段在某个接受写入的版本中缺失/非必需 -> 拒绝
  （该旧版本的有效事件会投影出与声明类型矛盾的空值）
- 重命名显示名称 -> 允许（字段身份由数字 ID 决定）
- 可选带默认值字段在旧版本缺失 -> 允许（接入侧用可见版本默认值补齐）

可见版本指针的更新是原子的：所有检查通过、新版本完整构建后，
才在一次赋值中切换 ``_current``；任何失败都不会留下中间状态。
"""
from __future__ import annotations

import threading
from dataclasses import replace
from typing import Dict, Iterable, List, Optional, Protocol

from .model import CompatibilityError, Field, FieldType, SchemaError, SchemaVersion, ValidationError


class ConsumerRequirements(Protocol):
    """注册中心眼中的消费者：只关心它固定了哪些字段 ID 与类型。"""

    consumer_id: str
    requirements: Dict[int, FieldType]


def find_compatibility_problems(
    fields: Dict[int, Field], consumers: Iterable[ConsumerRequirements]
) -> List[str]:
    """检查一组字段定义是否能满足所有给定消费者。返回问题列表（空 = 兼容）。"""
    problems: List[str] = []
    for c in consumers:
        for fid, pinned in c.requirements.items():
            f = fields.get(fid)
            if f is None:
                problems.append(f"消费者 {c.consumer_id} 仍需要字段 {fid}，不能删除")
            elif f.type is not pinned:
                problems.append(
                    f"消费者 {c.consumer_id} 固定字段 {fid} 的类型为 {pinned.value}，"
                    f"与新类型 {f.type.value} 不兼容"
                )
    return problems


def find_transition_problems(
    visible: SchemaVersion,
    accepting_versions: Iterable[SchemaVersion],
    consumers: Iterable[ConsumerRequirements],
) -> List[str]:
    """过渡期一致性检查：切换到 ``visible`` 后，所有**仍接受写入**的版本
    都必须能为每个活跃消费者生成与其固定类型相符的投影。

    对每个消费者固定的 (字段 ID, 类型)：

    - 可见版本缺该字段、或类型不符 -> 拒绝（删除字段/类型不兼容）；
    - 任一仍接受写入的版本把该字段定义成别的类型 -> 拒绝
      （该版本的有效事件会投影出类型不符的值）；
    - 可见版本中该字段为**必需**：它在每个仍接受写入的版本中都必须存在且必需，
      否则旧版有效事件缺字段时投影只能得到空值 -> 拒绝；
    - 可见版本中为带默认值的可选字段：旧版事件缺字段时由接入侧用该默认值补齐；
    - 可见版本中为无默认值的可选字段：缺字段投影为 None，即"可选即缺席"语义。
    """
    accepting = list(accepting_versions)
    problems: List[str] = []
    for c in consumers:
        for fid, pinned in c.requirements.items():
            vf = visible.fields.get(fid)
            if vf is None:
                problems.append(f"消费者 {c.consumer_id} 仍需要字段 {fid}，不能删除")
                continue
            if vf.type is not pinned:
                problems.append(
                    f"消费者 {c.consumer_id} 固定字段 {fid} 的类型为 {pinned.value}，"
                    f"与可见版本 v{visible.version} 的类型 {vf.type.value} 不兼容"
                )
                continue
            for sv in accepting:
                f = sv.fields.get(fid)
                if f is not None and f.type is not pinned:
                    problems.append(
                        f"消费者 {c.consumer_id} 固定字段 {fid} 的类型为 {pinned.value}，"
                        f"但仍接受写入的 v{sv.version} 中该字段类型为 {f.type.value}"
                    )
            if vf.required:
                weak = [
                    sv.version
                    for sv in accepting
                    if (f := sv.fields.get(fid)) is None or not f.required
                ]
                if weak:
                    problems.append(
                        f"消费者 {c.consumer_id} 需要字段 {fid}，但仍接受写入的版本 "
                        f"{weak} 中该字段缺失或非必需，而可见版本 v{visible.version} 中其为必需；"
                        f"过渡期内这些版本的有效事件将投影出与声明类型不符的空值"
                    )
    return problems


class Proposal:
    """管理员提出的新版模式草案，基于提出那一刻的可见版本。

    版本号单调递增（取历史最大版本号 + 1），即使回退过也不会复用旧号。
    """

    def __init__(self, base_version: int, new_version: int, fields: Dict[int, Field]):
        self.base_version = base_version
        self.version = new_version
        self._fields: Dict[int, Field] = dict(fields)

    @property
    def fields(self) -> Dict[int, Field]:
        return dict(self._fields)

    def _require(self, field_id: int) -> Field:
        try:
            return self._fields[field_id]
        except KeyError:
            raise ValidationError(f"字段 {field_id} 不存在") from None

    def add_field(self, field: Field) -> "Proposal":
        if field.field_id in self._fields:
            raise ValidationError(
                f"字段 ID {field.field_id} 已存在；字段身份由 ID 决定，不能复用"
            )
        self._fields[field.field_id] = field
        return self

    def remove_field(self, field_id: int) -> "Proposal":
        self._require(field_id)
        del self._fields[field_id]
        return self

    def rename_field(self, field_id: int, new_name: str) -> "Proposal":
        """重命名显示名称。字段身份（ID/类型/约束）完全不变。"""
        self._fields[field_id] = replace(self._require(field_id), name=new_name)
        return self

    def change_type(self, field_id: int, new_type: FieldType) -> "Proposal":
        # 若现有默认值与新类型不符，replace 会触发 Field 的校验而拒绝；
        # 此时应先 set_default(None) 再改类型。
        self._fields[field_id] = replace(self._require(field_id), type=new_type)
        return self

    def set_required(self, field_id: int, required: bool) -> "Proposal":
        f = self._require(field_id)
        # Field 不允许必需字段携带默认值：收紧为必需时必须同时摘掉默认值；
        # 放开必需时不自动恢复默认值（由管理员显式 set_default 决定）。
        if required:
            f = replace(f, required=True, default=None)
        else:
            f = replace(f, required=False)
        self._fields[field_id] = f
        return self

    def set_default(self, field_id: int, default) -> "Proposal":
        self._fields[field_id] = replace(self._require(field_id), default=default)
        return self

    def build(self) -> SchemaVersion:
        return SchemaVersion(self.version, dict(self._fields))


class SchemaRegistry:
    """模式注册中心：保存全部已发布版本，原子地推进/回退可见版本。"""

    def __init__(self, lock: Optional[threading.RLock] = None):
        self._lock = lock or threading.RLock()
        self._versions: Dict[int, SchemaVersion] = {}
        self._deprecated: set[int] = set()
        self._current: Optional[int] = None

    # ---------- 查询 ----------

    @property
    def current_version(self) -> Optional[int]:
        with self._lock:
            return self._current

    def current(self) -> SchemaVersion:
        with self._lock:
            if self._current is None:
                raise SchemaError("尚未注册任何模式版本")
            return self._versions[self._current]

    def get_version(self, version: int) -> Optional[SchemaVersion]:
        with self._lock:
            return self._versions.get(version)

    def published_versions(self) -> List[int]:
        with self._lock:
            return sorted(self._versions)

    def is_accepting(self, version: int) -> bool:
        """该版本是否仍接受写入：已发布且未被弃用。

        发布新版本不会弃用旧版本 —— 过渡期内新旧生产者都能提交有效事件。
        """
        with self._lock:
            return version in self._versions and version not in self._deprecated

    def accepting_versions(self) -> List[SchemaVersion]:
        """所有仍接受写入的已发布版本（发布/回退/消费者升级据此做过渡期检查）。"""
        with self._lock:
            return [sv for v, sv in self._versions.items() if v not in self._deprecated]

    # ---------- 初始注册 ----------

    def register_initial(self, fields: Iterable[Field]) -> int:
        with self._lock:
            if self._versions:
                raise SchemaError("初始模式已注册")
            sv = SchemaVersion(1, {f.field_id: f for f in fields})
            self._versions[1] = sv
            self._current = 1
            return 1

    # ---------- 提案 / 发布 ----------

    def begin_proposal(self) -> Proposal:
        """管理员基于当前可见版本提出新版草案。"""
        with self._lock:
            if self._current is None:
                raise SchemaError("尚未注册初始模式")
            base = self._versions[self._current]
            return Proposal(base.version, max(self._versions) + 1, base.fields)

    def publish(self, proposal: Proposal, consumers: Iterable[ConsumerRequirements]) -> int:
        """发布提案。先检查所有仍活跃消费者，通过后原子切换可见版本。"""
        with self._lock:
            if proposal.base_version != self._current:
                raise SchemaError(
                    f"提案基于 v{proposal.base_version}，但当前可见版本已是 "
                    f"v{self._current}，请重新提案"
                )
            if proposal.version in self._versions:
                raise SchemaError(f"版本号 v{proposal.version} 已存在")
            new_version = proposal.build()  # 草案自身合法性校验
            # 发布后仍接受写入的版本：新可见版本 + 此前所有未弃用的旧版本（过渡期）
            accepting = [sv for v, sv in self._versions.items() if v not in self._deprecated]
            accepting.append(new_version)
            problems = find_transition_problems(new_version, accepting, consumers)
            if problems:
                raise CompatibilityError("；".join(problems))
            # 原子提交：全部检查通过后，一次赋值切换可见版本
            self._versions[new_version.version] = new_version
            self._current = new_version.version
            return new_version.version

    # ---------- 回退 / 弃用 ----------

    def rollback(self, target_version: int, consumers: Iterable[ConsumerRequirements]) -> int:
        """回退到历史版本。目标必须仍能满足所有活跃消费者，否则拒绝。"""
        with self._lock:
            target = self._versions.get(target_version)
            if target is None:
                raise SchemaError(f"版本 v{target_version} 不存在，无法回退")
            if target_version == self._current:
                raise SchemaError(f"v{target_version} 就是当前可见版本")
            if target_version in self._deprecated:
                raise SchemaError(f"版本 v{target_version} 已弃用，无法回退")
            # 回退不改变哪些版本接受写入，只是把可见指针指回旧版本；
            # 所有仍接受写入（含更新的版本）的版本都必须与活跃消费者一致。
            accepting = [sv for v, sv in self._versions.items() if v not in self._deprecated]
            problems = find_transition_problems(target, accepting, consumers)
            if problems:
                raise CompatibilityError("；".join(problems))
            self._current = target_version  # 原子切换可见版本
            return target_version

    def deprecate(self, version: int) -> None:
        """弃用一个旧版本（结束其过渡期），之后该版本的事件将被隔离。"""
        with self._lock:
            if version not in self._versions:
                raise SchemaError(f"版本 v{version} 不存在")
            if version == self._current:
                raise SchemaError("不能弃用当前可见版本")
            self._deprecated.add(version)
