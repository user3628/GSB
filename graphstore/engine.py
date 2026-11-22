"""图存储引擎：把持久化、索引、查询、版本绑定与并发控制组装在一起。

并发模型：

* 一把全局写锁（单写者），所有快照加载与增量更新串行；
* 只读查询通过 :class:`~graphstore.view.ViewRegistry` 获取不可变视图，
  多读者可并发执行，不被写者阻塞；
* 写者在事务提交后原子替换当前视图，读者永远看不到图数据与索引不一致的
  中间状态。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .dedup import RequestDeduplicator
from .diff import _diff_results
from .errors import GraphInconsistentError, SnapshotInvalidError
from .indexes import IndexBundle
from .models import Edge, Node
from .query import (
    FastBudget,
    QUERY_HANDLERS,
    execute,
    query_signature,
    request_hash,
)
from .recovery import recover
from .snapshot import GraphSnapshot, load_snapshot_file
from .storage import Store
from .view import SnapshotView, ViewRegistry


@dataclass
class Delta:
    add_nodes: List[Node] = field(default_factory=list)
    remove_nodes: List[str] = field(default_factory=list)
    add_edges: List[Edge] = field(default_factory=list)
    remove_edges: List[Edge] = field(default_factory=list)
    generated_at: str = ""

    @classmethod
    def from_dict(cls, raw: dict) -> "Delta":
        return cls(
            add_nodes=[Node.from_dict(item) for item in raw.get("add_nodes", [])],
            remove_nodes=list(raw.get("remove_nodes", [])),
            add_edges=[Edge.from_dict(item) for item in raw.get("add_edges", [])],
            remove_edges=[Edge.from_dict(item) for item in raw.get("remove_edges", [])],
            generated_at=raw.get("generated_at", ""),
        )


class GraphEngine:
    def __init__(self, db_path: str):
        self.store = Store(db_path)
        self.registry = ViewRegistry()
        self._write_lock = threading.RLock()
        self._dedup = RequestDeduplicator()
        self.crash_hook = None
        self.recovery_report: Optional[dict] = None

    # --------------------------------------------------------------- 生命周期
    def start(self) -> dict:
        report = recover(self.store)
        self.recovery_report = report
        version = report["last_complete_version"]
        if version is not None:
            loaded = self.store.load_version(version)
            if loaded is not None:
                snapshot, index = loaded
                self.registry.publish(SnapshotView(snapshot, index))
        return report

    def close(self):
        self.store.close()

    @property
    def current_version(self) -> Optional[int]:
        return self.registry.current_version

    # ------------------------------------------------------------- 快照加载
    def load_snapshot(self, path: str) -> dict:
        with self._write_lock:
            expected = self.current_version
            snapshot = load_snapshot_file(path, expected_min_version=expected)
            index = IndexBundle.from_snapshot(snapshot)
            intent_id = self.store.create_intent(
                "load_snapshot", snapshot.version,
                {"path": path, "version": snapshot.version},
            )
            self.store.materialize_version(
                snapshot, index, intent_id,
                previous_version=expected,
                affected_dependencies=None,
            )
            self._maybe_crash("after_snapshot_commit")
            view = SnapshotView(snapshot, index)
            self.registry.publish(view)
            if expected is not None:
                self.store.prune_old_versions(snapshot.version)
            return {"version": snapshot.version, "checksum": snapshot.checksum,
                    "nodes": len(snapshot.nodes), "edges": len(snapshot.edges)}

    # ------------------------------------------------------------- 增量更新
    def apply_delta(self, raw_delta: dict) -> dict:
        delta = Delta.from_dict(raw_delta)
        with self._write_lock:
            current_view = self.registry.acquire()
            try:
                old_snapshot = current_view.snapshot
                new_version = old_snapshot.version + 1
                new_snapshot, index, affected = self._build_next_snapshot(
                    old_snapshot, current_view.index, new_version, delta
                )
                intent_id = self.store.create_intent(
                    "apply_delta", new_version,
                    {"version": new_version,
                     "add_nodes": len(delta.add_nodes),
                     "remove_nodes": len(delta.remove_nodes),
                     "add_edges": len(delta.add_edges),
                     "remove_edges": len(delta.remove_edges)},
                )
                self._maybe_crash("after_delta_intent")
                invalidated = self.store.materialize_version(
                    new_snapshot, index, intent_id,
                    previous_version=old_snapshot.version,
                    affected_dependencies=sorted(affected),
                )
                self._maybe_crash("after_delta_commit")
                self.registry.publish(SnapshotView(new_snapshot, index))
                self.store.prune_old_versions(new_version)
                return {
                    "version": new_version,
                    "nodes": len(new_snapshot.nodes),
                    "edges": len(new_snapshot.edges),
                    "affected_dependencies": sorted(affected),
                    "invalidated_cache_entries": invalidated,
                }
            finally:
                self.registry.release(current_view)

    def _build_next_snapshot(self, old: GraphSnapshot, old_index: IndexBundle,
                             new_version: int, delta: Delta):
        nodes_added: Dict[str, Node] = {}
        for node in delta.add_nodes:
            if node.id in old.nodes or node.id in nodes_added:
                raise SnapshotInvalidError(
                    "新增节点 id 已存在", {"node_id": node.id}
                )
            nodes_added[node.id] = node

        nodes_removed: Dict[str, str] = {}
        for node_id in delta.remove_nodes:
            existing = old.nodes.get(node_id)
            if existing is None:
                raise GraphInconsistentError(
                    "待删除节点不存在", {"missing_node": node_id, "operation": "remove_node"}
                )
            nodes_removed[node_id] = existing.label

        removed_edge_keys = set()
        edges_removed: List[Edge] = []
        for edge in delta.remove_edges:
            key = (edge.source, edge.type, edge.target)
            if key not in old.edges:
                raise GraphInconsistentError(
                    "待删除边不存在",
                    {"missing_edge": edge.to_dict(), "operation": "remove_edge"},
                )
            removed_edge_keys.add(key)
            edges_removed.append(edge)

        for node_id in nodes_removed:
            for key, edge in old.edges.items():
                if key in removed_edge_keys:
                    continue
                if edge.source == node_id or edge.target == node_id:
                    removed_edge_keys.add(key)
                    edges_removed.append(edge)

        edges_added: List[Edge] = []
        for edge in delta.add_edges:
            key = (edge.source, edge.type, edge.target)
            if key in old.edges or any(
                (candidate.source, candidate.type, candidate.target) == key
                for candidate in edges_added
            ):
                raise SnapshotInvalidError("新增边已存在", {"edge": edge.to_dict()})
            if edge.source not in old.nodes and edge.source not in nodes_added:
                raise GraphInconsistentError(
                    "新增边的源节点不存在",
                    {"missing_node": edge.source, "operation": "add_edge"},
                )
            if edge.target not in old.nodes and edge.target not in nodes_added:
                raise GraphInconsistentError(
                    "新增边的目标节点不存在",
                    {"missing_node": edge.target, "operation": "add_edge"},
                )
            edges_added.append(edge)

        new_nodes = {
            node_id: node
            for node_id, node in old.nodes.items()
            if node_id not in nodes_removed
        }
        new_nodes.update(nodes_added)
        new_edges = {
            key: edge
            for key, edge in old.edges.items()
            if key not in removed_edge_keys
        }
        for edge in edges_added:
            new_edges[(edge.source, edge.type, edge.target)] = edge

        new_snapshot = GraphSnapshot.build(
            version=new_version,
            generated_at=delta.generated_at or old.generated_at,
            nodes=list(new_nodes.values()),
            edges=list(new_edges.values()),
        )
        new_index, affected = old_index.apply_delta(
            new_version,
            nodes_added=nodes_added,
            nodes_removed=nodes_removed,
            edges_added=edges_added,
            edges_removed=edges_removed,
        )
        rebuilt = IndexBundle.from_snapshot(new_snapshot)
        if rebuilt.label_index != new_index.label_index or rebuilt.adj_index != new_index.adj_index:
            raise GraphInconsistentError(
                "增量索引与全量重建结果不一致", {"version": new_version}
            )
        return new_snapshot, new_index, affected

    # ----------------------------------------------------------------- 查询
    def query(self, body: dict) -> dict:
        request_id = body.get("request_id")
        submitted_hash = request_hash(body)
        if request_id is not None:
            persisted = self.store.get_request_run(request_id)
            if persisted is not None:
                if persisted["request_hash"] != submitted_hash:
                    from .errors import ConflictingRequestError

                    raise ConflictingRequestError(
                        "同一 request_id 历史提交内容不一致",
                        {"request_id": request_id},
                    )
                return self._reuse_persisted(persisted)
            return self._dedup.run_once(
                request_id, submitted_hash, lambda: self._query_and_record(body, request_id)
            )
        return self._execute_query(body)

    def _reuse_persisted(self, row) -> dict:
        import json

        if row["status"] == "COMPLETE":
            result = json.loads(row["result_json"])
            result["dedup"] = {"request_id": row["request_id"], "reused": "persisted"}
            return result
        error = json.loads(row["error_json"])
        from .errors import GraphStoreError

        exc_type = {
            "E_SNAPSHOT_INVALID": SnapshotInvalidError,
            "E_GRAPH_INCONSISTENT": GraphInconsistentError,
        }.get(error["code"], GraphStoreError)
        raise exc_type(error["message"], error.get("context", {}))

    def _query_and_record(self, body: dict, request_id: str) -> dict:
        try:
            result = self._execute_query(body)
        except Exception as exc:
            self.store.save_request_run(
                request_id, request_hash(body), "FAILED",
                body.get("version"), None,
                exc.to_dict() if hasattr(exc, "to_dict") else
                {"code": "E_INTERNAL", "message": str(exc), "context": {}},
            )
            raise
        self.store.save_request_run(
            request_id, request_hash(body), "COMPLETE",
            result["version"], result, None,
        )
        result["dedup"] = {"request_id": request_id, "reused": "fresh"}
        return result

    def _execute_query(self, body: dict) -> dict:
        kind = body["query"]
        if kind not in QUERY_HANDLERS:
            raise GraphInconsistentError("未知查询类型", {"query_type": kind})
        mode = body.get("mode", "exact")
        budget = FastBudget.from_request(body.get("fast_budget"))
        requested_version = body.get("version")
        view = self.registry.acquire(requested_version)
        try:
            params = body.get("params", {})
            signature = query_signature(kind, params)
            cached = self.store.cache_get(view.version, signature)
            if cached is not None and cached["mode"] == mode:
                result = cached["result"]
                result["cache"] = {"hit": True, "signature": signature,
                                   "version": view.version}
                return result
            result = execute(view, kind, params, mode, budget)
            dependencies = result.pop("dependencies")
            intent_id = self.store.create_intent(
                "cache_write", view.version,
                {"signature": signature, "query": kind, "mode": mode},
            )
            self.store.cache_put(
                view.version, signature, request_hash(body), mode,
                result, dependencies, intent_id,
            )
            result["cache"] = {"hit": False, "signature": signature,
                               "version": view.version,
                               "dependencies": dependencies}
            return result
        finally:
            self.registry.release(view)

    # --------------------------------------------------------------- 核对/统计
    def audit_modes(self, body: dict) -> dict:
        """同版本分别执行快速与精确模式，输出逐项可核对的差异。"""
        kind = body["query"]
        params = body.get("params", {})
        view = self.registry.acquire(body.get("version"))
        try:
            budget = FastBudget.from_request(body.get("fast_budget"))
            fast = execute(view, kind, params, "fast", budget)
            exact = execute(view, kind, params, "exact", budget)
            fast.pop("dependencies", None)
            exact.pop("dependencies", None)
            return {
                "query": "audit_modes",
                "version": view.version,
                "fast": fast,
                "exact": exact,
                "diff": _diff_results(kind, fast, exact),
            }
        finally:
            self.registry.release(view)

    def verify_integrity(self, version: Optional[int] = None) -> dict:
        target = version or self.current_version
        if target is None:
            raise GraphInconsistentError("没有可校验的快照", {})
        view = self.registry.acquire(target)
        try:
            problems: List[str] = []
            snapshot = view.snapshot
            for edge in snapshot.edges.values():
                if edge.source not in snapshot.nodes:
                    problems.append(f"边 {edge.source}->{edge.target} 源节点缺失")
                if edge.target not in snapshot.nodes:
                    problems.append(f"边 {edge.source}->{edge.target} 目标节点缺失")
            problems.extend(self.store.verify_persistence(target))
            return {"version": target, "consistent": not problems, "problems": problems}
        finally:
            self.registry.release(view)

    def _maybe_crash(self, point: str):
        if self.crash_hook is not None:
            hook = self.crash_hook
            self.crash_hook = None
            hook(point)
