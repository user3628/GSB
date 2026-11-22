"""强制终止后的启动恢复。

恢复只依赖 SQLite 中已经持久化的事实：

* ``intents`` 中状态仍为 ``PENDING`` 的意图；
* ``committed_versions`` 中是否存在该意图对应的提交标记。

判定规则（无需猜测进程崩溃在哪个阶段）：

* 意图 PENDING 且版本已有提交标记 -> 事务 B 实际已落盘，补记为
  ``COMPLETE``（崩溃发生在提交之后、内存状态丢失）。
* 意图 PENDING 且没有提交标记 -> 事务 B 未完成或未开始，删除该版本的任何
  残留行，并把意图标记为 ``ABORTED``，系统回到最后一个完整版本。
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .storage import Store

from .storage import utc_now_iso

GRAPH_TABLES = ("nodes", "edges", "label_index", "adj_index", "query_cache", "cache_deps")


def recover(store: "Store") -> dict:
    intents = [row for row in store.list_intents() if row["status"] == "PENDING"]
    actions = []
    writer = store._writer
    writer.execute("BEGIN IMMEDIATE")
    try:
        for row in intents:
            intent_id = row["intent_id"]
            version = row["version"]
            kind = row["kind"]
            committed = writer.execute(
                "SELECT 1 FROM committed_versions WHERE version=?", (version,)
            ).fetchone()
            payload = json.loads(row["payload_json"])
            if committed:
                writer.execute(
                    "UPDATE intents SET status='COMPLETE', updated_at=?, detail_json=? "
                    "WHERE intent_id=?",
                    (
                        utc_now_iso(),
                        json.dumps({"recovery": "commit_marker_present"}),
                        intent_id,
                    ),
                )
                actions.append(
                    {
                        "intent_id": intent_id,
                        "kind": kind,
                        "version": version,
                        "decision": "completed",
                        "evidence": "committed_versions contains the version",
                    }
                )
            else:
                removed = {}
                for table in GRAPH_TABLES:
                    removed[table] = writer.execute(
                        f"DELETE FROM {table} WHERE version=?", (version,)
                    ).rowcount
                writer.execute(
                    "UPDATE intents SET status='ABORTED', updated_at=?, detail_json=? "
                    "WHERE intent_id=?",
                    (
                        utc_now_iso(),
                        json.dumps({"recovery": "no_commit_marker", "removed": removed,
                                    "payload_kind": payload.get("kind")}),
                        intent_id,
                    ),
                )
                actions.append(
                    {
                        "intent_id": intent_id,
                        "kind": kind,
                        "version": version,
                        "decision": "aborted",
                        "evidence": "commit marker missing; residual rows removed",
                        "removed_rows": removed,
                    }
                )
        writer.execute("COMMIT")
    except Exception:
        writer.execute("ROLLBACK")
        raise

    last_complete = store.current_version()
    return {
        "pending_intents_found": len(intents),
        "actions": actions,
        "last_complete_version": last_complete,
        "current_version_after_recovery": last_complete,
    }
