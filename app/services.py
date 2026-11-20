"""业务逻辑：参考/样本上传、重验、替换、汇总查询。

参考切换采用「暂存 + 原子生效」两阶段流程：

1. 在极短的写事务内登记一个 PENDING 参考（同一时间只允许一个）；
2. 逐样本在**事务外**按新参考重新校验，结果分批写入暂存表
   ``revalidation_staging``，待生效变体以 PENDING 参考 ID 存入
   ``sample_variants``。这些写入都是短事务，期间其他窗口的样本上传、
   重传、删除以及页面读取都不会被长时间阻塞；
3. 全部样本处理完后，在一个短写事务里把「样本当前最后成功写入的内容」
   的结果原子切换为生效状态（ACTIVE），旧参考降为 SUPERSEDED。

页面只读取 ACTIVE 参考，所以整个重验期间展示的始终是原参考下完整、
一致的结果。若重验/生效失败，则丢弃 PENDING 参考，原参考与期间已成功
提交的样本改动都保持不变。
"""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass, field

from .db import read_tx, revalidation_lock, write_tx
from .normalization import (
    MAX_NAME_LEN,
    ValidationError,
    parse_reference,
    parse_tsv,
    validate_and_normalize,
)

# 每个暂存批次的上限，保证单次持锁时间很短。
STAGING_VARIANT_CHUNK = 500
STAGING_SAMPLE_CHUNK = 200
# 每次从库里读取的样本数（只读取，不持锁做计算）。
SAMPLE_PAGE_SIZE = 100

_STATUS_VALID = "VALID"
_STATUS_NEEDS = "NEEDS_REUPLOAD"


class RevalidationCancelled(Exception):
    """待生效参考已不存在（例如进程重启清理），本次重验作废。"""


@dataclass
class _StagedSample:
    sample_id: int
    revision: int
    content_hash: str
    status: str
    error: str | None = None
    variants: list[dict] = field(default_factory=list)


def _content_hash(raw_tsv: str) -> str:
    return hashlib.sha256(raw_tsv.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# 参考切换（两阶段）
# --------------------------------------------------------------------------- #
class RevalidationInProgress(ValidationError):
    """已有一次换参考正在进行，拒绝并发的第二次换参考。"""


def upload_reference(raw: bytes) -> dict:
    """上传新参考并重验全部样本；成功后新参考原子生效。

    重验期间新参考处于 PENDING，页面继续展示原参考。同一时间只处理一次
    换参考：进程内/跨进程用文件锁互斥，数据库唯一索引兜底。任何异常都会
    丢弃 PENDING 参考，保留原参考及期间已成功写入的样本改动。
    """
    sequence = parse_reference(raw)
    superseded_id: int | None = None
    with revalidation_lock() as locked:
        if locked is False:
            raise RevalidationInProgress(
                "已有另一份参考正在重验中，本次上传未生效，当前结果保持不变。"
                "请等待其完成后再尝试。"
            )
        pending_id = _create_pending_reference(sequence, exclusive=locked is True)
        try:
            _revalidate_all(pending_id, sequence)
            _top_up_staging(pending_id)
            result, superseded_id = _activate_reference(pending_id)
        except BaseException:
            _discard_pending_reference(pending_id)
            raise
    # 旧参考的变体在生效后已不可见，锁释放后再按批清理，避免占用换参考互斥锁；
    # 即使清理中断，遗留行也因 reference_id 过滤而不参与任何展示。
    if superseded_id is not None:
        try:
            _purge_reference_data_chunked(superseded_id)
        except sqlite3.Error:
            # 清理由后续换参考/启动过程继续，不影响本次切换结果。
            pass
    return result


def _create_pending_reference(sequence: str, exclusive: bool) -> int:
    """登记一个待生效参考；同一时间只允许一个。

    ``exclusive`` 为真表示当前进程已持有换参考互斥锁，此时若库里还有
    PENDING，只可能是此前进程崩溃遗留的，可以直接回收后再登记。
    """
    try:
        with write_tx() as conn:
            pending = conn.execute(
                "SELECT id FROM reference_sequences WHERE status='PENDING' LIMIT 1"
            ).fetchone()
            if pending is not None:
                if not exclusive:
                    raise RevalidationInProgress(
                        "已有另一份参考正在重验中，本次上传未生效，当前结果保持不变。"
                        "请等待其完成后再尝试。"
                    )
                _purge_reference_data(conn, pending["id"])
                conn.execute(
                    "DELETE FROM reference_sequences WHERE id=? AND status='PENDING'",
                    (pending["id"],),
                )
            cur = conn.execute(
                "INSERT INTO reference_sequences (sequence, length, status) "
                "VALUES (?, ?, 'PENDING')",
                (sequence, len(sequence)),
            )
            return cur.lastrowid
    except sqlite3.IntegrityError as exc:
        # 唯一索引兜底：没有文件锁的平台上并发登记也只能有一个 PENDING。
        raise RevalidationInProgress(
            "已有另一份参考正在重验中，本次上传未生效，当前结果保持不变。"
            "请等待其完成后再尝试。"
        ) from exc


def _discard_pending_reference(ref_id: int) -> None:
    """丢弃未生效的参考，只清理它自己的暂存数据，不触碰当前结果。"""
    try:
        with write_tx() as conn:
            _purge_reference_data(conn, ref_id)
            conn.execute(
                "DELETE FROM reference_sequences WHERE id=? AND status='PENDING'",
                (ref_id,),
            )
    except Exception:
        # 清理失败不应掩盖原始错误；遗留行会在下次启动时回收。
        pass


def _purge_reference_data(conn: sqlite3.Connection, ref_id: int) -> None:
    conn.execute("DELETE FROM sample_variants WHERE reference_id=?", (ref_id,))
    conn.execute("DELETE FROM revalidation_staging WHERE reference_id=?", (ref_id,))


def _purge_reference_data_chunked(ref_id: int) -> None:
    """按小批次清理不再可见的变体，避免一次持有长时间写锁。"""
    while True:
        with write_tx() as conn:
            conn.execute(
                "DELETE FROM sample_variants WHERE id IN ("
                "  SELECT id FROM sample_variants WHERE reference_id=? LIMIT ?"
                ")",
                (ref_id, STAGING_VARIANT_CHUNK),
            )
            remaining = conn.execute(
                "SELECT 1 FROM sample_variants WHERE reference_id=? LIMIT 1",
                (ref_id,),
            ).fetchone()
            conn.execute(
                "DELETE FROM revalidation_staging WHERE reference_id=?", (ref_id,)
            )
        if remaining is None:
            return


def _revalidate_all(pending_id: int, sequence: str) -> None:
    """按样本「当前最后成功写入的版本」重验，结果分批暂存。

    逐页读取样本快照（短读事务），耗时的解析/校验/归一化完全在事务外；
    期间被删除的样本不会再次出现，被重传的样本按新内容重新校验后暂存。
    """
    cursor = 0
    while True:
        with read_tx() as conn:
            page = conn.execute(
                "SELECT id, revision, raw_tsv FROM samples "
                "WHERE id > ? ORDER BY id LIMIT ?",
                (cursor, SAMPLE_PAGE_SIZE),
            ).fetchall()
        if not page:
            break
        cursor = page[-1]["id"]

        batch: list[_StagedSample] = []
        batch_variants = 0
        for row in page:
            staged = _evaluate_sample(
                row["id"], row["revision"], row["raw_tsv"], sequence
            )
            batch.append(staged)
            batch_variants += len(staged.variants)
            if (
                batch_variants >= STAGING_VARIANT_CHUNK
                or len(batch) >= STAGING_SAMPLE_CHUNK
            ):
                _store_staged(pending_id, batch)
                batch, batch_variants = [], 0
        if batch:
            _store_staged(pending_id, batch)


def _top_up_staging(pending_id: int) -> None:
    """补齐在重验扫描之后又被重传/新建的样本。

    先在事务外算好结果并分批暂存，尽量把切换事务里的计算量降到最低；
    期间持续有其他写入时最多循环有限轮，剩余部分由切换事务兜底。
    """
    with read_tx() as conn:
        pending = conn.execute(
            "SELECT sequence FROM reference_sequences "
            "WHERE id=? AND status='PENDING'",
            (pending_id,),
        ).fetchone()
        if pending is None:
            raise RevalidationCancelled("待生效参考已失效。")
    sequence = pending["sequence"]

    for _ in range(100):
        with read_tx() as conn:
            stale = conn.execute(
                "SELECT s.id, s.revision, s.raw_tsv FROM samples s "
                "LEFT JOIN revalidation_staging g "
                "  ON g.reference_id=? AND g.sample_id=s.id "
                "WHERE g.sample_id IS NULL OR g.revision<>s.revision "
                "ORDER BY s.id LIMIT ?",
                (pending_id, STAGING_SAMPLE_CHUNK),
            ).fetchall()
        if not stale:
            return
        _store_staged(
            pending_id,
            [
                _evaluate_sample(
                    row["id"], row["revision"], row["raw_tsv"], sequence
                )
                for row in stale
            ],
        )


def _evaluate_sample(
    sample_id: int, revision: int, raw_tsv: str, sequence: str
) -> _StagedSample:
    """纯计算：解析 + 校验 + 归一化，不触碰数据库。"""
    content_hash = _content_hash(raw_tsv)
    try:
        rows = parse_tsv(raw_tsv)
        variants = validate_and_normalize(rows, sequence)
    except ValidationError as exc:
        return _StagedSample(
            sample_id, revision, content_hash, _STATUS_NEEDS, str(exc)
        )
    return _StagedSample(
        sample_id, revision, content_hash, _STATUS_VALID, None, variants
    )


def _sample_matches(
    conn: sqlite3.Connection, staged: _StagedSample
) -> bool:
    """样本仍存在，且版本/内容与暂存时一致（防止 id 复用等 ABA 情况）。"""
    current = conn.execute(
        "SELECT revision, raw_tsv FROM samples WHERE id=?", (staged.sample_id,)
    ).fetchone()
    if current is None or current["revision"] != staged.revision:
        return False
    return _content_hash(current["raw_tsv"]) == staged.content_hash


def _store_staged(pending_id: int, batch: list[_StagedSample]) -> None:
    """短写事务写入一批暂存结果；样本版本已变化或已删除的条目跳过。"""
    with write_tx() as conn:
        _require_pending(conn, pending_id)
        for staged in batch:
            if not _sample_matches(conn, staged):
                continue  # 已被删除或重传，以生效时的最新内容为准
            _write_staging_row(conn, pending_id, staged)


def _write_staging_row(
    conn: sqlite3.Connection, pending_id: int, staged: _StagedSample
) -> None:
    conn.execute(
        "DELETE FROM sample_variants WHERE sample_id=? AND reference_id=?",
        (staged.sample_id, pending_id),
    )
    if staged.status == _STATUS_VALID:
        _insert_variants(conn, staged.sample_id, pending_id, staged.variants)
    conn.execute(
        "INSERT INTO revalidation_staging "
        "(reference_id, sample_id, revision, content_hash, status, error) "
        "VALUES (?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(reference_id, sample_id) DO UPDATE SET "
        "revision=excluded.revision, content_hash=excluded.content_hash, "
        "status=excluded.status, error=excluded.error",
        (
            pending_id,
            staged.sample_id,
            staged.revision,
            staged.content_hash,
            staged.status,
            staged.error,
        ),
    )


def _require_pending(conn: sqlite3.Connection, pending_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT id, sequence FROM reference_sequences "
        "WHERE id=? AND status='PENDING'",
        (pending_id,),
    ).fetchone()
    if row is None:
        raise RevalidationCancelled("待生效参考已失效。")
    return row


def _activate_reference(pending_id: int) -> tuple[dict, int | None]:
    """短写事务：按样本最后成功写入的内容补齐暂存并原子切换生效。

    返回 (结果摘要, 被替换的旧参考 id)。
    """
    with write_tx() as conn:
        pending = _require_pending(conn, pending_id)
        old_active = conn.execute(
            "SELECT id FROM reference_sequences WHERE status='ACTIVE' LIMIT 1"
        ).fetchone()

        staged_map = {
            row["sample_id"]: row
            for row in conn.execute(
                "SELECT sample_id, revision, content_hash, status, error "
                "FROM revalidation_staging WHERE reference_id=?",
                (pending_id,),
            )
        }
        samples = conn.execute(
            "SELECT id, revision, raw_tsv FROM samples ORDER BY id"
        ).fetchall()
        for sample in samples:
            staged = staged_map.get(sample["id"])
            if (
                staged is not None
                and staged["revision"] == sample["revision"]
                and staged["content_hash"] == _content_hash(sample["raw_tsv"])
            ):
                status, error = staged["status"], staged["error"]
            else:
                # 重验开始后又被重传/新建（或暂存后再次重传）的样本：
                # 按切换前最后一次成功写入的内容在现场补齐。
                fresh = _evaluate_sample(
                    sample["id"],
                    sample["revision"],
                    sample["raw_tsv"],
                    pending["sequence"],
                )
                _write_staging_row(conn, pending_id, fresh)
                status, error = fresh.status, fresh.error
            conn.execute(
                "UPDATE samples SET reference_id=?, status=?, error=?, "
                "revalidated_at=datetime('now') WHERE id=?",
                (pending_id, status, error, sample["id"]),
            )

        # 旧参考及其他历史版本的变体不再参与展示：切换事务内只更新状态，
        # 旧变体在事务外按批清理（见 _purge_reference_data_chunked）。
        if old_active is not None:
            conn.execute(
                "UPDATE reference_sequences SET status='SUPERSEDED' WHERE id=?",
                (old_active["id"],),
            )
        conn.execute(
            "UPDATE reference_sequences SET status='ACTIVE', "
            "activated_at=datetime('now') WHERE id=?",
            (pending_id,),
        )
        conn.execute(
            "DELETE FROM revalidation_staging WHERE reference_id=?", (pending_id,)
        )

        rows = conn.execute(
            "SELECT name, status, error FROM samples "
            "WHERE reference_id=? ORDER BY name",
            (pending_id,),
        ).fetchall()
        return (
            {
                "reference_id": pending_id,
                "length": len(pending["sequence"]),
                "revalidated": [r for r in rows if r["status"] == _STATUS_VALID],
                "failed": [r for r in rows if r["status"] == _STATUS_NEEDS],
            },
            old_active["id"] if old_active is not None else None,
        )


# --------------------------------------------------------------------------- #
# 样本写入
# --------------------------------------------------------------------------- #
def upload_sample(name: str, raw: bytes) -> dict:
    """上传（或同名重传）一份样本 TSV。

    解析在事务外完成；校验与写入在一个短写事务内。重传时先完整校验新
    文件，成功才替换旧版本，任何错误整体回滚。参考重验期间上传同样只
    需要这个短事务，不会等待整批重验结束；若期间参考发生切换，事务会
    按切换后的最新参考校验。
    """
    name = (name or "").strip()
    if not name:
        raise ValidationError("样本名称不能为空。")
    if len(name) > MAX_NAME_LEN:
        raise ValidationError(f"样本名称不能超过 {MAX_NAME_LEN} 个字符。")

    rows = parse_tsv(raw)
    raw_text = raw.decode("utf-8")

    with write_tx() as conn:
        ref_row = conn.execute(
            "SELECT id, sequence FROM reference_sequences "
            "WHERE status='ACTIVE' LIMIT 1"
        ).fetchone()
        if ref_row is None:
            raise ValidationError("请先上传参考序列，再上传样本。")
        ref_id, ref_seq = ref_row["id"], ref_row["sequence"]

        variants = validate_and_normalize(rows, ref_seq)

        old = conn.execute(
            "SELECT id, revision FROM samples WHERE name=?", (name,)
        ).fetchone()
        revision = 1
        if old is not None:
            revision = old["revision"] + 1
            # 级联删除旧变体与旧暂存结果，保证旧版本不参与任何汇总。
            conn.execute("DELETE FROM samples WHERE id=?", (old["id"],))

        cur = conn.execute(
            "INSERT INTO samples "
            "(name, reference_id, status, error, raw_tsv, revision) "
            "VALUES (?, ?, 'VALID', NULL, ?, ?)",
            (name, ref_id, raw_text, revision),
        )
        sample_id = cur.lastrowid
        _insert_variants(conn, sample_id, ref_id, variants)

    return {"sample": name, "count": len(variants), "replaced": old is not None}


def delete_sample(sample_id: int) -> None:
    with write_tx() as conn:
        # sample_variants / revalidation_staging 通过外键级联删除；
        # 正在重验的批次在生效时也会发现样本已不存在。
        conn.execute("DELETE FROM samples WHERE id=?", (sample_id,))


def _insert_variants(
    conn: sqlite3.Connection,
    sample_id: int,
    reference_id: int,
    variants: list[dict],
) -> None:
    conn.executemany(
        "INSERT INTO sample_variants "
        "(sample_id, reference_id, pos, ref, alt, gt, kind, "
        " orig_pos, orig_ref, orig_alt) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                sample_id,
                reference_id,
                v["pos"],
                v["ref"],
                v["alt"],
                v["gt"],
                v["kind"],
                v["orig_pos"],
                v["orig_ref"],
                v["orig_alt"],
            )
            for v in variants
        ],
    )


# --------------------------------------------------------------------------- #
# 读操作（同一读事务/快照内完成，页面只展示同一版参考的完整汇总）
# --------------------------------------------------------------------------- #
def _active_reference(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM reference_sequences WHERE status='ACTIVE' LIMIT 1"
    ).fetchone()


def get_dashboard() -> dict:
    with read_tx() as conn:
        ref = _active_reference(conn)
        pending = conn.execute(
            "SELECT r.id, r.length, r.uploaded_at, "
            "  (SELECT COUNT(*) FROM revalidation_staging g WHERE g.reference_id=r.id) "
            "  AS staged_count, "
            "  (SELECT COUNT(*) FROM samples) AS total_samples "
            "FROM reference_sequences r WHERE r.status='PENDING' LIMIT 1"
        ).fetchone()
        if ref is None:
            return {
                "reference": None,
                "pending_reference": pending,
                "stats": {},
                "samples": [],
                "variants": [],
            }

        ref_id = ref["id"]
        stats = {
            "valid": conn.execute(
                "SELECT COUNT(*) c FROM samples WHERE reference_id=? AND status='VALID'",
                (ref_id,),
            ).fetchone()["c"],
            "needs_reupload": conn.execute(
                "SELECT COUNT(*) c FROM samples WHERE reference_id=? "
                "AND status='NEEDS_REUPLOAD'",
                (ref_id,),
            ).fetchone()["c"],
            "stale": conn.execute(
                "SELECT COUNT(*) c FROM samples WHERE reference_id IS NOT NULL "
                "AND reference_id<>?",
                (ref_id,),
            ).fetchone()["c"],
        }
        samples = conn.execute(
            "SELECT s.*, "
            "  (SELECT COUNT(*) FROM sample_variants v "
            "   WHERE v.sample_id=s.id AND v.reference_id=?) AS variant_count "
            "FROM samples s ORDER BY s.name",
            (ref_id,),
        ).fetchall()
        variants = conn.execute(
            "SELECT v.pos, v.ref, v.alt, v.kind, COUNT(DISTINCT v.sample_id) AS n_samples "
            "FROM sample_variants v "
            "JOIN samples s ON s.id = v.sample_id "
            "  AND s.reference_id=? AND s.status='VALID' "
            "WHERE v.reference_id=? "
            "GROUP BY v.pos, v.ref, v.alt, v.kind "
            "ORDER BY v.pos, v.ref, v.alt",
            (ref_id, ref_id),
        ).fetchall()
        return {
            "reference": ref,
            "pending_reference": pending,
            "stats": stats,
            "samples": samples,
            "variants": variants,
        }


def get_variant_detail(pos: int, ref: str, alt: str) -> dict | None:
    with read_tx() as conn:
        current = _active_reference(conn)
        if current is None:
            return None
        rows = conn.execute(
            "SELECT s.name AS sample, v.gt, v.orig_pos, v.orig_ref, v.orig_alt, v.kind "
            "FROM sample_variants v "
            "JOIN samples s ON s.id = v.sample_id "
            "WHERE v.pos=? AND v.ref=? AND v.alt=? "
            "AND v.reference_id=? AND s.reference_id=? AND s.status='VALID' "
            "ORDER BY s.name",
            (pos, ref, alt, current["id"], current["id"]),
        ).fetchall()
        if not rows:
            return None
        return {"pos": pos, "ref": ref, "alt": alt, "rows": rows}
