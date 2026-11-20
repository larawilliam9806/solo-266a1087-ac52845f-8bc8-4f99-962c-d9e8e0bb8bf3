"""SQLite 连接管理与事务。

并发模型：
  * 开启 WAL，允许多读单写；
  * 样本上传/重传/删除与参考切换只在**短事务**内写库。大批判样本重验
    的耗时计算全部在事务外完成，结果分批写入暂存区，因此重验期间其他
    窗口仍可正常读写，不会因长时间持有写锁而报 "database is locked"；
  * 所有写事务使用 BEGIN IMMEDIATE 立即取写锁，保证写操作严格串行；
  * 网页读取使用快照事务，页面只会展示同一版参考下完整、一致的汇总。

参考版本状态：
  ACTIVE     当前生效、页面展示的参考；
  PENDING    正在重验、尚未生效的参考（同一时间最多一个）；
  SUPERSEDED 已被替换的历史参考。
"""

from __future__ import annotations

import os
import sqlite3
import time
from contextlib import contextmanager

from flask import Flask, current_app, g

try:  # POSIX 文件锁；非 POSIX 平台退化为仅靠数据库唯一索引互斥
    import fcntl
except ImportError:  # pragma: no cover - 仅在 Windows 等平台发生
    fcntl = None

# 写锁等待时间（秒）。重验期间只有很短的暂存写入，正常不会长时间占用。
BUSY_TIMEOUT_MS = 30_000


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        conn = sqlite3.connect(
            current_app.config["DATABASE"],
            detect_types=sqlite3.PARSE_DECLTYPES,
            timeout=BUSY_TIMEOUT_MS / 1000,
        )
        conn.row_factory = sqlite3.Row
        conn.isolation_level = None  # 手动事务
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
        g.db = conn
    return g.db


def close_db(_exc=None) -> None:
    conn: sqlite3.Connection | None = g.pop("db", None)
    if conn is not None:
        conn.close()


def _begin_immediate(conn: sqlite3.Connection) -> None:
    """BEGIN IMMEDIATE，必要时对瞬时 SQLITE_BUSY 做少量重试。"""
    for attempt in range(3):
        try:
            conn.execute("BEGIN IMMEDIATE")
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == 2:
                raise
            time.sleep(0.05 * (attempt + 1))


@contextmanager
def read_tx():
    """读事务：单条快照，保证一次请求内所有读取来自同一版本。"""
    conn = get_db()
    conn.execute("BEGIN")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


@contextmanager
def write_tx():
    """写事务：立即取写锁，串行化所有写操作；失败整体回滚。"""
    conn = get_db()
    _begin_immediate(conn)
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        raise


@contextmanager
def revalidation_lock():
    """跨进程互斥：同一时间只允许一个换参考任务。

    使用数据库旁路文件锁：持有锁的进程退出（包括崩溃）时由内核自动释放，
    无需手工清理；未被获取时立刻返回 False，调用方因此可以无阻塞地拒绝
    并发的第二次换参考。无 POSIX 文件锁的平台返回 None，由数据库唯一
    索引兜底。
    """
    if fcntl is None:
        yield None
        return
    path = _lock_path(current_app.config["DATABASE"])
    fd = None
    acquired = False
    try:
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except OSError:
            acquired = False
        yield acquired
    finally:
        if fd is not None:
            if acquired:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                except OSError:
                    pass
            os.close(fd)


def _lock_path(config_database: str) -> str:
    return config_database + ".revalidate.lock"


def _lock_probe(path: str) -> bool:
    """不阻塞地探测文件锁是否空闲（未被其他进程/线程持有）。"""
    if fcntl is None:
        return True
    try:
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    except OSError:
        return True
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return False
        fcntl.flock(fd, fcntl.LOCK_UN)
        return True
    finally:
        os.close(fd)


SCHEMA = """
CREATE TABLE IF NOT EXISTS reference_sequences (
    id            INTEGER PRIMARY KEY,
    sequence      TEXT NOT NULL,
    length        INTEGER NOT NULL,
    status        TEXT NOT NULL DEFAULT 'ACTIVE'
                  CHECK (status IN ('ACTIVE', 'PENDING', 'SUPERSEDED')),
    uploaded_at   TEXT NOT NULL DEFAULT (datetime('now')),
    activated_at  TEXT
);

CREATE TABLE IF NOT EXISTS samples (
    id                INTEGER PRIMARY KEY,
    name              TEXT NOT NULL UNIQUE,
    reference_id      INTEGER REFERENCES reference_sequences(id),
    status            TEXT NOT NULL CHECK (status IN ('VALID', 'NEEDS_REUPLOAD')),
    error             TEXT,
    raw_tsv           TEXT NOT NULL,
    revision          INTEGER NOT NULL DEFAULT 1,
    uploaded_at       TEXT NOT NULL DEFAULT (datetime('now')),
    revalidated_at    TEXT
);

CREATE TABLE IF NOT EXISTS sample_variants (
    id            INTEGER PRIMARY KEY,
    sample_id     INTEGER NOT NULL REFERENCES samples(id) ON DELETE CASCADE,
    reference_id  INTEGER REFERENCES reference_sequences(id),
    pos           INTEGER NOT NULL,
    ref           TEXT NOT NULL,
    alt           TEXT NOT NULL,
    gt            TEXT NOT NULL,
    kind          TEXT NOT NULL,
    orig_pos      INTEGER NOT NULL,
    orig_ref      TEXT NOT NULL,
    orig_alt      TEXT NOT NULL
);

-- 参考切换时的暂存区：只保存「样本当前版本」在待生效参考下的校验结果。
-- 真正的待生效变体写入 sample_variants(待生效 reference_id)，页面按
-- ACTIVE 参考过滤，所以重验过程完全不改变当前展示结果。
CREATE TABLE IF NOT EXISTS revalidation_staging (
    reference_id  INTEGER NOT NULL REFERENCES reference_sequences(id) ON DELETE CASCADE,
    sample_id     INTEGER NOT NULL REFERENCES samples(id) ON DELETE CASCADE,
    revision      INTEGER NOT NULL,
    content_hash  TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL CHECK (status IN ('VALID', 'NEEDS_REUPLOAD')),
    error         TEXT,
    PRIMARY KEY (reference_id, sample_id)
);
"""

INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_variant_canonical "
    "ON sample_variants(reference_id, pos, ref, alt)",
    "CREATE INDEX IF NOT EXISTS idx_variant_sample ON sample_variants(sample_id)",
    "CREATE INDEX IF NOT EXISTS idx_staging_reference "
    "ON revalidation_staging(reference_id)",
    # 同一时间最多一个生效参考、最多一个正在重验的参考。
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_reference_single_active "
    "ON reference_sequences(status) WHERE status = 'ACTIVE'",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_reference_single_pending "
    "ON reference_sequences(status) WHERE status = 'PENDING'",
)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _index_columns(conn: sqlite3.Connection, index: str) -> list[str] | None:
    rows = conn.execute(f"PRAGMA index_info({index})").fetchall()
    if not rows:
        return None
    return [row[2] for row in rows]


def _migrate(conn: sqlite3.Connection) -> None:
    """把旧版本数据库平滑升级到带参考状态/暂存区的新结构。"""
    ref_cols = _table_columns(conn, "reference_sequences")
    if "status" not in ref_cols:
        conn.execute(
            "ALTER TABLE reference_sequences ADD COLUMN status TEXT NOT NULL "
            "DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'PENDING', 'SUPERSEDED'))"
        )
    if "activated_at" not in ref_cols:
        conn.execute("ALTER TABLE reference_sequences ADD COLUMN activated_at TEXT")

    sample_cols = _table_columns(conn, "samples")
    if "revision" not in sample_cols:
        conn.execute(
            "ALTER TABLE samples ADD COLUMN revision INTEGER NOT NULL DEFAULT 1"
        )

    variant_cols = _table_columns(conn, "sample_variants")
    if "reference_id" not in variant_cols:
        conn.execute(
            "ALTER TABLE sample_variants ADD COLUMN reference_id INTEGER "
            "REFERENCES reference_sequences(id)"
        )
        conn.execute(
            "UPDATE sample_variants SET reference_id = ("
            "  SELECT s.reference_id FROM samples s WHERE s.id = sample_variants.sample_id"
            ") WHERE reference_id IS NULL"
        )

    # 归一化历史版本状态：保留最新一版为 ACTIVE，只保留最新一个 PENDING。
    # （旧库升级后所有行默认 ACTIVE；异常中断也可能留下多个 PENDING。）
    active_ids = [
        row[0]
        for row in conn.execute(
            "SELECT id FROM reference_sequences WHERE status='ACTIVE' ORDER BY id DESC"
        )
    ]
    for old_id in active_ids[1:]:
        conn.execute(
            "UPDATE reference_sequences SET status='SUPERSEDED' WHERE id=?", (old_id,)
        )
    pending_ids = [
        row[0]
        for row in conn.execute(
            "SELECT id FROM reference_sequences WHERE status='PENDING' ORDER BY id DESC"
        )
    ]
    for stale_id in pending_ids[1:]:
        conn.execute("DELETE FROM sample_variants WHERE reference_id=?", (stale_id,))
        conn.execute(
            "DELETE FROM revalidation_staging WHERE reference_id=?", (stale_id,)
        )
        conn.execute(
            "DELETE FROM reference_sequences WHERE id=? AND status='PENDING'",
            (stale_id,),
        )
    conn.execute(
        "UPDATE reference_sequences SET activated_at=COALESCE(activated_at, uploaded_at) "
        "WHERE status='ACTIVE'"
    )

    # 旧版规范化索引不含 reference_id，按新列定义重建。
    if _index_columns(conn, "idx_variant_canonical") != [
        "reference_id", "pos", "ref", "alt",
    ]:
        conn.execute("DROP INDEX IF EXISTS idx_variant_canonical")
    if _index_columns(conn, "idx_variant_sample") != ["sample_id"]:
        conn.execute("DROP INDEX IF EXISTS idx_variant_sample")


def _drop_orphaned_pending(conn: sqlite3.Connection) -> None:
    """清理上次进程异常退出遗留的 PENDING 参考。

    换参考在单个同步任务内完成；进程启动时仍有 PENDING，说明该任务已中断
    （进程重启后不存在存活的重验任务），应当丢弃待生效版本、保留原 ACTIVE
    参考。期间已提交的样本改动不受影响。
    """
    pending_ids = [
        row[0]
        for row in conn.execute("SELECT id FROM reference_sequences WHERE status='PENDING'")
    ]
    for ref_id in pending_ids:
        conn.execute("DELETE FROM sample_variants WHERE reference_id=?", (ref_id,))
        conn.execute("DELETE FROM revalidation_staging WHERE reference_id=?", (ref_id,))
        conn.execute("DELETE FROM reference_sequences WHERE id=?", (ref_id,))


def _purge_invisible_variants(conn: sqlite3.Connection) -> None:
    """清理不可能再展示的变体（历史/遗留参考、无参考）。

    启动时执行：只保留当前生效与待生效参考的变体，避免异常中断后残留。
    这类行无论是否存在都不会被任何查询展示，删除对并发请求安全。
    """
    conn.execute(
        "DELETE FROM sample_variants WHERE reference_id IS NULL OR reference_id NOT IN "
        "(SELECT id FROM reference_sequences WHERE status IN ('ACTIVE', 'PENDING'))"
    )


def _create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


def _create_indexes(conn: sqlite3.Connection) -> None:
    for statement in INDEXES:
        conn.execute(statement)


def init_db(app: Flask) -> None:
    db_path = app.config["DATABASE"]
    conn = sqlite3.connect(db_path, timeout=BUSY_TIMEOUT_MS / 1000)
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
        conn.execute("BEGIN IMMEDIATE")
        try:
            # 先建表 -> 迁移（补齐新列并归一化历史状态）-> 再建索引，
            # 这样旧库升级时索引引用的列一定已经存在。
            _create_tables(conn)
            _migrate(conn)
            _create_indexes(conn)
            # 只有确认没有其他进程正在重验时，才能把 PENDING 当作崩溃遗留清理。
            if _lock_probe(_lock_path(db_path)):
                _drop_orphaned_pending(conn)
            _purge_invisible_variants(conn)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


def init_app(app: Flask) -> None:
    app.teardown_appcontext(close_db)
    with app.app_context():
        init_db(app)
