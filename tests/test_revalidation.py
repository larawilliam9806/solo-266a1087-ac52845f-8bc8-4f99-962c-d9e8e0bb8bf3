"""换参考期间并发行为测试：短事务重验、原子生效、失败保留原状。

覆盖需求：
  * 重验大批样本期间，其他窗口仍能按当前参考上传/重传/删除样本；
  * 重验期间首页与变异详情持续展示当前参考下的完整结果；
  * 新参考生效时，按切换前最后一次成功写入的样本内容给出重验结果；
  * 已删除的样本不再出现，未通过校验的样本标记为待重传；
  * 同一时间只处理一次换参考；
  * 换版失败时保留原参考与期间成功的样本改动。
"""

import io
import os
import tempfile
import threading
import unittest
from unittest import mock

from app import create_app, services
from app.db import get_db

REF_V1 = "GCTTTGCACGT"   # 第 4 位为 T
REF_V2 = "GCTATGCACGT"   # 第 4 位为 A；第 1 位仍为 G


def tsv(text: str) -> bytes:
    return text.encode("utf-8")


class RevalidationTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "reval.db")
        os.environ["DATABASE_PATH"] = self.db_path
        self.app = create_app()
        self.app.config.update(TESTING=True, SECRET_KEY="test")
        self.client = self.app.test_client()

    def tearDown(self):
        os.environ.pop("DATABASE_PATH", None)
        self.tmp.cleanup()

    def upload_ref(self, seq: str):
        data = {"reference": (io.BytesIO(seq.encode()), "ref.txt")}
        return self.client.post(
            "/reference", data=data, content_type="multipart/form-data",
            follow_redirects=True,
        )

    def upload_sample(self, name: str, content: bytes):
        data = {"name": name, "sample": (io.BytesIO(content), f"{name}.tsv")}
        return self.client.post(
            "/samples", data=data, content_type="multipart/form-data",
            follow_redirects=True,
        )

    def delete_sample(self, name: str):
        with self.app.app_context():
            row = get_db().execute(
                "SELECT id FROM samples WHERE name=?", (name,)
            ).fetchone()
        return self.client.post(
            f"/samples/{row['id']}/delete", follow_redirects=True
        )

    def sample_row(self, name: str):
        with self.app.app_context():
            return get_db().execute(
                "SELECT * FROM samples WHERE name=?", (name,)
            ).fetchone()

    def active_ref(self):
        with self.app.app_context():
            return get_db().execute(
                "SELECT * FROM reference_sequences WHERE status='ACTIVE'"
            ).fetchone()

    def dashboard(self):
        with self.app.app_context():
            return services.get_dashboard()

    def variant_map(self):
        return {
            (v["pos"], v["ref"], v["alt"]): v["n_samples"]
            for v in self.dashboard()["variants"]
        }


class PendingRevalidationTest(RevalidationTestBase):
    """用事件钩子把重验暂停在中间，模拟「大批样本重验仍在进行」."""

    def setUp(self):
        super().setUp()
        self.upload_ref(REF_V1)
        self.upload_sample("S1", tsv("1\tG\tC\t0/1\n3\tT\tTT\t0/1\n"))
        self.upload_sample("S2", tsv("4\tT\tTT\t1/1\n"))
        self.upload_sample("S3", tsv("1\tG\tC\t1/1\n"))

        self.paused = threading.Event()
        self.resume = threading.Event()
        self.reval_errors: list[BaseException] = []
        self.original_store = services._store_staged
        self.pause_after = 1
        self.store_calls = 0

        def slow_store(pending_id, batch):
            self.store_calls += 1
            result = self.original_store(pending_id, batch)
            if self.store_calls == self.pause_after:
                # 第一批结果已暂存后暂停，模拟「大批重验仍在进行」
                self.paused.set()
                self.resume.wait(10)
            return result

        self._patcher = mock.patch.object(services, "_store_staged", slow_store)
        self._patcher.start()

    def tearDown(self):
        self._patcher.stop()
        self.resume.set()
        super().tearDown()

    def start_reference_rotation(self, target: str = REF_V2) -> threading.Thread:
        def worker():
            try:
                with self.app.app_context():
                    services.upload_reference(target.encode())
            except BaseException as exc:  # noqa: BLE001 - 测试需要收集所有异常
                self.reval_errors.append(exc)

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        self.assertTrue(self.paused.wait(10), "重验未按预期暂停")
        return thread


class InterleavingTest(PendingRevalidationTest):
    def test_uploads_allowed_while_revalidating(self):
        thread = self.start_reference_rotation()

        # 重验进行中：按当前参考（V1）上传新样本、重传、删除都不被阻塞
        self.upload_sample("S4", tsv("1\tG\tC\t0/1\n"))
        self.upload_sample("S2", tsv("1\tG\tC\t1/1\n"))     # 重传改为 V1 下合法内容
        self.delete_sample("S3")

        # 期间页面仍展示 V1 下的完整结果，并提示重验进度
        page = self.client.get("/").get_data(as_text=True)
        self.assertIn("参考重验中", page)
        self.assertIn("当前参考", page)
        detail = self.client.get("/variants/1/G>C")
        self.assertEqual(detail.status_code, 200)
        self.assertIn("S4", detail.get_data(as_text=True))

        self.resume.set()
        thread.join(timeout=15)
        self.assertFalse(thread.is_alive(), "换参考线程未结束")
        self.assertEqual(self.reval_errors, [])

        # 生效后：S2 按切换前最后一次成功写入的内容重验（V2 下第 1 位 G 仍合法）
        self.assertEqual(self.active_ref()["sequence"], REF_V2)
        self.assertEqual(self.sample_row("S2")["status"], "VALID")
        self.assertEqual(self.sample_row("S4")["status"], "VALID")
        self.assertIsNone(self.sample_row("S3"), "已删除样本不应重新出现")
        # S1 + S2 + S4 都含 (1,G,C)
        self.assertEqual(self.variant_map()[(1, "G", "C")], 3)

    def test_deleted_sample_absent_and_invalid_marked_needs_reupload(self):
        thread = self.start_reference_rotation()
        self.delete_sample("S1")
        self.resume.set()
        thread.join(timeout=15)
        self.assertFalse(thread.is_alive())

        self.assertIsNone(self.sample_row("S1"))
        # S2 依赖 V1 第 4 位的 T，在 V2 下 REF 不符 -> 待重传，且没有变体
        s2 = self.sample_row("S2")
        self.assertEqual(s2["status"], "NEEDS_REUPLOAD")
        self.assertIn("REF 核对失败", s2["error"])
        with self.app.app_context():
            count = get_db().execute(
                "SELECT COUNT(*) c FROM sample_variants WHERE sample_id=?",
                (s2["id"],),
            ).fetchone()["c"]
        self.assertEqual(count, 0)
        page = self.client.get("/").get_data(as_text=True)
        self.assertIn("待重传", page)

    def test_only_one_reference_change_at_a_time(self):
        thread = self.start_reference_rotation()

        # 第二次换参考在锁上立刻被拒绝，且不影响正在进行的重验
        resp = self.upload_ref("ACGTACGTACGT")
        self.assertIn("正在重验", resp.get_data(as_text=True))
        with self.app.app_context():
            pendings = get_db().execute(
                "SELECT COUNT(*) c FROM reference_sequences WHERE status='PENDING'"
            ).fetchone()["c"]
            actives = get_db().execute(
                "SELECT COUNT(*) c FROM reference_sequences WHERE status='ACTIVE'"
            ).fetchone()["c"]
        self.assertEqual(pendings, 1)
        self.assertEqual(actives, 1)

        self.resume.set()
        thread.join(timeout=15)
        self.assertFalse(thread.is_alive())
        self.assertEqual(self.reval_errors, [])
        self.assertEqual(self.active_ref()["sequence"], REF_V2)


class FailureAtomicityTest(RevalidationTestBase):
    def setUp(self):
        super().setUp()
        self.upload_ref(REF_V1)
        self.upload_sample("S1", tsv("1\tG\tC\t0/1\n"))
        self.upload_sample("S2", tsv("4\tT\tTT\t1/1\n"))

    def test_failure_keeps_original_reference_and_successful_changes(self):
        def failing_activate(pending_id):
            # 模拟在切换阶段的失败（例如写锁超时/进程错误）
            raise RuntimeError("simulated activation failure")

        with mock.patch.object(services, "_activate_reference", failing_activate):
            with self.assertRaises(RuntimeError):
                with self.app.app_context():
                    services.upload_reference(REF_V2.encode())

        # 原参考仍是 V1，样本变化（重验期间的成功写入）保留
        self.assertEqual(self.active_ref()["sequence"], REF_V1)
        with self.app.app_context():
            pending = get_db().execute(
                "SELECT COUNT(*) c FROM reference_sequences WHERE status='PENDING'"
            ).fetchone()["c"]
        self.assertEqual(pending, 0, "失败的待生效参考应被清理")
        self.assertEqual(self.sample_row("S1")["status"], "VALID")
        self.assertEqual(self.variant_map()[(1, "G", "C")], 1)
        # S2 的 T>TT 在 V1 的 TTT 重复区左对齐为 (2,C,CT)
        self.assertEqual(self.variant_map()[(2, "C", "CT")], 1)

        # 再次换参考仍可正常完成
        self.upload_ref(REF_V2)
        self.assertEqual(self.active_ref()["sequence"], REF_V2)

    def test_invalid_reference_leaves_everything_unchanged(self):
        resp = self.upload_ref("ACGN")
        self.assertIn("只允许包含大写", resp.get_data(as_text=True))
        self.assertEqual(self.active_ref()["sequence"], REF_V1)
        with self.app.app_context():
            refs = get_db().execute(
                "SELECT COUNT(*) c FROM reference_sequences"
            ).fetchone()["c"]
        self.assertEqual(refs, 1, "非法参考不应留下记录")

    def test_no_long_write_lock_during_revalidation(self):
        """重验计算期间不应持有写锁：其他连接能立即写入。"""
        observed = {}
        original_store = services._store_staged

        def store_with_probe(pending_id, batch):
            # 在暂存批次之间（持锁之外）用独立连接尝试写入，必须立即成功
            import sqlite3

            conn = sqlite3.connect(self.db_path, timeout=0.5)
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT INTO samples (name, reference_id, status, error, raw_tsv) "
                    "VALUES ('PROBE', NULL, 'VALID', NULL, '1\\tG\\tC\\t0/1')"
                )
                conn.execute("ROLLBACK")
                observed["ok"] = True
            except sqlite3.OperationalError as exc:
                observed["error"] = str(exc)
            finally:
                conn.close()
            return original_store(pending_id, batch)

        with mock.patch.object(services, "_store_staged", store_with_probe):
            with self.app.app_context():
                services.upload_reference(REF_V2.encode())

        self.assertEqual(observed, {"ok": True})


class StalePendingCleanupTest(RevalidationTestBase):
    def setUp(self):
        super().setUp()
        self.upload_ref(REF_V1)
        self.upload_sample("S1", tsv("1\tG\tC\t0/1\n"))

    def test_restart_discards_orphan_pending(self):
        # 模拟上次进程在重验中途崩溃：库里留下 PENDING 参考与暂存数据
        with self.app.app_context():
            conn = get_db()
            conn.execute(
                "INSERT INTO reference_sequences (sequence, length, status) "
                "VALUES (?, ?, 'PENDING')",
                (REF_V2, len(REF_V2)),
            )
            pending_id = conn.execute(
                "SELECT id FROM reference_sequences WHERE status='PENDING'"
            ).fetchone()["id"]
            row = conn.execute("SELECT * FROM samples WHERE name='S1'").fetchone()
            conn.execute(
                "INSERT INTO revalidation_staging "
                "(reference_id, sample_id, revision, content_hash, status, error) "
                "VALUES (?, ?, ?, ?, 'VALID', NULL)",
                (pending_id, row["id"], row["revision"], ""),
            )
            conn.execute(
                "INSERT INTO sample_variants "
                "(sample_id, reference_id, pos, ref, alt, gt, kind, "
                " orig_pos, orig_ref, orig_alt) "
                "VALUES (?, ?, 1, 'G', 'C', '0/1', 'SNP', 1, 'G', 'C')",
                (row["id"], pending_id),
            )

        # 重新创建应用（等价于进程重启）
        app2 = create_app()
        app2.config.update(TESTING=True, SECRET_KEY="test")
        with app2.app_context():
            dash = services.get_dashboard()
            self.assertEqual(dash["reference"]["sequence"], REF_V1)
            self.assertIsNone(dash["pending_reference"])
            count = get_db().execute(
                "SELECT COUNT(*) c FROM sample_variants WHERE reference_id=?",
                (pending_id,),
            ).fetchone()["c"]
            self.assertEqual(count, 0)
            staging = get_db().execute(
                "SELECT COUNT(*) c FROM revalidation_staging"
            ).fetchone()["c"]
            self.assertEqual(staging, 0)

    def test_missing_sample_scenarios_do_not_crash(self):
        """重验期间样本被删除/重传，最终结果只反映最后成功写入的内容。"""
        self.upload_sample("S2", tsv("4\tT\tTT\t1/1\n"))
        with self.app.app_context():
            services.delete_sample(
                get_db().execute(
                    "SELECT id FROM samples WHERE name='S2'"
                ).fetchone()["id"]
            )
        self.upload_ref(REF_V2)
        names = {s["name"] for s in self.dashboard()["samples"]}
        self.assertEqual(names, {"S1"})


class PendingUiTest(PendingRevalidationTest):
    def test_home_and_detail_show_current_reference_during_revalidation(self):
        thread = self.start_reference_rotation()

        dash = self.dashboard()
        self.assertEqual(dash["reference"]["sequence"], REF_V1)
        self.assertIsNotNone(dash["pending_reference"])
        self.assertGreater(dash["pending_reference"]["staged_count"], 0)

        # 当前参考下的完整汇总仍然可读
        self.assertIn((1, "G", "C"), self.variant_map())
        detail = self.client.get("/variants/1/G>C")
        self.assertEqual(detail.status_code, 200)
        self.assertIn("S1", detail.get_data(as_text=True))

        self.resume.set()
        thread.join(timeout=15)
        self.assertFalse(thread.is_alive())
        self.assertEqual(self.reval_errors, [])
        self.assertEqual(self.active_ref()["sequence"], REF_V2)
        page = self.client.get("/").get_data(as_text=True)
        self.assertNotIn("参考重验中", page)
        self.assertIn("当前参考 #2", page)


if __name__ == "__main__":
    unittest.main()
