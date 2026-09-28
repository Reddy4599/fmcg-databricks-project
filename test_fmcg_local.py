import csv
import shutil
import tempfile
import unittest
from pathlib import Path

from fmcg_local import DEFAULT_DATA, answer, build, dashboard_data


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.data = self.root / "input"
        shutil.copytree(DEFAULT_DATA, self.data)
        self.db = self.root / "out" / "fmcg.sqlite"

    def tearDown(self):
        self.tmp.cleanup()

    def test_sample_kpis_and_idempotent_rebuild(self):
        first = build(self.data, self.db)
        self.assertEqual(first, {"orders": 8, "quantity": 63, "revenue_inr": 6890.0, "customers": 4, "products": 4, "rejected": 1})
        self.assertEqual(build(self.data, self.db), first)
        data = dashboard_data(self.db)
        self.assertEqual(sum(row["revenue_cents"] for row in data["category"]), 689000)
        self.assertEqual(data["rejections"][0]["record_id"], "O009")

    def test_new_order_file_counts_once_and_rejects_duplicate_id(self):
        with (self.data / "orders_incremental.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["order_id", "customer_id", "product_id", "order_qty", "order_placement_date"])
            writer.writerow(["O010", "C001", "P001", "2", "2026-03-01"])
            writer.writerow(["O001", "C001", "P001", "2", "2026-03-01"])
        result = build(self.data, self.db)
        self.assertEqual((result["orders"], result["quantity"], result["revenue_inr"], result["rejected"]), (9, 65, 7150.0, 2))
        self.assertEqual(build(self.data, self.db), result)
        self.assertEqual(answer("monthly revenue", self.db)["results"][0]["month"], "2025-09")

    def test_missing_source_fails_without_replacing_last_good_database(self):
        expected = build(self.data, self.db)
        (self.data / "products.csv").unlink()
        with self.assertRaises(FileNotFoundError):
            build(self.data, self.db)
        self.assertEqual(dashboard_data(self.db)["summary"], expected)


if __name__ == "__main__":
    unittest.main()
