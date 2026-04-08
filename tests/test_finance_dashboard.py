from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src import analysis
from src.load_data import load_transactions, prepare_dataframe


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = PROJECT_ROOT / "data" / "transactions.csv"


class FinanceDashboardTests(unittest.TestCase):
    """Integration-style tests for the SQL pipeline and analysis layer."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.expected_df = prepare_dataframe(CSV_PATH)

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_finance.db"
        self.inserted_rows = load_transactions(self.db_path, CSV_PATH)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_loader_creates_database_and_inserts_expected_rows(self) -> None:
        self.assertTrue(self.db_path.exists())
        self.assertEqual(self.inserted_rows, len(self.expected_df))

    def test_total_spending_matches_csv_calculation(self) -> None:
        expected_total = float(self.expected_df["amount"].sum())
        actual_total = analysis.total_spending(self.db_path)
        self.assertAlmostEqual(actual_total, expected_total, places=2)

    def test_spending_by_category_matches_grouped_csv_results(self) -> None:
        actual = analysis.spending_by_category(self.db_path).reset_index(drop=True)

        expected = (
            self.expected_df.groupby("category", as_index=False)
            .agg(total_spent=("amount", "sum"), transaction_count=("amount", "size"))
            .sort_values(["total_spent", "category"], ascending=[False, True])
            .reset_index(drop=True)
        )

        pd.testing.assert_frame_equal(actual, expected, check_dtype=False, atol=1e-9, rtol=1e-9)

    def test_monthly_trend_matches_csv_grouping(self) -> None:
        actual = analysis.monthly_trend(self.db_path).copy()
        actual["month"] = pd.to_datetime(actual["month"])

        expected = self.expected_df.copy()
        expected["month"] = pd.to_datetime(expected["date"]).dt.to_period("M").dt.to_timestamp()
        expected = (
            expected.groupby("month", as_index=False)
            .agg(total_spent=("amount", "sum"), transaction_count=("amount", "size"))
            .sort_values("month")
            .reset_index(drop=True)
        )

        pd.testing.assert_frame_equal(actual, expected, check_dtype=False, atol=1e-9, rtol=1e-9)

    def test_top_merchants_respects_limit_and_sort_order(self) -> None:
        actual = analysis.top_merchants(limit=5, db_path=self.db_path).reset_index(drop=True)

        expected = (
            self.expected_df.groupby("merchant", as_index=False)
            .agg(total_spent=("amount", "sum"), transaction_count=("amount", "size"))
            .sort_values(["total_spent", "merchant"], ascending=[False, True])
            .head(5)
            .reset_index(drop=True)
        )

        self.assertEqual(len(actual), 5)
        pd.testing.assert_frame_equal(actual, expected, check_dtype=False, atol=1e-9, rtol=1e-9)

    def test_named_queries_are_loaded_from_sql_file(self) -> None:
        queries = analysis.load_sql_queries()
        self.assertEqual(
            set(queries),
            {"total_spending", "spending_by_category", "monthly_spending_trend", "top_merchants"},
        )


if __name__ == "__main__":
    unittest.main()
