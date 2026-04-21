from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from app.streamlit_app import ensure_database, prepare_anomaly_display, prepare_forecast_chart
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

    def test_app_bootstrap_creates_database_when_missing(self) -> None:
        bootstrap_db_path = Path(self.temp_dir.name) / "bootstrap_finance.db"

        ensure_database(bootstrap_db_path, CSV_PATH)

        self.assertTrue(bootstrap_db_path.exists())
        self.assertAlmostEqual(
            analysis.total_spending(bootstrap_db_path),
            float(self.expected_df["amount"].sum()),
            places=2,
        )

    def test_transaction_csv_contains_realistic_spending_history(self) -> None:
        self.assertGreaterEqual(len(self.expected_df), 500)
        self.assertGreaterEqual(self.expected_df["category"].nunique(), 15)
        self.assertGreaterEqual(self.expected_df["merchant"].nunique(), 50)
        self.assertGreaterEqual(
            pd.to_datetime(self.expected_df["date"]).max()
            - pd.to_datetime(self.expected_df["date"]).min(),
            pd.Timedelta(days=365),
        )

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

    def test_detect_spending_anomalies_flags_category_month_spikes(self) -> None:
        anomaly_csv = Path(self.temp_dir.name) / "anomaly_transactions.csv"
        anomaly_csv.write_text(
            "\n".join(
                [
                    "date,amount,category,merchant",
                    "2024-01-01,100.00,Travel,Airline",
                    "2024-02-01,110.00,Travel,Airline",
                    "2024-03-01,90.00,Travel,Airline",
                    "2024-04-01,140.00,Travel,Airline",
                    "2024-01-01,50.00,Groceries,Market",
                    "2024-02-01,52.00,Groceries,Market",
                    "2024-03-01,48.00,Groceries,Market",
                    "2024-04-01,53.00,Groceries,Market",
                ]
            )
        )
        anomaly_db_path = Path(self.temp_dir.name) / "anomaly_test_finance.db"
        load_transactions(anomaly_db_path, anomaly_csv)

        actual = analysis.detect_spending_anomalies(anomaly_db_path)

        self.assertEqual(len(actual), 1)
        self.assertEqual(actual.loc[0, "category"], "Travel")
        self.assertEqual(pd.Timestamp(actual.loc[0, "month"]), pd.Timestamp("2024-04-01"))
        self.assertAlmostEqual(actual.loc[0, "rolling_mean"], 100.00, places=2)
        self.assertAlmostEqual(actual.loc[0, "rolling_std"], 10.00, places=2)
        self.assertAlmostEqual(actual.loc[0, "amount_above_normal"], 40.00, places=2)

    def test_forecast_spending_fits_prophet_and_returns_future_predictions(self) -> None:
        monthly_spending = pd.DataFrame(
            {
                "month": pd.date_range("2024-01-01", periods=4, freq="MS"),
                "total_spent": [100.0, 125.0, 115.0, 150.0],
            }
        )
        expected_forecast = pd.DataFrame(
            {
                "ds": pd.date_range("2024-05-01", periods=30, freq="D"),
                "yhat": range(30),
                "yhat_lower": range(-1, 29),
                "yhat_upper": range(1, 31),
            }
        )

        with patch("src.analysis._safe_prophet_forecast", return_value=expected_forecast) as prophet_fit:
            actual = analysis.forecast_spending(monthly_spending, periods=30)

        self.assertEqual(len(actual), 30)
        self.assertEqual(list(actual.columns), ["ds", "yhat", "yhat_lower", "yhat_upper"])
        prophet_input = prophet_fit.call_args.args[0]
        self.assertEqual(list(prophet_input.columns), ["ds", "y"])
        self.assertEqual(prophet_input["y"].tolist(), [100.0, 125.0, 115.0, 150.0])

    def test_forecast_spending_returns_trend_fallback_when_prophet_fails(self) -> None:
        monthly_spending = pd.DataFrame(
            {
                "month": pd.date_range("2024-01-01", periods=4, freq="MS"),
                "total_spent": [100.0, 125.0, 115.0, 150.0],
            }
        )

        with patch("src.analysis._safe_prophet_forecast", return_value=None):
            actual = analysis.forecast_spending(monthly_spending, periods=30)

        self.assertEqual(len(actual), 30)
        self.assertEqual(list(actual.columns), ["ds", "yhat", "yhat_lower", "yhat_upper"])
        self.assertFalse(actual["yhat"].isna().any())

    def test_dashboard_anomaly_display_formats_recruiter_friendly_columns(self) -> None:
        anomaly_df = pd.DataFrame(
            {
                "month": [pd.Timestamp("2024-04-01")],
                "category": ["Travel"],
                "total_spent": [140.0],
                "rolling_mean": [100.0],
                "rolling_std": [10.0],
                "threshold": [120.0],
                "amount_above_normal": [40.0],
            }
        )

        actual = prepare_anomaly_display(anomaly_df)

        self.assertEqual(
            list(actual.columns),
            [
                "Month",
                "Category",
                "Spending",
                "3-month average",
                "3-month std dev",
                "Above normal",
            ],
        )
        self.assertEqual(actual.loc[0, "Month"], "2024-04")
        self.assertEqual(actual.loc[0, "Above normal"], 40.0)

    def test_dashboard_forecast_chart_combines_history_and_predictions(self) -> None:
        month_df = pd.DataFrame(
            {
                "month": pd.date_range("2024-01-01", periods=2, freq="MS"),
                "total_spent": [100.0, 125.0],
            }
        )
        forecast_df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-03-01", periods=2, freq="D"),
                "yhat": [130.0, 132.0],
            }
        )

        actual = prepare_forecast_chart(month_df, forecast_df)

        self.assertEqual(len(actual), 4)
        self.assertEqual(list(actual.columns), ["Historical spending", "Predicted spending"])
        self.assertEqual(actual.loc[pd.Timestamp("2024-01-01"), "Historical spending"], 100.0)
        self.assertEqual(actual.loc[pd.Timestamp("2024-03-01"), "Predicted spending"], 130.0)

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
