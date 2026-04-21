from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


DB_PATH = Path(__file__).resolve().parents[1] / "finance.db"
SQL_FILE = Path(__file__).resolve().parents[1] / "sql" / "queries.sql"


def get_connection(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Return a connection to the file-backed DuckDB database."""
    return duckdb.connect(str(db_path))


def load_sql_queries(sql_file: Path = SQL_FILE) -> dict[str, str]:
    """Parse named SQL statements from queries.sql.

    Each query in the SQL file starts with:
    -- name: query_name
    """
    queries: dict[str, str] = {}
    current_name: str | None = None
    current_lines: list[str] = []

    for raw_line in sql_file.read_text().splitlines():
        if raw_line.startswith("-- name:"):
            if current_name and current_lines:
                queries[current_name] = "\n".join(current_lines).strip()
            current_name = raw_line.split(":", maxsplit=1)[1].strip()
            current_lines = []
            continue

        if current_name:
            current_lines.append(raw_line)

    if current_name and current_lines:
        queries[current_name] = "\n".join(current_lines).strip()

    return queries


QUERIES = load_sql_queries()


def run_named_query(
    query_name: str,
    parameters: list[Any] | None = None,
    db_path: Path = DB_PATH,
) -> pd.DataFrame:
    """Execute a named SQL query and return the result as a DataFrame."""
    sql = QUERIES[query_name]
    with get_connection(db_path) as connection:
        if parameters:
            return connection.execute(sql, parameters).df()
        return connection.execute(sql).df()


def total_spending(db_path: Path = DB_PATH) -> float:
    """Return total spending as a Python float."""
    result = run_named_query("total_spending", db_path=db_path)
    return float(result.loc[0, "total_spending"])


def spending_by_category(db_path: Path = DB_PATH) -> pd.DataFrame:
    """Return spend totals by transaction category."""
    return run_named_query("spending_by_category", db_path=db_path)


def monthly_trend(db_path: Path = DB_PATH) -> pd.DataFrame:
    """Return monthly spending totals using SQL date functions."""
    return run_named_query("monthly_spending_trend", db_path=db_path)


def detect_spending_anomalies(db_path: Path = DB_PATH) -> pd.DataFrame:
    """Flag category-month spend that is unusually high versus the prior 3 months."""
    monthly_category_sql = """
        SELECT
            DATE_TRUNC('month', date) AS month,
            category,
            SUM(amount) AS total_spent
        FROM transactions
        GROUP BY month, category
        ORDER BY category, month
    """

    with get_connection(db_path) as connection:
        monthly_category_spend = connection.execute(monthly_category_sql).df()

    if monthly_category_spend.empty:
        return pd.DataFrame(
            columns=[
                "month",
                "category",
                "total_spent",
                "rolling_mean",
                "rolling_std",
                "threshold",
                "amount_above_normal",
            ]
        )

    monthly_category_spend["month"] = pd.to_datetime(monthly_category_spend["month"])
    monthly_category_spend = monthly_category_spend.sort_values(["category", "month"])

    baseline = monthly_category_spend.groupby("category")["total_spent"].transform(
        lambda series: series.shift(1).rolling(window=3, min_periods=3).mean()
    )
    volatility = monthly_category_spend.groupby("category")["total_spent"].transform(
        lambda series: series.shift(1).rolling(window=3, min_periods=3).std()
    )

    monthly_category_spend["rolling_mean"] = baseline
    monthly_category_spend["rolling_std"] = volatility
    monthly_category_spend["threshold"] = baseline + (2 * volatility)
    monthly_category_spend["amount_above_normal"] = (
        monthly_category_spend["total_spent"] - monthly_category_spend["rolling_mean"]
    )

    anomalies = monthly_category_spend[
        monthly_category_spend["total_spent"] > monthly_category_spend["threshold"]
    ].copy()

    return anomalies.sort_values(["month", "amount_above_normal"], ascending=[False, False]).reset_index(
        drop=True
    )


def top_merchants(limit: int = 10, db_path: Path = DB_PATH) -> pd.DataFrame:
    """Return the merchants with the highest total spending."""
    return run_named_query("top_merchants", parameters=[limit], db_path=db_path)


if __name__ == "__main__":
    print("Total spending:", total_spending())
    print("\nSpending by category:")
    print(spending_by_category().head())
    print("\nMonthly trend:")
    print(monthly_trend().head())
    print("\nTop merchants:")
    print(top_merchants().head())
