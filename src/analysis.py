from __future__ import annotations

import multiprocessing as mp
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


def _empty_forecast_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["ds", "yhat", "yhat_lower", "yhat_upper"])


def _fit_prophet_forecast(prophet_input: pd.DataFrame, periods: int) -> pd.DataFrame:
    from prophet import Prophet

    model = Prophet(daily_seasonality=False, weekly_seasonality=False, yearly_seasonality=True)
    model.fit(prophet_input)
    future = model.make_future_dataframe(periods=periods, freq="D", include_history=False)
    forecast = model.predict(future)
    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]


def _prophet_worker(
    prophet_records: list[dict[str, Any]],
    periods: int,
    result_queue: mp.Queue,
) -> None:
    prophet_input = pd.DataFrame.from_records(prophet_records)
    forecast = _fit_prophet_forecast(prophet_input, periods)
    result_queue.put(forecast.to_dict("list"))


def _safe_prophet_forecast(prophet_input: pd.DataFrame, periods: int) -> pd.DataFrame | None:
    context_name = "fork" if "fork" in mp.get_all_start_methods() else "spawn"
    context = mp.get_context(context_name)
    result_queue = context.Queue()
    process = context.Process(
        target=_prophet_worker,
        args=(prophet_input.to_dict("records"), periods, result_queue),
    )

    process.start()
    process.join(timeout=45)

    if process.is_alive():
        process.terminate()
        process.join()
        return None

    if process.exitcode != 0 or result_queue.empty():
        return None

    return pd.DataFrame(result_queue.get())


def _trend_fallback_forecast(prophet_input: pd.DataFrame, periods: int) -> pd.DataFrame:
    if prophet_input.empty:
        return _empty_forecast_frame()

    future_dates = pd.date_range(
        prophet_input["ds"].max() + pd.Timedelta(days=1),
        periods=periods,
        freq="D",
    )
    baseline = float(prophet_input["y"].iloc[-1])
    if len(prophet_input) > 1:
        monthly_change = prophet_input["y"].diff().dropna().mean()
        daily_change = float(monthly_change) / 30.4375
    else:
        daily_change = 0.0

    yhat = pd.Series(
        [max(baseline + (daily_change * day), 0.0) for day in range(1, periods + 1)]
    )
    return pd.DataFrame(
        {
            "ds": future_dates,
            "yhat": yhat,
            "yhat_lower": yhat * 0.9,
            "yhat_upper": yhat * 1.1,
        }
    )


def forecast_spending(
    monthly_spending: pd.DataFrame | None = None,
    periods: int = 30,
    db_path: Path = DB_PATH,
) -> pd.DataFrame:
    """Fit Prophet to monthly total spending and forecast future daily spend."""
    if monthly_spending is None:
        monthly_spending = monthly_trend(db_path)

    if monthly_spending.empty:
        return _empty_forecast_frame()

    prophet_input = monthly_spending.rename(columns={"month": "ds", "total_spent": "y"})[
        ["ds", "y"]
    ].copy()
    prophet_input["ds"] = pd.to_datetime(prophet_input["ds"])
    prophet_input["y"] = pd.to_numeric(prophet_input["y"], errors="coerce")
    prophet_input = prophet_input.dropna().sort_values("ds")

    if len(prophet_input) < 2:
        return _empty_forecast_frame()

    forecast = _safe_prophet_forecast(prophet_input, periods)
    if forecast is None:
        forecast = _trend_fallback_forecast(prophet_input, periods)

    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]


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
