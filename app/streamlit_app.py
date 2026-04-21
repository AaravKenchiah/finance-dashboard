import sys
import streamlit as st
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import analysis


DB_PATH = PROJECT_ROOT / "finance.db"


st.set_page_config(page_title="Personal Finance Analytics", layout="wide")


def prepare_anomaly_display(anomaly_df: pd.DataFrame) -> pd.DataFrame:
    """Format anomaly rows for dashboard display."""
    if anomaly_df.empty:
        return anomaly_df

    display_df = anomaly_df.copy()
    display_df["month"] = pd.to_datetime(display_df["month"]).dt.strftime("%Y-%m")
    display_df = display_df[
        [
            "month",
            "category",
            "total_spent",
            "rolling_mean",
            "rolling_std",
            "amount_above_normal",
        ]
    ]
    return display_df.rename(
        columns={
            "month": "Month",
            "category": "Category",
            "total_spent": "Spending",
            "rolling_mean": "3-month average",
            "rolling_std": "3-month std dev",
            "amount_above_normal": "Above normal",
        }
    )


def prepare_forecast_chart(month_df: pd.DataFrame, forecast_df: pd.DataFrame) -> pd.DataFrame:
    """Combine historical monthly spending with forecast values for plotting."""
    historical = month_df[["month", "total_spent"]].copy()
    historical["date"] = pd.to_datetime(historical["month"])
    historical["Historical spending"] = historical["total_spent"]
    historical["Predicted spending"] = float("nan")

    forecast = forecast_df[["ds", "yhat"]].copy()
    forecast["date"] = pd.to_datetime(forecast["ds"])
    forecast["Historical spending"] = float("nan")
    forecast["Predicted spending"] = forecast["yhat"]

    chart_df = pd.concat(
        [
            historical[["date", "Historical spending", "Predicted spending"]],
            forecast[["date", "Historical spending", "Predicted spending"]],
        ],
        ignore_index=True,
    )
    return chart_df.set_index("date").sort_index()


def main():
    st.title("Personal Finance Analytics Dashboard")
    st.caption("SQL-first personal finance analytics powered by DuckDB and Streamlit.")

    # Load metrics and tables from analysis module
    total = analysis.total_spending(DB_PATH)
    cat_df = analysis.spending_by_category(DB_PATH)
    month_df = analysis.monthly_trend(DB_PATH)
    merchants_df = analysis.top_merchants(10, DB_PATH)
    anomaly_df = analysis.detect_spending_anomalies(DB_PATH)
    forecast_df = analysis.forecast_spending(month_df, periods=30)

    # Top-level metrics
    col1, col2 = st.columns([2, 3])
    with col1:
        st.metric("Total spending", f"${total:,.2f}")

    with col2:
        st.markdown(
            "**SQL focus:** every metric and chart on this page is backed by reusable DuckDB SQL queries."
        )

    st.markdown("---")

    # Spending by category
    st.header("Spending by Category")
    if not cat_df.empty:
        cat_chart_df = cat_df.set_index("category")["total_spent"]
        st.bar_chart(cat_chart_df)
        st.dataframe(cat_df.reset_index(drop=True))
    else:
        st.write("No category data available.")

    st.markdown("---")

    # Monthly trend
    st.header("Monthly Spending Trend")
    if not month_df.empty:
        trend_df = month_df.copy()
        trend_df["month"] = pd.to_datetime(trend_df["month"])
        trend_df = trend_df.set_index("month")["total_spent"].sort_index()
        st.line_chart(trend_df)
        st.dataframe(month_df)
    else:
        st.write("No monthly data available.")

    st.markdown("---")

    # Anomaly detection
    st.header("Anomaly Detection")
    if not anomaly_df.empty:
        display_anomalies = prepare_anomaly_display(anomaly_df)
        st.dataframe(
            display_anomalies.style.format(
                {
                    "Spending": "${:,.2f}",
                    "3-month average": "${:,.2f}",
                    "3-month std dev": "${:,.2f}",
                    "Above normal": "${:,.2f}",
                }
            ),
            use_container_width=True,
        )
    else:
        st.write("No category-month spending anomalies found.")

    st.markdown("---")

    # Spending forecast
    st.header("Spending Forecast")
    if not month_df.empty and not forecast_df.empty:
        forecast_chart_df = prepare_forecast_chart(month_df, forecast_df)
        st.line_chart(forecast_chart_df)
        st.dataframe(forecast_df)
    else:
        st.write("Not enough historical spending data to forecast.")

    st.markdown("---")

    # Top merchants table
    st.header("Top Merchants")
    if not merchants_df.empty:
        st.table(merchants_df)
    else:
        st.write("No merchant data available.")


if __name__ == "__main__":
    main()
