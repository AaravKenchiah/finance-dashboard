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


def main():
    st.title("Personal Finance Analytics Dashboard")
    st.caption("SQL-first personal finance analytics powered by DuckDB and Streamlit.")

    # Load metrics and tables from analysis module
    total = analysis.total_spending(DB_PATH)
    cat_df = analysis.spending_by_category(DB_PATH)
    month_df = analysis.monthly_trend(DB_PATH)
    merchants_df = analysis.top_merchants(10, DB_PATH)

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

    # Top merchants table
    st.header("Top Merchants")
    if not merchants_df.empty:
        st.table(merchants_df)
    else:
        st.write("No merchant data available.")


if __name__ == "__main__":
    main()
