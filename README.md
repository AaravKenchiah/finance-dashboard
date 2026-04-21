# Personal Finance Analytics Dashboard

## Overview

This project is a personal finance analytics dashboard built with Python, DuckDB, SQL, Prophet, pandas, and Streamlit. It loads a realistic personal finance transaction dataset from CSV into DuckDB, runs reusable SQL analytics queries, detects unusual category-level spending spikes, forecasts future spending, and presents the results in a clean Streamlit dashboard.

The project is intentionally SQL-first for the core reporting layer, with Python used for orchestration, statistical analysis, forecasting, testing, and dashboard delivery. This keeps the project close to real analytics workflows: ingest data, model it in a database, run repeatable queries, and surface useful business-style insights.

## Tech Stack

- Python
- SQL
- DuckDB
- pandas
- Prophet
- Streamlit
- unittest

## Features

- Realistic personal finance transaction dataset in `data/transactions.csv`
- Persistent DuckDB database generated from the CSV
- Safe table refresh logic for the `transactions` table
- Clean transaction schema with `DATE`, `DOUBLE`, and text columns
- Reusable SQL queries stored in `sql/queries.sql`
- SQL-driven analytics module in `src/analysis.py`
- Monthly category anomaly detection using a 3-month rolling baseline
- Prophet-powered spending forecast for the next 30 days, with a defensive fallback for environments where Prophet's backend is unavailable
- Streamlit dashboard with:
  - total spending metric
  - spending by category bar chart
  - monthly spending trend line chart
  - anomaly detection table
  - spending forecast chart
  - top merchants table
- Integration-style tests for loading, SQL outputs, anomaly detection, forecasting, and dashboard data preparation

## Project Structure

```text
finance-dashboard/
├── data/transactions.csv
├── sql/queries.sql
├── src/load_data.py
├── src/analysis.py
├── app/streamlit_app.py
├── requirements.txt
├── tests/test_finance_dashboard.py
└── README.md
```

## How It Works

`src/load_data.py` reads the CSV, cleans key fields, drops and recreates the `transactions` table, and inserts the data into DuckDB with explicit SQL types.

`sql/queries.sql` contains reusable analytics queries for total spending, category spend, monthly spend, and top merchants. These queries power the core reporting layer directly, which keeps the project centered on SQL rather than hiding the main analysis in ad hoc dashboard code.

`src/analysis.py` loads named SQL queries from the SQL file, executes them against `finance.db`, and returns pandas DataFrames for the dashboard layer. It also adds two higher-value analytics functions:

- `detect_spending_anomalies()` pulls monthly category spending from DuckDB, computes a rolling 3-month mean and standard deviation, and flags months more than 2 standard deviations above normal.
- `forecast_spending()` takes monthly total spending, fits a Prophet forecast, and returns the next 30 days of predictions for Streamlit to plot.

`app/streamlit_app.py` renders the metrics, charts, anomaly table, and forecast chart in Streamlit.

## Data

The app uses a normalized personal finance transaction CSV with these columns:

```text
date, amount, category, merchant
```

The current dataset contains 688 spending transactions across 21 categories and 64 merchants, spanning January 2018 through September 2019. It was normalized from the public Personal Finance transaction dataset originally listed on Kaggle and mirrored in the supporting Power BI example repository.

## How to Run

1. Create and activate a virtual environment.

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies.

```bash
pip install -r requirements.txt
```

3. Load the CSV into DuckDB.

```bash
python src/load_data.py
```

4. Start the dashboard.

```bash
streamlit run app/streamlit_app.py
```

## Tests

Run the automated checks with:

```bash
python -m unittest discover -s tests -v
```

The tests validate:

- CSV cleaning and DuckDB loading
- SQL query outputs against pandas-calculated expected results
- realistic dataset coverage
- anomaly detection behavior on a controlled spike dataset
- forecast output shape and fallback behavior
- dashboard data preparation for anomaly and forecast sections

## Deployment

This project is ready for Streamlit Community Cloud.

1. Push the repository to GitHub.
2. Go to `https://share.streamlit.io`.
3. Create a new app from the GitHub repo.
4. Set the main file path to:

```text
app/streamlit_app.py
```

5. Deploy.

## Resume Value

This project highlights:

- SQL table creation and refresh workflows
- Analytical SQL queries with `GROUP BY`, `SUM`, `COUNT`, `ORDER BY`, and date functions
- A Python data pipeline that connects CSV data to a relational analytics workflow
- Rolling-window anomaly detection for spending spikes
- Time-series forecasting with Prophet
- Defensive production-minded handling around forecasting failures
- Streamlit dashboard development
- Automated tests covering the data pipeline, analysis layer, and dashboard preparation logic
