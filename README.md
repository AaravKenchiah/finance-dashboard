# Personal Finance Analytics Dashboard

## Overview

This project is a beginner-friendly but professional personal finance analytics dashboard built with Python, DuckDB, SQL, and Streamlit. It loads transaction data from CSV into a persistent DuckDB database, runs reusable SQL analytics queries, and presents the results in a clean dashboard.

The project is intentionally SQL-first so it clearly demonstrates database and querying skills for recruiters. Python is used to orchestrate loading, execute SQL, and connect the results to the dashboard.

## Tech Stack

- Python
- SQL
- DuckDB
- pandas
- Streamlit

## Features

- Persistent `finance.db` database file stored in the project root
- Safe table refresh logic for the `transactions` table
- Clean transaction schema with `DATE`, `DOUBLE`, and text columns
- Reusable SQL queries stored in [`sql/queries.sql`](/Users/aaravkenchiah/finance-dashboard/sql/queries.sql)
- SQL-driven analytics module in [`src/analysis.py`](/Users/aaravkenchiah/finance-dashboard/src/analysis.py)
- Streamlit dashboard with:
  - total spending metric
  - spending by category bar chart
  - monthly spending trend line chart
  - top merchants table

## Project Structure

```text
finance-dashboard/
├── data/transactions.csv
├── sql/queries.sql
├── src/load_data.py
├── src/analysis.py
├── app/streamlit_app.py
├── finance.db
├── requirements.txt
└── README.md
```

## How It Works

[`src/load_data.py`](/Users/aaravkenchiah/finance-dashboard/src/load_data.py) reads the CSV, cleans key fields, drops and recreates the `transactions` table, and inserts the data into DuckDB with explicit SQL types.

[`sql/queries.sql`](/Users/aaravkenchiah/finance-dashboard/sql/queries.sql) contains the reusable analytics queries. These queries power the application directly, which keeps the project strongly centered on SQL rather than pandas aggregations.

[`src/analysis.py`](/Users/aaravkenchiah/finance-dashboard/src/analysis.py) loads named SQL queries from the SQL file, executes them against `finance.db`, and returns pandas DataFrames for the dashboard layer.

[`app/streamlit_app.py`](/Users/aaravkenchiah/finance-dashboard/app/streamlit_app.py) renders the metrics and charts in Streamlit using the SQL query results.

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

## Resume Value

This project highlights:

- SQL table creation and refresh workflows
- Analytical SQL queries with `GROUP BY`, `SUM`, `COUNT`, `ORDER BY`, and date functions
- A Python data pipeline that connects CSV data to a relational analytics workflow
- A clean dashboard that turns SQL outputs into business-style insights
