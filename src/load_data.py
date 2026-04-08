from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


DB_PATH = Path(__file__).resolve().parents[1] / "finance.db"
CSV_PATH = Path(__file__).resolve().parents[1] / "data" / "transactions.csv"


def get_connection(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Return a connection to the persistent DuckDB database file."""
    return duckdb.connect(str(db_path))


def prepare_dataframe(csv_path: Path = CSV_PATH) -> pd.DataFrame:
    """Read the CSV file and normalize values before loading to DuckDB."""
    dataframe = pd.read_csv(csv_path).dropna(how="all")
    dataframe["date"] = pd.to_datetime(dataframe["date"], errors="coerce").dt.date
    dataframe["amount"] = pd.to_numeric(dataframe["amount"], errors="coerce").astype(float)
    dataframe["category"] = dataframe["category"].fillna("Uncategorized").astype(str).str.strip()
    dataframe["merchant"] = dataframe["merchant"].fillna("Unknown").astype(str).str.strip()

    cleaned_dataframe = dataframe.dropna(subset=["date", "amount"]).copy()
    return cleaned_dataframe[["date", "amount", "category", "merchant"]]


def recreate_transactions_table(
    connection: duckdb.DuckDBPyConnection,
    dataframe: pd.DataFrame,
) -> int:
    """Drop and recreate the transactions table with a clean schema."""
    connection.register("transactions_source", dataframe)
    connection.execute("DROP TABLE IF EXISTS transactions")
    connection.execute(
        """
        CREATE TABLE transactions (
            date DATE,
            amount DOUBLE,
            category VARCHAR,
            merchant VARCHAR
        )
        """
    )
    connection.execute(
        """
        INSERT INTO transactions (date, amount, category, merchant)
        SELECT
            CAST(date AS DATE) AS date,
            CAST(amount AS DOUBLE) AS amount,
            category,
            merchant
        FROM transactions_source
        """
    )
    connection.unregister("transactions_source")
    connection.execute("ANALYZE transactions")
    return connection.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]


def load_transactions(
    db_path: Path = DB_PATH,
    csv_path: Path = CSV_PATH,
) -> int:
    """Load cleaned transaction data from CSV into finance.db."""
    dataframe = prepare_dataframe(csv_path)
    with get_connection(db_path) as connection:
        return recreate_transactions_table(connection, dataframe)


if __name__ == "__main__":
    inserted_rows = load_transactions()
    print(f"Loaded {inserted_rows} rows into {DB_PATH}")
