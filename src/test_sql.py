import duckdb
con = duckdb.connect("finance.db")
df = con.execute("""SELECT SUM(AMOUNT) AS total_spending FROM transactions""").df()
print(df)
df1 = con.execute("""SELECT CATEGORY, SUM(AMOUNT) AS total_spent FROM transactions GROUP BY CATEGORY ORDER BY total_spent DESC""").df()
print(df1)