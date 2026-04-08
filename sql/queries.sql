-- name: total_spending
SELECT
    COALESCE(SUM(amount), 0) AS total_spending
FROM transactions;

-- name: spending_by_category
SELECT
    category,
    SUM(amount) AS total_spent,
    COUNT(*) AS transaction_count
FROM transactions
GROUP BY category
ORDER BY total_spent DESC, category ASC;

-- name: monthly_spending_trend
SELECT
    DATE_TRUNC('month', date) AS month,
    SUM(amount) AS total_spent,
    COUNT(*) AS transaction_count
FROM transactions
GROUP BY month
ORDER BY month;

-- name: top_merchants
SELECT
    merchant,
    SUM(amount) AS total_spent,
    COUNT(*) AS transaction_count
FROM transactions
GROUP BY merchant
ORDER BY total_spent DESC, merchant ASC
LIMIT ?;
