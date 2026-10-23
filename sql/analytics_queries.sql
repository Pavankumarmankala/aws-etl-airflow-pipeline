-- AWS ETL Pipeline — Redshift analytics over the star schema
-- Tables: fact_orders, dim_customer, dim_product, dim_date
-- Dialect: Amazon Redshift (standard SQL; all 10 queries were also
-- executed successfully against the local SQLite warehouse).

-- 1. Daily revenue and order volume
SELECT d.full_date, COUNT(*) AS orders, SUM(f.amount) AS revenue,
       ROUND(AVG(f.amount), 2) AS avg_order_value
FROM fact_orders f JOIN dim_date d ON f.date_key = d.date_key
GROUP BY 1 ORDER BY 1;

-- 2. Revenue by product category
SELECT p.category, COUNT(*) AS orders, SUM(f.amount) AS revenue
FROM fact_orders f JOIN dim_product p ON f.product_id = p.product_id
GROUP BY 1 ORDER BY revenue DESC;

-- 3. Top 10 products by revenue
SELECT p.product_name, p.category, SUM(f.quantity) AS units_sold,
       SUM(f.amount) AS revenue
FROM fact_orders f JOIN dim_product p ON f.product_id = p.product_id
GROUP BY 1, 2 ORDER BY revenue DESC LIMIT 10;

-- 4. Revenue by sales channel
SELECT channel, COUNT(*) AS orders, SUM(amount) AS revenue,
       ROUND(100.0 * SUM(amount) / SUM(SUM(amount)) OVER (), 2) AS revenue_pct
FROM fact_orders GROUP BY 1 ORDER BY revenue DESC;

-- 5. Top 20 customers by spend
SELECT customer_id, COUNT(*) AS orders, SUM(amount) AS total_spend,
       ROUND(AVG(amount), 2) AS avg_order_value
FROM fact_orders GROUP BY 1 ORDER BY total_spend DESC LIMIT 20;

-- 6. Returns rate by category (status = 'returned')
SELECT p.category, COUNT(*) AS orders,
       SUM(CASE WHEN f.status = 'returned' THEN 1 ELSE 0 END) AS returns,
       ROUND(100.0 * SUM(CASE WHEN f.status = 'returned' THEN 1 ELSE 0 END)
             / COUNT(*), 2) AS return_rate_pct
FROM fact_orders f JOIN dim_product p ON f.product_id = p.product_id
GROUP BY 1 ORDER BY return_rate_pct DESC;

-- 7. Day-of-week pattern
SELECT d.day_of_week, COUNT(*) AS orders, SUM(f.amount) AS revenue
FROM fact_orders f JOIN dim_date d ON f.date_key = d.date_key
GROUP BY 1 ORDER BY 2 DESC;

-- 8. Load reconciliation: warehouse rows per load_date (pipeline health)
SELECT load_date, COUNT(*) AS fact_rows, SUM(amount) AS loaded_revenue
FROM fact_orders GROUP BY 1 ORDER BY 1;

-- 9. Repeat-customer share per day
SELECT load_date,
       COUNT(DISTINCT customer_id) AS active_customers,
       COUNT(*) AS orders,
       ROUND(1.0 * COUNT(*) / COUNT(DISTINCT customer_id), 2) AS orders_per_customer
FROM fact_orders GROUP BY 1 ORDER BY 1;

-- 10. New vs returning customers (first_seen_date = load_date means new)
SELECT f.load_date,
       SUM(CASE WHEN c.first_seen_date = f.load_date THEN 1 ELSE 0 END) AS new_customer_orders,
       SUM(CASE WHEN c.first_seen_date <> f.load_date THEN 1 ELSE 0 END) AS returning_customer_orders
FROM fact_orders f JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY 1 ORDER BY 1;
