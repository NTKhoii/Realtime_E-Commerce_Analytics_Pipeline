INSERT INTO agg_daily_top_products (sk_date, product_id, view_count, rank)
SELECT sk_date, product_id, view_count, rank FROM (
    SELECT
        f.sk_date,
        p.product_id,
        COUNT(*) AS view_count,
        ROW_NUMBER() OVER (PARTITION BY f.sk_date ORDER BY COUNT(*) DESC) AS rank
    FROM fact_product_views f
    JOIN dim_product p ON p.sk_product = f.sk_product
    WHERE f.sk_date = %(sk_date)s
    GROUP BY f.sk_date, p.product_id
) ranked
WHERE rank <= %(top_n)s
ON CONFLICT (sk_date, product_id) DO UPDATE SET
    view_count = EXCLUDED.view_count,
    rank = EXCLUDED.rank;