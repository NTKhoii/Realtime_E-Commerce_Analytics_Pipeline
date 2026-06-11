INSERT INTO agg_daily_views_by_country (sk_date, country_code, country_name, view_count, unique_products)
SELECT
    f.sk_date,
    COALESCE(l.country_code, 'UNKNOWN') AS country_code,
    COALESCE(l.country_name, 'UNKNOWN') AS country_name,
    COUNT(*) AS view_count,
    COUNT(DISTINCT f.sk_product) AS unique_products
FROM fact_product_views f
LEFT JOIN dim_location l ON l.sk_location = f.sk_location
WHERE f.sk_date = %(sk_date)s
GROUP BY f.sk_date, l.country_code, l.country_name
ON CONFLICT (sk_date, country_code) DO UPDATE SET
    country_name = EXCLUDED.country_name,
    view_count = EXCLUDED.view_count,
    unique_products = EXCLUDED.unique_products;