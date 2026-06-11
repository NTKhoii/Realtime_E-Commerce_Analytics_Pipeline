INSERT INTO agg_daily_traffic_breakdown (sk_date, traffic_type, view_count)
SELECT
    f.sk_date,
    COALESCE(t.traffic_type, 'UNKNOWN') AS traffic_type,
    COUNT(*) AS view_count
FROM fact_product_views f
LEFT JOIN dim_traffic_source t ON t.sk_traffic = f.sk_traffic
WHERE f.sk_date = %(sk_date)s
GROUP BY f.sk_date, t.traffic_type
ON CONFLICT (sk_date, traffic_type) DO UPDATE SET
    view_count = EXCLUDED.view_count;