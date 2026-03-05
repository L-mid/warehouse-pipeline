-- revenue is by day/country (from `fact_orders`)
-- grain is (day, country)
-- paid rule is (status) = 'paid'

SELECT
  fo.date AS day,
  fo.country AS country,
  SUM(fo.total_usd) AS paid_revenue_usd,
  COUNT(*) AS paid_orders
FROM fact_orders fo
WHERE fo.status = 'paid'
GROUP BY 1, 2
ORDER BY 1 ASC, 2 ASC;