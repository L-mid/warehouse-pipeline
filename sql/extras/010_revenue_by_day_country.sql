-- revenue is by day/country (from fact_orders)
-- grain is (day, country)
-- paid rule is lower(trim(status)) = 'paid'

SELECT
  fo.date AS day,
  upper(trim(fo.country)) AS country,
  sum(fo.total_usd) AS paid_revenue_usd,
  count(*) AS paid_orders
FROM fact_orders fo
WHERE lower(trim(fo.status)) = 'paid'
GROUP BY 1, 2
ORDER BY 1 ASC, 2 ASC;