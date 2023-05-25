SELECT manufacturer, COUNT(*) AS count
FROM (
  SELECT *
  FROM public.planes p
  INNER JOIN public.flights f ON p.tailnum = f.tailnum
) AS manufacturer_planes
GROUP BY manufacturer
ORDER BY count DESC
LIMIT 1;