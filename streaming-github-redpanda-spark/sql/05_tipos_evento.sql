-- Mezcla de tipos de evento y actividad máxima en 5 minutos.
SELECT tipo,
       SUM(eventos) AS eventos,
       ROUND(100.0 * SUM(eventos) / SUM(SUM(eventos)) OVER (), 2) AS pct,
       MAX(eventos) AS max_en_5_min,
       ROUND(AVG(eventos), 0) AS media_en_5_min
FROM actividad_5min
GROUP BY tipo
ORDER BY eventos DESC;
