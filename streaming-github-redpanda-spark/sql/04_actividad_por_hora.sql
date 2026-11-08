-- Actividad por hora del día (UTC) y tipo de evento, promediada sobre los días observados.
SELECT HOUR(ventana_inicio) AS hora_utc,
       tipo,
       ROUND(SUM(eventos) / COUNT(DISTINCT CAST(ventana_inicio AS DATE)), 0) AS eventos_promedio
FROM actividad_5min
GROUP BY ALL
ORDER BY hora_utc, eventos_promedio DESC;
