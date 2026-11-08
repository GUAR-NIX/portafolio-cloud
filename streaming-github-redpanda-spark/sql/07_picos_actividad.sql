-- Picos de actividad global por tipo de evento (z >= 3 frente a la hora anterior).
SELECT ventana_inicio, tipo, eventos, media_base, desv_base, z
FROM picos_actividad
ORDER BY z DESC
LIMIT 25;
