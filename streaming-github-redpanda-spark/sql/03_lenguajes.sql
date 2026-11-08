-- Lenguajes con más actividad de pull requests (lenguaje del repositorio base del PR).
SELECT lenguaje,
       SUM(eventos_pr)::BIGINT AS eventos_pr,
       SUM(prs_abiertos)::BIGINT AS prs_abiertos,
       SUM(prs_fusionados)::BIGINT AS prs_fusionados,
       ROUND(100.0 * SUM(eventos_pr) / SUM(SUM(eventos_pr)) OVER (), 2) AS pct_eventos_pr,
       ROUND(AVG(repos_aprox), 0) AS repos_por_hora
FROM lenguajes_hora
GROUP BY lenguaje
ORDER BY eventos_pr DESC
LIMIT 15;
