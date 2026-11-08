-- Repos en tendencia: los 15 con más estrellas nuevas en todo el periodo, con su mejor hora.
WITH por_repo AS (
    SELECT repo_nombre,
           SUM(estrellas)::BIGINT AS estrellas, SUM(forks)::BIGINT AS forks, SUM(prs_abiertos)::BIGINT AS prs_abiertos,
           SUM(eventos)::BIGINT AS eventos, MAX(lenguaje) AS lenguaje,
           ARG_MAX(ventana_inicio, estrellas) AS mejor_hora, MAX(estrellas) AS estrellas_mejor_hora
    FROM repos_hora
    GROUP BY repo_nombre
)
SELECT * FROM por_repo ORDER BY estrellas DESC LIMIT 15;
