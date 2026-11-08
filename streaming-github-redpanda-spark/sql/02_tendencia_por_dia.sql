-- Top 5 repos por estrellas de cada día (ranking con ventana sobre la tabla oro).
WITH diario AS (
    SELECT CAST(ventana_inicio AS DATE) AS dia, repo_nombre,
           SUM(estrellas)::BIGINT AS estrellas, SUM(forks)::BIGINT AS forks, MAX(lenguaje) AS lenguaje
    FROM repos_hora
    GROUP BY ALL
)
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dia ORDER BY estrellas DESC, forks DESC) AS puesto FROM diario
) WHERE puesto <= 5
ORDER BY dia, puesto;
