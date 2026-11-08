-- Picos de estrellas detectados en streaming: repos que se dispararon frente a su línea base de 24 h.
SELECT ventana_inicio, repo_nombre, estrellas, forks, lenguaje, linea_base, veces_sobre_base
FROM picos_repos
ORDER BY estrellas DESC
LIMIT 25;
