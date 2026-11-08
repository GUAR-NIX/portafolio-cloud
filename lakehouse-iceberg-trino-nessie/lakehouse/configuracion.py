"""Parámetros del proyecto (sin credenciales: las de MinIO son locales y vienen del entorno)."""

# Final del Mundial 2026 (domingo 19-jul-2026, MetLife Stadium, 19:00 UTC). Los tres primeros días entran a `main`
# directamente; el cuarto se carga en una rama `dev` de Nessie y solo se fusiona si pasa los controles.
DIAS_MAIN = ["2026-07-18", "2026-07-19", "2026-07-20"]
DIA_RAMA = "2026-07-21"

IDIOMAS = {"en": "en", "en.m": "en", "es": "es", "es.m": "es"}
PLATAFORMAS = {"en": "escritorio", "en.m": "movil", "es": "escritorio", "es.m": "movil"}
