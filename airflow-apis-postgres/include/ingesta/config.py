"""Parámetros del pipeline: ciudades, variables de clima, indicadores del Banco Mundial y nombres de tablas.

Este módulo no importa Airflow: lo usan los DAG, las pruebas y los scripts del equipo por igual.
"""
from __future__ import annotations

from datetime import date

ZONA_HORARIA = "America/Guayaquil"
CONEXION_DATOS = "datos_postgres"          # conexión de Airflow (AIRFLOW_CONN_DATOS_POSTGRES)

# --------------------------------------------------------------------------------------------------------------
# Clima: 13 ciudades de las 4 regiones del Ecuador
# --------------------------------------------------------------------------------------------------------------
CIUDADES: list[dict] = [
    {"ciudad": "Quito", "provincia": "Pichincha", "region": "Sierra", "latitud": -0.2299, "longitud": -78.5249},
    {"ciudad": "Guayaquil", "provincia": "Guayas", "region": "Costa", "latitud": -2.1894, "longitud": -79.8891},
    {"ciudad": "Cuenca", "provincia": "Azuay", "region": "Sierra", "latitud": -2.9001, "longitud": -79.0059},
    {"ciudad": "Santo Domingo", "provincia": "Santo Domingo de los Tsáchilas", "region": "Costa",
     "latitud": -0.2530, "longitud": -79.1754},
    {"ciudad": "Machala", "provincia": "El Oro", "region": "Costa", "latitud": -3.2581, "longitud": -79.9554},
    {"ciudad": "Manta", "provincia": "Manabí", "region": "Costa", "latitud": -0.9677, "longitud": -80.7089},
    {"ciudad": "Esmeraldas", "provincia": "Esmeraldas", "region": "Costa", "latitud": 0.9682, "longitud": -79.6517},
    {"ciudad": "Ambato", "provincia": "Tungurahua", "region": "Sierra", "latitud": -1.2491, "longitud": -78.6168},
    {"ciudad": "Loja", "provincia": "Loja", "region": "Sierra", "latitud": -3.9931, "longitud": -79.2042},
    {"ciudad": "Ibarra", "provincia": "Imbabura", "region": "Sierra", "latitud": 0.3517, "longitud": -78.1223},
    {"ciudad": "Puyo", "provincia": "Pastaza", "region": "Amazonía", "latitud": -1.4924, "longitud": -78.0024},
    {"ciudad": "Nueva Loja", "provincia": "Sucumbíos", "region": "Amazonía", "latitud": 0.0847, "longitud": -76.8828},
    {"ciudad": "Puerto Ayora", "provincia": "Galápagos", "region": "Insular", "latitud": -0.7432,
     "longitud": -90.3138},
]

# variable de Open-Meteo -> columna en staging. Son 9 (< 10) a propósito: con más de 10 variables
# Open-Meteo cobra cada ubicación como varias llamadas de la cuota gratuita.
VARIABLES_CLIMA: dict[str, str] = {
    "temperature_2m_max": "temp_max_c",
    "temperature_2m_min": "temp_min_c",
    "temperature_2m_mean": "temp_media_c",
    "precipitation_sum": "precipitacion_mm",
    "precipitation_hours": "horas_precipitacion",
    "wind_speed_10m_max": "viento_max_kmh",
    "shortwave_radiation_sum": "radiacion_mj_m2",
    "et0_fao_evapotranspiration": "evapotranspiracion_mm",
    "relative_humidity_2m_mean": "humedad_media_pct",
}

URL_ARCHIVO = "https://archive-api.open-meteo.com/v1/archive"     # reanálisis ERA5 (definitivo)
URL_PRONOSTICO = "https://api.open-meteo.com/v1/forecast"         # modelos de pronóstico (provisional)
DIAS_RETRASO_ARCHIVO = 5        # días más recientes que esto se piden a la API de pronóstico
SEGUNDOS_ENTRE_LLAMADAS = 10    # ritmo mínimo: 6 peticiones/min × 13 ciudades = 4.680 llamadas/h (< 5.000/h)
INICIO_CLIMA = date(2025, 10, 1)

# --------------------------------------------------------------------------------------------------------------
# Banco Mundial: 10 indicadores para Ecuador, 7 países de comparación y el agregado de América Latina
# --------------------------------------------------------------------------------------------------------------
URL_BANCO_MUNDIAL = "https://api.worldbank.org/v2"
PAISES = ["ECU", "COL", "PER", "BOL", "CHL", "BRA", "ARG", "MEX", "LCN"]
AGREGADO_REGION = "LCN"         # Latin America & Caribbean

# código -> (columna en el mart, mínimo, máximo) · los rangos alimentan los controles de calidad
INDICADORES: dict[str, tuple[str, float, float]] = {
    "SP.POP.TOTL": ("poblacion", 1e5, 1e9),
    "SP.URB.TOTL.IN.ZS": ("poblacion_urbana_pct", 0, 100),
    "NY.GDP.PCAP.CD": ("pib_per_capita_usd", 100, 100_000),
    "NY.GDP.MKTP.KD.ZG": ("crecimiento_pib_pct", -30, 30),
    "NV.AGR.TOTL.ZS": ("agro_pct_pib", 0, 100),
    "EG.ELC.ACCS.ZS": ("acceso_electricidad_pct", 0, 100),
    "EN.GHG.CO2.PC.CE.AR5": ("co2_t_per_capita", 0, 50),
    "SL.UEM.TOTL.ZS": ("desempleo_pct", 0, 100),
    "FP.CPI.TOTL.ZG": ("inflacion_pct", -50, 5000),
    "SI.POV.GINI": ("gini", 15, 80),
}
INICIO_BANCO_MUNDIAL = 2000


def ciudades_por_nombre() -> dict[str, dict]:
    return {c["ciudad"]: c for c in CIUDADES}
