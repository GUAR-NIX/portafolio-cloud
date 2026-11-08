from descargar_datos import transformar


def test_transformar_respetando_unidades():
    documento = {
        "daily": {
            "time": ["2024-01-01"],
            "temperature_2m_mean": [14.2],
            "temperature_2m_max": [20.1],
            "temperature_2m_min": [8.3],
            "precipitation_sum": [1.5],
        }
    }
    fila = transformar("Quito", documento).iloc[0]
    assert fila["ciudad"] == "Quito"
    assert fila["precipitacion_mm"] == 1.5

