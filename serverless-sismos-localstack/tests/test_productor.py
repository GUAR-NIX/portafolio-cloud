"""Modo periódico del productor (`--cada N`), con reloj, descarga y subida simulados: sin red ni boto3."""

import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]


def cargar_productor():
    spec = importlib.util.spec_from_file_location("productor", RAIZ / "scripts" / "productor.py")
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


class Entorno:
    """Reloj falso que avanza solo al dormir, y registro de descargas y subidas."""

    def __init__(self, fallos=()):
        self.t = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
        self.dormidos = []
        self.descargas = 0
        self.subidas = []
        self.mensajes = []
        self.fallos = set(fallos)

    def ahora(self):
        return self.t

    def dormir(self, segundos):
        self.dormidos.append(segundos)
        self.t += timedelta(seconds=segundos)

    def descargar(self, destino):
        self.descargas += 1
        if self.descargas in self.fallos:
            raise ConnectionError("sin red")

    def subir(self, ruta, clave):
        self.subidas.append((ruta, clave))


def correr(entorno, **kw):
    productor = cargar_productor()
    return productor.producir(
        Path("data/raw/usgs_hora.geojson"),
        entorno.subir,
        dormir=entorno.dormir,
        ahora=entorno.ahora,
        registrar=entorno.mensajes.append,
        **kw,
    )


def test_sin_cada_sube_una_vez_sin_descargar_ni_dormir():
    e = Entorno()
    claves = correr(e, descargar=e.descargar)
    assert claves == ["entrada/usgs-20261025T120000Z.geojson"]
    assert e.descargas == 0 and e.dormidos == []


def test_cada_descarga_sube_y_duerme_entre_vueltas():
    e = Entorno()
    claves = correr(e, cada_minutos=5, descargar=e.descargar, iteraciones=3)
    assert e.descargas == 3 and len(e.subidas) == 3
    assert e.dormidos == [300, 300]  # no duerme después de la última vuelta
    assert claves == [
        "entrada/usgs-20261025T120000Z.geojson",
        "entrada/usgs-20261025T120500Z.geojson",
        "entrada/usgs-20261025T121000Z.geojson",
    ]


def test_una_descarga_fallida_no_sube_nada_y_el_ciclo_sigue():
    e = Entorno(fallos={2})
    claves = correr(e, cada_minutos=1, descargar=e.descargar, iteraciones=3)
    assert e.descargas == 3
    assert len(claves) == 2 and len(e.subidas) == 2
    assert any("descarga fallida" in m for m in e.mensajes)


def test_ctrl_c_termina_el_ciclo_con_lo_ya_subido():
    e = Entorno()

    def dormir_y_cortar(segundos):
        e.dormir(segundos)
        if len(e.dormidos) == 2:
            raise KeyboardInterrupt

    productor = cargar_productor()
    claves = productor.producir(
        Path("x.geojson"),
        e.subir,
        cada_minutos=2,
        descargar=e.descargar,
        dormir=dormir_y_cortar,
        ahora=e.ahora,
        registrar=e.mensajes.append,
    )
    assert len(claves) == 2
    assert e.mensajes[-1] == "detenido por el usuario"
