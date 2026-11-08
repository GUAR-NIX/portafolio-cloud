import gzip
import io
import json
from datetime import datetime

import pytest

from lakehouse import descarga
from lakehouse.descarga import descargar_hora, horas_de_dias, linea_de_proyecto, url_hora

ORIGEN = b"\n".join([b"en Lionel_Messi 10 0", b"de Berlin 5 0", b"es.m Copa 30 0", b"en.m Foo 1 0",
                     b"commons.m File:X.jpg 20 0", b"en.wikibooks Y 9 0", b"en.m Bar 5 0", b"es roto"]) + b"\n"


class Respuesta(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


@pytest.fixture
def servidor(monkeypatch):
    class Llamadas(list):
        fallar_primero = False

    llamadas = Llamadas()

    def urlopen(req, timeout):
        llamadas.append(req.full_url)
        assert req.headers["User-agent"].startswith("lakehouse-iceberg")
        if len(llamadas) == 1 and llamadas.fallar_primero:
            raise OSError("conexión reiniciada")
        return Respuesta(gzip.compress(ORIGEN))

    monkeypatch.setattr(descarga.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(descarga.time, "sleep", lambda s: None)
    return llamadas


def test_url_y_horas():
    assert url_hora(datetime(2026, 7, 19, 9)).endswith("/2026/2026-07/pageviews-20260719-090000.gz")
    horas = horas_de_dias(["2026-07-18", "2026-07-19"])
    assert len(horas) == 48 and horas[0] == datetime(2026, 7, 18) and horas[-1] == datetime(2026, 7, 19, 23)


def test_filtro_de_proyecto():
    assert linea_de_proyecto(b"en Foo 1 0") and linea_de_proyecto(b"es.m Foo 1 0")
    assert not linea_de_proyecto(b"en.wikibooks Foo 1 0") and not linea_de_proyecto(b"de Foo 1 0")
    assert not linea_de_proyecto(b"") and not linea_de_proyecto(b"enFoo")


def test_descarga_filtra_y_reutiliza(tmp_path, servidor):
    h = datetime(2026, 7, 19, 19)
    r = descargar_hora(h, tmp_path)
    assert (r.lineas_origen, r.lineas_guardadas, r.reutilizado) == (8, 4, False)
    # umbral de 5 vistas: Foo (1) queda fuera; la línea rota pasa para que la limpieza la cuente
    assert gzip.decompress((tmp_path / r.archivo).read_bytes()).splitlines() == [
        b"en Lionel_Messi 10 0", b"es.m Copa 30 0", b"en.m Bar 5 0", b"es roto"]
    assert (r.vistas_proyectos, r.vistas_guardadas) == (46, 45)
    assert json.loads((tmp_path / (r.archivo + ".json")).read_text())["sha256"] == r.sha256
    r2 = descargar_hora(h, tmp_path)
    assert r2.reutilizado and len(servidor) == 1  # segunda vez no toca la red


def test_archivo_alterado_se_vuelve_a_bajar(tmp_path, servidor):
    h = datetime(2026, 7, 19, 19)
    r = descargar_hora(h, tmp_path)
    (tmp_path / r.archivo).write_bytes(b"corrupto")
    assert not descargar_hora(h, tmp_path).reutilizado and len(servidor) == 2


def test_reintenta_y_no_deja_parciales(tmp_path, servidor):
    servidor.fallar_primero = True
    r = descargar_hora(datetime(2026, 7, 19, 19), tmp_path)
    assert r.lineas_guardadas == 4 and len(servidor) == 2
    assert not list(tmp_path.glob("*.part"))
