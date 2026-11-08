"""Descarga reanudable con un servidor HTTP simulado (sin internet)."""

import io

import pytest

from clima_mds.descarga import descargar_reanudable

CONTENIDO = bytes(range(256)) * 4000  # ~1 MB


class RespuestaFalsa(io.BytesIO):
    def __init__(self, datos, status=200, cabeceras=None):
        super().__init__(datos)
        self.status = status
        self.headers = cabeceras or {}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


class ServidorFalso:
    def __init__(self, contenido=CONTENIDO, etag="v1", corta_en=None):
        self.contenido, self.etag, self.corta_en = contenido, etag, corta_en
        self.peticiones = []

    def __call__(self, req, timeout=None):
        rango = req.get_header("Range")
        self.peticiones.append((req.get_method(), rango))
        if req.get_method() == "HEAD":
            return RespuestaFalsa(b"", cabeceras={"Content-Length": str(len(self.contenido)), "ETag": f'"{self.etag}"'})
        inicio = int(rango.split("=")[1].rstrip("-")) if rango else 0
        datos = self.contenido[inicio:]
        if self.corta_en is not None:  # simula un apagón: el servidor entrega solo una parte
            datos, self.corta_en = datos[: self.corta_en], None
        return RespuestaFalsa(datos, status=206 if rango else 200)


def test_descarga_completa_y_reutiliza(tmp_path):
    srv = ServidorFalso()
    destino = tmp_path / "2024.csv.gz"
    r1 = descargar_reanudable("http://x/2024.csv.gz", destino, abrir=srv)
    assert destino.read_bytes() == CONTENIDO and not r1.reutilizado
    r2 = descargar_reanudable("http://x/2024.csv.gz", destino, abrir=srv)
    assert r2.reutilizado and r2.bytes_descargados == 0
    assert [m for m, _ in srv.peticiones] == ["HEAD", "GET", "HEAD"]


def test_reanuda_desde_el_parcial(tmp_path):
    srv = ServidorFalso(corta_en=300_000)
    destino = tmp_path / "2024.csv.gz"
    with pytest.raises(OSError, match="incompleta"):
        descargar_reanudable("http://x/2024.csv.gz", destino, abrir=srv)
    assert (tmp_path / "2024.csv.gz.part").stat().st_size == 300_000
    r = descargar_reanudable("http://x/2024.csv.gz", destino, abrir=srv)
    assert destino.read_bytes() == CONTENIDO
    assert r.bytes_descargados == len(CONTENIDO) - 300_000
    assert srv.peticiones[-1] == ("GET", "bytes=300000-")


def test_vuelve_a_bajar_si_el_archivo_remoto_cambio(tmp_path):
    destino = tmp_path / "2026.csv.gz"
    descargar_reanudable("http://x/2026.csv.gz", destino, abrir=ServidorFalso())
    nuevo = CONTENIDO + b"dia nuevo"
    r = descargar_reanudable("http://x/2026.csv.gz", destino, abrir=ServidorFalso(contenido=nuevo, etag="v2"))
    assert not r.reutilizado and destino.read_bytes() == nuevo
