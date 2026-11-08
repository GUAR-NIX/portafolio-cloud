"""La muestra versionada no contiene correos de terceros y el anonimizador es estable."""

import gzip
import importlib.util
import re
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
CORREO = re.compile(rb"[A-Za-z0-9._%+\-]+@(?:[A-Za-z0-9\-]+\.)+[A-Za-z]{2,}")


def _crear_muestra():
    spec = importlib.util.spec_from_file_location("crear_muestra", RAIZ / "scripts" / "crear_muestra.py")
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


def test_la_muestra_no_trae_correos_reales():
    modulo = _crear_muestra()
    permitidos = modulo.CORREOS_DE_SISTEMA
    with gzip.open(RAIZ / "data" / "muestra" / "gharchive_muestra.json.gz", "rb") as f:
        correos = {c for linea in f for c in CORREO.findall(linea)}
    ajenos = {c for c in correos if not c.endswith(b"@ejemplo.invalid") and c.lower() not in permitidos}
    assert not ajenos


def test_anonimizar_es_estable_y_respeta_las_direcciones_de_sistema():
    modulo = _crear_muestra()
    linea = b'{"email":"Persona.Real@empresa.com","url":"git@github.com:a/b.git","otra":"persona.real@empresa.com"}'
    a = modulo.anonimizar_linea(linea)
    assert b"empresa.com" not in a
    assert b"git@github.com:a/b.git" in a
    correos = re.findall(rb"u[0-9a-f]{12}@ejemplo\.invalid", a)
    assert len(correos) == 2 and correos[0] == correos[1]
    assert modulo.anonimizar_linea(a) == a
