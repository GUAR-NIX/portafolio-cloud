import json

import pytest

from lakehouse import nessie
from lakehouse.nessie import ErrorNessie, Nessie


class Falso:
    """Simula el servidor: registra (método, url, cuerpo) y responde según la ruta."""

    def __init__(self):
        self.llamadas = []
        self.refs = {"main": "aaa"}

    def __call__(self, req, timeout):
        cuerpo = json.loads(req.data) if req.data else None
        self.llamadas.append((req.get_method(), req.full_url, cuerpo))
        url = req.full_url.split("/api/v2")[1]
        if req.get_method() == "GET" and url.startswith("/trees/") and "/" not in url[7:]:
            nombre = url[7:]
            if nombre not in self.refs:
                raise nessie.urllib.error.HTTPError(req.full_url, 404, "no", {}, _Cuerpo(b"not found"))
            return _Cuerpo(json.dumps({"reference": {"type": "BRANCH", "name": nombre,
                                                     "hash": self.refs[nombre]}}).encode())
        if req.get_method() == "POST" and url.startswith("/trees?"):
            self.refs["dev"] = cuerpo["hash"]
            return _ref("dev", cuerpo["hash"])
        if req.get_method() == "PUT":
            return _ref("dev", cuerpo["hash"])
        return _Cuerpo(b"{}")


def _ref(nombre, h):
    return _Cuerpo(json.dumps({"reference": {"type": "BRANCH", "name": nombre, "hash": h}}).encode())


class _Cuerpo:
    def __init__(self, b):
        self.b = b

    def read(self):
        return self.b

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def close(self):
        pass


@pytest.fixture
def falso(monkeypatch):
    f = Falso()
    monkeypatch.setattr(nessie.urllib.request, "urlopen", f)
    return f


def test_asegurar_rama_la_crea_desde_main(falso):
    n = Nessie("http://nessie:19120/")
    ref = n.asegurar_rama("dev")
    assert ref["hash"] == "aaa"
    metodo, url, cuerpo = falso.llamadas[-1]
    assert metodo == "POST" and url.endswith("/api/v2/trees?name=dev&type=BRANCH") and cuerpo["name"] == "main"
    n.asegurar_rama("dev")
    assert falso.llamadas[-1][0] == "GET"  # ya existe: no se vuelve a crear


def test_fusionar_usa_hashes_esperados(falso):
    falso.refs["dev"] = "bbb"
    Nessie().fusionar("dev", "main", "día validado")
    metodo, url, cuerpo = falso.llamadas[-1]
    assert metodo == "POST" and url.endswith("/trees/main@aaa/history/merge")
    assert cuerpo == {"fromRefName": "dev", "fromHash": "bbb", "message": "día validado"}


def test_reasignar_mueve_dev_a_main(falso):
    falso.refs["dev"] = "bbb"
    Nessie().reasignar("dev", "main")
    metodo, url, cuerpo = falso.llamadas[-1]
    assert metodo == "PUT" and "/trees/dev@bbb?type=BRANCH" in url and cuerpo["hash"] == "aaa"


def test_error_http_se_convierte(falso):
    with pytest.raises(ErrorNessie, match="404"):
        Nessie().ref("no-existe")
