"""Reducción en el origen sobre eventos reales de GH Archive."""

import json
from collections import Counter

from streaming_gh.reducir import EVENTOS_CON_PR, reducir_evento


def test_todos_los_eventos_de_la_muestra_se_reducen(eventos_crudos, eventos_reducidos):
    assert len(eventos_reducidos) == len(eventos_crudos) > 1000
    for crudo, red in zip(eventos_crudos, eventos_reducidos, strict=True):
        assert red["id"] == str(crudo["id"])
        assert red["type"] == crudo["type"]
        assert red["created_at"] == crudo["created_at"]
        assert red["repo"]["name"] == crudo["repo"]["name"]
        assert red["actor"]["login"] == crudo["actor"]["login"]


def test_la_reduccion_achica_el_mensaje_mas_de_5_veces(eventos_crudos, eventos_reducidos):
    crudo = sum(len(json.dumps(e)) for e in eventos_crudos)
    reducido = sum(len(json.dumps(e)) for e in eventos_reducidos)
    assert crudo / reducido > 5


def test_reducir_es_idempotente(eventos_reducidos):
    for e in eventos_reducidos:
        assert reducir_evento(e) == e


def test_campos_de_negocio_por_tipo(eventos_crudos, eventos_reducidos):
    tipos = Counter(e["type"] for e in eventos_reducidos)
    assert {"PushEvent", "WatchEvent", "ForkEvent", "PullRequestEvent"} <= set(tipos)
    for crudo, red in zip(eventos_crudos, eventos_reducidos, strict=True):
        p, r = crudo["payload"], red["payload"]
        if red["type"] == "PushEvent":
            assert r["size"] == p["size"] and r["ref"] == p["ref"]
        elif red["type"] in EVENTOS_CON_PR:
            assert r["number"] == p.get("number", p["pull_request"]["number"])
            assert r["language"] == p["pull_request"]["base"]["repo"]["language"]
        elif red["type"] == "ForkEvent":
            assert r["forkee"] == p["forkee"]["full_name"]
        elif red["type"] == "WatchEvent":
            assert r == {"action": "started"}


def test_org_ausente_queda_en_none():
    evento = {"id": 1, "type": "WatchEvent", "created_at": "2025-09-16T14:00:00Z",
              "actor": {"id": 9, "login": "ana", "avatar_url": "x"}, "repo": {"id": 5, "name": "a/b", "url": "u"},
              "payload": {"action": "started"}, "public": True}
    red = reducir_evento(evento)
    assert red["org"] is None
    assert red["actor"] == {"id": 9, "login": "ana"}
    assert red["id"] == "1"
