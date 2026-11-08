"""Reducción de eventos de GH Archive en el origen.

Un evento crudo pesa en promedio 4,2 KB (un PullRequestEvent trae el objeto pull_request completo, ~20 KB). El
productor publica el sobre del evento (id, tipo, actor, repo, org, fecha) y solo los campos del payload que usa el
negocio, como haría un conector de origen que proyecta columnas. El mensaje queda en ~0,4 KB y la forma sigue
siendo la de GitHub (actor/repo/org anidados), así que bronce sigue siendo "crudo" respecto a lo que llega al tema.
"""

from __future__ import annotations

from typing import Any

# Eventos cuyo payload trae pull_request (y con él el lenguaje del repositorio base)
EVENTOS_CON_PR = {"PullRequestEvent", "PullRequestReviewEvent", "PullRequestReviewCommentEvent"}


def _persona(obj: dict | None) -> dict | None:
    if not obj:
        return None
    return {"id": obj.get("id"), "login": obj.get("login")}


def reducir_payload(tipo: str, payload: dict | None) -> dict[str, Any]:
    """Se queda con los campos del payload que usan plata y oro. Idempotente: reducir algo ya reducido no cambia."""
    p = payload or {}
    r: dict[str, Any] = {}
    if "action" in p:
        r["action"] = p["action"]
    if tipo in ("PushEvent", "CreateEvent", "DeleteEvent"):
        r["ref"] = p.get("ref")
        if tipo == "PushEvent":
            r["size"] = p.get("size")
            r["distinct_size"] = p.get("distinct_size")
        else:
            r["ref_type"] = p.get("ref_type")
    if tipo in EVENTOS_CON_PR:
        pr = p.get("pull_request") or {}
        r["number"] = p.get("number", pr.get("number"))
        if "pull_request" in p:
            r["merged"] = pr.get("merged")
            r["language"] = ((pr.get("base") or {}).get("repo") or {}).get("language")
        else:  # ya reducido
            r["merged"] = p.get("merged")
            r["language"] = p.get("language")
    elif tipo in ("IssuesEvent", "IssueCommentEvent"):
        issue = p.get("issue") or {}
        r["number"] = issue.get("number", p.get("number"))
    elif tipo == "ForkEvent":
        forkee = p.get("forkee")
        r["forkee"] = forkee.get("full_name") if isinstance(forkee, dict) else forkee
    elif tipo == "ReleaseEvent":
        release = p.get("release") or {}
        r["tag"] = release.get("tag_name", p.get("tag"))
    return r


def reducir_evento(evento: dict) -> dict:
    """Sobre del evento + payload reducido, con las mismas claves que GitHub."""
    tipo = evento.get("type")
    reducido = {
        "id": str(evento["id"]),
        "type": tipo,
        "created_at": evento.get("created_at"),
        "actor": _persona(evento.get("actor")),
        "repo": {"id": (evento.get("repo") or {}).get("id"), "name": (evento.get("repo") or {}).get("name")},
        "org": _persona(evento.get("org")),
        "public": evento.get("public", True),
        "payload": reducir_payload(tipo, evento.get("payload")),
    }
    return reducido
