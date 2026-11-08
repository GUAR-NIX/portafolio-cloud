"""Cliente mínimo de la API v2 de Nessie (solo biblioteca estándar): ramas, merge, historial y diferencias."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request


class ErrorNessie(RuntimeError):
    pass


class Nessie:
    def __init__(self, uri: str = "http://localhost:19120"):
        self.base = uri.rstrip("/") + "/api/v2"

    def _pedir(self, metodo: str, ruta: str, cuerpo: dict | None = None, **params) -> dict:
        url = self.base + ruta
        if params:
            url += "?" + urllib.parse.urlencode(params)
        datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
        req = urllib.request.Request(url, data=datos, method=metodo, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                texto = r.read().decode()
        except urllib.error.HTTPError as e:
            raise ErrorNessie(f"{metodo} {ruta}: {e.code} {e.read().decode()[:300]}") from e
        return json.loads(texto) if texto else {}

    # --- referencias ---------------------------------------------------------------------------------------
    def ref(self, nombre: str) -> dict:
        return self._pedir("GET", f"/trees/{nombre}")["reference"]

    def hash(self, nombre: str) -> str:
        return self.ref(nombre)["hash"]

    def ramas(self) -> dict[str, str]:
        return {r["name"]: r["hash"] for r in self._pedir("GET", "/trees")["references"]}

    def crear_rama(self, nombre: str, desde: str = "main") -> dict:
        origen = self.ref(desde)
        return self._pedir("POST", "/trees", origen, name=nombre, type="BRANCH")["reference"]

    def asegurar_rama(self, nombre: str, desde: str = "main") -> dict:
        try:
            return self.ref(nombre)
        except ErrorNessie:
            return self.crear_rama(nombre, desde)

    def reasignar(self, nombre: str, hacia: str = "main") -> dict:
        """Mueve la rama `nombre` al commit actual de `hacia` (descarta lo que tenía)."""
        destino = self.ref(hacia)
        return self._pedir("PUT", f"/trees/{nombre}@{self.hash(nombre)}", destino, type="BRANCH")["reference"]

    def crear_etiqueta(self, nombre: str, hash_: str, desde: str = "main") -> dict:
        origen = {**self.ref(desde), "hash": hash_}
        return self._pedir("POST", "/trees", origen, name=nombre, type="TAG")["reference"]

    def asignar_etiqueta(self, nombre: str, hash_: str, desde: str = "main") -> dict:
        """Mueve la etiqueta `nombre` al commit `hash_` (que debe estar en la historia de `desde`)."""
        destino = {**self.ref(desde), "hash": hash_}
        return self._pedir("PUT", f"/trees/{nombre}@{self.hash(nombre)}", destino, type="TAG")["reference"]

    def asegurar_etiqueta(self, nombre: str, desde: str = "main") -> dict:
        try:
            return self.ref(nombre)
        except ErrorNessie:
            return self.crear_etiqueta(nombre, self.hash(desde), desde)

    def contenido(self, ref: str, clave: str) -> dict | None:
        """Contenido de una tabla en una referencia (`rama`, `etiqueta` o `rama@hash`); None si no existe."""
        try:
            return self._pedir("GET", f"/trees/{ref}/contents/{clave}")["content"]
        except ErrorNessie:
            return None

    def borrar_rama(self, nombre: str) -> None:
        self._pedir("DELETE", f"/trees/{nombre}@{self.hash(nombre)}", type="BRANCH")

    def fusionar(self, desde: str, hacia: str = "main", mensaje: str | None = None) -> dict:
        cuerpo = {"fromRefName": desde, "fromHash": self.hash(desde)}
        if mensaje:
            cuerpo["message"] = mensaje
        return self._pedir("POST", f"/trees/{hacia}@{self.hash(hacia)}/history/merge", cuerpo)

    # --- consultas -----------------------------------------------------------------------------------------
    def historial(self, ref: str, maximo: int = 50) -> list[dict]:
        r = self._pedir("GET", f"/trees/{ref}/history", **{"max-records": maximo})
        return [e["commitMeta"] for e in r.get("logEntries", [])]

    def diferencias(self, desde: str, hacia: str) -> list[dict]:
        r = self._pedir("GET", f"/trees/{desde}/diff/{hacia}")
        return [{"clave": ".".join(d["key"]["elements"]),
                 "desde": (d.get("from") or {}).get("metadataLocation"),
                 "hacia": (d.get("to") or {}).get("metadataLocation")} for d in r.get("diffs", [])]
