"""Limpieza de las líneas de pageviews (Python puro, sin dependencias, para probarla sin contenedores).

Formato de origen (una línea por página y hora):  `dominio título vistas bytes_respuesta`
  - dominio: `en`, `en.m`, `es`, `es.m` (Wikipedia de escritorio y móvil)
  - título: tal como llegó en la URL; a veces con %XX y otras ya decodificado
  - bytes_respuesta: siempre 0 en los dumps actuales (se conserva en crudo y se descarta en limpio)

Reglas de la capa limpia:
  1. Línea válida: 4 campos, vistas entero > 0 y título no vacío ni `-`.
  2. Título normalizado como lo hace MediaWiki: decodificar %XX, espacios → `_` y primera letra en mayúscula.
  3. Fuera las páginas que no son artículos: espacios de nombres (Special:, Especial:, Archivo:, Usuario:…) y
     las portadas (Main_Page, Wikipedia:Portada).
  4. Deduplicado: un mismo artículo puede llegar con dos grafías (`Caf%C3%A9` y `Café`); tras normalizar, las
     filas con la misma clave (hora, dominio, título) se suman en una sola.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from urllib.parse import unquote

from lakehouse.configuracion import IDIOMAS, PLATAFORMAS

# Espacios de nombres que no son artículos, en inglés y en español (comparación sin mayúsculas).
# «Anexo» (listas en eswiki) sí es contenido y no se excluye.
ESPACIOS_NO_ARTICULO = {
    "special", "especial", "file", "archivo", "image", "imagen", "media", "category", "categoría", "categoria",
    "user", "usuario", "usuaria", "user_talk", "usuario_discusión", "usuaria_discusión", "talk", "discusión",
    "wikipedia", "wikipedia_talk", "wikipedia_discusión", "help", "ayuda", "help_talk", "ayuda_discusión",
    "template", "plantilla", "template_talk", "plantilla_discusión", "portal", "portal_talk", "portal_discusión",
    "draft", "borrador", "mediawiki", "module", "módulo", "modulo", "timedtext", "topic", "file_talk",
    "archivo_discusión", "category_talk", "categoría_discusión", "wikiproyecto", "wikiproject", "book", "libro",
    "gadget", "gadget_definition", "anexo_discusión", "draft_talk", "mos",
}
PORTADAS = {"Main_Page", "Wikipedia:Portada", "Portada"}


def normalizar_titulo(crudo: str) -> str:
    """Decodifica %XX, cambia espacios por `_` y pone la primera letra en mayúscula (regla de MediaWiki)."""
    t = unquote(crudo, errors="replace").replace(" ", "_").strip("_")
    return t[:1].upper() + t[1:] if t else t


def es_no_articulo(titulo: str) -> bool:
    """True para portadas y páginas de espacios de nombres que no son artículos."""
    if titulo in PORTADAS:
        return True
    prefijo, dos_puntos, resto = titulo.partition(":")
    return bool(dos_puntos and resto and prefijo.lower() in ESPACIOS_NO_ARTICULO)


@dataclass
class ResultadoLimpieza:
    """Filas limpias de una hora más los conteos que permiten conciliar crudo → limpio."""

    filas: dict[tuple[str, str], int] = field(default_factory=dict)  # (dominio, título) → vistas
    lineas: int = 0
    invalidas: Counter = field(default_factory=Counter)
    no_articulo: int = 0
    vistas_no_articulo: int = 0
    fusionadas: int = 0

    @property
    def filas_limpias(self) -> int:
        return len(self.filas)

    @property
    def vistas_limpias(self) -> int:
        return sum(self.filas.values())

    def resumen(self) -> dict:
        return {"lineas": self.lineas, "invalidas": dict(self.invalidas), "no_articulo": self.no_articulo,
                "vistas_no_articulo": self.vistas_no_articulo, "fusionadas": self.fusionadas,
                "filas_limpias": self.filas_limpias, "vistas_limpias": self.vistas_limpias}


def separar_linea(linea: str) -> tuple[str, str, str, str] | None:
    partes = linea.rstrip("\r\n").split(" ")
    return tuple(partes) if len(partes) == 4 else None  # type: ignore[return-value]


def limpiar(lineas) -> ResultadoLimpieza:
    """Aplica las reglas 1-4 a las líneas (str) de un archivo horario."""
    r = ResultadoLimpieza()
    for linea in lineas:
        r.lineas += 1
        partes = separar_linea(linea)
        if partes is None:
            r.invalidas["campos"] += 1
            continue
        dominio, titulo_crudo, vistas_txt, _ = partes
        if dominio not in IDIOMAS:
            r.invalidas["dominio"] += 1
            continue
        if not vistas_txt.isdigit() or int(vistas_txt) == 0:
            r.invalidas["vistas"] += 1
            continue
        titulo = normalizar_titulo(titulo_crudo)
        if not titulo or titulo == "-":
            r.invalidas["titulo"] += 1
            continue
        vistas = int(vistas_txt)
        if es_no_articulo(titulo):
            r.no_articulo += 1
            r.vistas_no_articulo += vistas
            continue
        clave = (dominio, titulo)
        if clave in r.filas:
            r.fusionadas += 1
            r.filas[clave] += vistas
        else:
            r.filas[clave] = vistas
    return r


def columnas_limpias(r: ResultadoLimpieza) -> dict[str, list]:
    """Columnas listas para Arrow/Iceberg (sin la hora, que la pone el cargador)."""
    dominios, titulos, vistas = [], [], []
    for (d, t), v in r.filas.items():
        dominios.append(d)
        titulos.append(t)
        vistas.append(v)
    return {"dominio": dominios, "idioma": [IDIOMAS[d] for d in dominios],
            "plataforma": [PLATAFORMAS[d] for d in dominios], "titulo": titulos, "vistas": vistas}
