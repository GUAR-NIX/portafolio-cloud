"""Aplicación FastAPI instrumentada para Prometheus."""

from __future__ import annotations

import os
from datetime import date

from fastapi import FastAPI, HTTPException, Query, Request, Response, status
from prometheus_fastapi_instrumentator import Instrumentator

from .configuracion import ruta_datos
from .repositorio import RepositorioClima

app = FastAPI(
    title="API de clima del Ecuador",
    version="1.0.0",
    description="Indicadores diarios reproducibles a partir de Open-Meteo.",
)
repositorio = RepositorioClima(ruta_datos())
estado = {"drenando": False}


class CerrarConexionesAlDrenar:
    """Middleware ASGI: tras /drenar, cada respuesta lleva `Connection: close`.

    Los clientes con keep-alive siguen enviando al pod viejo aunque kube-proxy ya lo haya sacado
    del Service; al recibir `close` abren una conexión nueva, que cae en un pod vivo. Así, cuando
    llega el SIGTERM ya no quedan conexiones que Uvicorn tenga que cortar (antes: ConnectionReset).
    """

    def __init__(self, siguiente):
        self.siguiente = siguiente

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not estado["drenando"]:
            return await self.siguiente(scope, receive, send)

        async def enviar(mensaje):
            if mensaje["type"] == "http.response.start":
                mensaje["headers"] = [*mensaje.get("headers", []), (b"connection", b"close")]
            await send(mensaje)

        await self.siguiente(scope, receive, enviar)


app.add_middleware(CerrarConexionesAlDrenar)
# Las cubetas por defecto de http_request_duration_seconds son 0,1 / 0,5 / 1 s: con respuestas de
# pocos milisegundos el p95 calculado en Prometheus quedaría siempre en ~95 ms. Se usan cubetas finas.
CUBETAS_LATENCIA = (0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1, 2.5)
Instrumentator(excluded_handlers=["/metricas"]).instrument(
    app, latency_lowr_buckets=CUBETAS_LATENCIA
).expose(app, endpoint="/metricas", include_in_schema=False)


@app.get("/salud", tags=["operación"])
def salud() -> dict[str, str]:
    return {"estado": "vivo", "version": os.getenv("VERSION_APP", "desarrollo")}


@app.get("/listo", tags=["operación"])
def listo(response: Response) -> dict[str, str]:
    if estado["drenando"]:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"estado": "drenando"}
    if not repositorio.listo():
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"estado": "sin_datos"}
    return {"estado": "listo"}


@app.post("/drenar", include_in_schema=False)
def drenar(request: Request) -> dict[str, str]:
    """Lo invoca el preStop del propio pod; desde fuera del contenedor se rechaza."""
    if request.client is None or request.client.host not in {"127.0.0.1", "::1"}:
        raise HTTPException(status_code=403, detail="Solo desde el propio pod")
    estado["drenando"] = True
    return {"estado": "drenando"}


@app.get("/ciudades", tags=["clima"])
def ciudades() -> dict[str, list[str]]:
    return {"ciudades": repositorio.ciudades()}


@app.get("/clima", tags=["clima"])
def clima(
    ciudad: str = Query(min_length=2),
    fecha: date = Query(),
) -> dict:
    resultado = repositorio.por_ciudad_fecha(ciudad, fecha.isoformat())
    if resultado is None:
        raise HTTPException(status_code=404, detail="No hay dato para esa ciudad y fecha")
    resultado["fecha"] = resultado["fecha"].isoformat()
    return resultado


@app.get("/indicadores/{ciudad}", tags=["clima"])
def indicadores(ciudad: str) -> dict:
    resultado = repositorio.indicadores(ciudad)
    if resultado is None:
        raise HTTPException(status_code=404, detail="Ciudad no encontrada")
    return resultado

