"""Mezcla de consultas usada en la línea base local y en el Job de carga dentro de kind."""

import os
import random
from datetime import date, timedelta

from locust import HttpUser, between, task

CIUDADES = [
    "Ambato", "Cuenca", "Esmeraldas", "Guayaquil", "Ibarra", "Loja",
    "Manta", "Nueva Loja", "Puerto Ayora", "Puyo", "Quito", "Santo Domingo",
]
INICIO = date(2024, 1, 1)


class UsuarioClima(HttpUser):
    wait_time = between(0.01, 0.05)

    def on_start(self):
        # Para atravesar ingress-nginx dentro del clúster hace falta la cabecera Host de la regla.
        if host := os.getenv("HOST_INGRESS"):
            self.client.headers["Host"] = host

    @task(3)
    def indicadores(self):
        self.client.get(f"/indicadores/{random.choice(CIUDADES)}", name="/indicadores/{ciudad}")

    @task(3)
    def clima(self):
        fecha = INICIO + timedelta(days=random.randrange(366))
        self.client.get(
            "/clima",
            params={"ciudad": random.choice(CIUDADES), "fecha": fecha.isoformat()},
            name="/clima",
        )

    @task(1)
    def ciudades(self):
        self.client.get("/ciudades", name="/ciudades")
