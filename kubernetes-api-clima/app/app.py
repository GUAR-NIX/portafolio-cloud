"""Interfaz Streamlit que consume la API por HTTP."""

from __future__ import annotations

import os

import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="Clima de Ecuador", page_icon="🌦️", layout="wide")
st.title("Clima histórico de ciudades del Ecuador")
st.caption("Datos diarios de Open-Meteo servidos por FastAPI sobre Kubernetes")

try:
    ciudades = requests.get(f"{API_URL}/ciudades", timeout=5).json()["ciudades"]
    ciudad = st.selectbox("Ciudad", ciudades)
    respuesta = requests.get(f"{API_URL}/indicadores/{ciudad}", timeout=5)
    respuesta.raise_for_status()
    datos = respuesta.json()
    columnas = st.columns(4)
    columnas[0].metric("Temperatura media", f"{datos['temperatura_media_c']} °C")
    columnas[1].metric("Máxima", f"{datos['temperatura_maxima_c']} °C")
    columnas[2].metric("Precipitación", f"{datos['precipitacion_total_mm']} mm")
    columnas[3].metric("Días lluviosos", datos["dias_lluviosos"])
    st.json(datos)
except (requests.RequestException, KeyError, ValueError) as error:
    st.error(f"La API no está disponible: {error}")

