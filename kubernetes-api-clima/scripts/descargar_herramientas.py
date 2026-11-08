"""Descarga kind y Helm a tools/ con requests para evitar problemas TLS de PowerShell."""

from __future__ import annotations

import shutil
import zipfile
from pathlib import Path

import requests

VERSION_KIND = "v0.30.0"
VERSION_HELM = "v3.19.0"


def descargar(url: str, destino: Path) -> None:
    if destino.exists():
        return
    respuesta = requests.get(url, timeout=180)
    respuesta.raise_for_status()
    temporal = destino.with_suffix(destino.suffix + ".parcial")
    temporal.write_bytes(respuesta.content)
    temporal.replace(destino)


def main() -> None:
    raiz = Path(__file__).resolve().parents[1]
    tools = raiz / "tools"
    tools.mkdir(exist_ok=True)
    descargar(
        f"https://kind.sigs.k8s.io/dl/{VERSION_KIND}/kind-windows-amd64",
        tools / "kind.exe",
    )
    if not (tools / "helm.exe").exists():
        archivo = tools / "helm.zip"
        descargar(
            f"https://get.helm.sh/helm-{VERSION_HELM}-windows-amd64.zip",
            archivo,
        )
        with zipfile.ZipFile(archivo) as comprimido:
            with comprimido.open("windows-amd64/helm.exe") as origen:
                with (tools / "helm.exe").open("wb") as destino:
                    shutil.copyfileobj(origen, destino)
    print(tools)


if __name__ == "__main__":
    main()

