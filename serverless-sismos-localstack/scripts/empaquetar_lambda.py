"""Construye el ZIP reproducible de Lambda con sus dependencias."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


def main() -> None:
    raiz = Path(__file__).resolve().parents[1]
    destino = raiz / "terraform/build/lambda"
    if destino.exists():
        shutil.rmtree(destino)
    destino.mkdir(parents=True)
    shutil.copy2(raiz / "lambdas/ingesta/app.py", destino / "app.py")
    shutil.copytree(raiz / "src/serverless_sismos", destino / "serverless_sismos")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-cache-dir",
            "--only-binary=:all:",
            "--platform=manylinux2014_x86_64",
            "--implementation=cp",
            "--python-version=3.12",
            "--abi=cp312",
            "--no-compile",
            "--target",
            str(destino),
            "pydantic==2.12.0",
        ],
        check=True,
    )
    print(destino)


if __name__ == "__main__":
    main()

