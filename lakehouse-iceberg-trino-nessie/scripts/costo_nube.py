"""Estimación de costo mensual en AWS (S3 + Glue Data Catalog + Athena + Fargate) con los tamaños medidos aquí.

No llama a ninguna API: aplica precios de lista de referencia (us-east-1, en USD) a supuestos explícitos.
Los precios cambian; verificar en la calculadora oficial antes de decidir. Uso: python scripts/costo_nube.py
"""

import json
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]

PRECIOS = {
    "s3_gb_mes": 0.023,             # S3 Standard, primeros 50 TB
    "s3_put_por_1000": 0.005,
    "s3_get_por_1000": 0.0004,
    "athena_usd_por_tb": 5.0,        # se cobra mínimo 10 MB por consulta
    "athena_minimo_mb": 10,
    "glue_objetos_gratis": 1_000_000,  # Data Catalog: 1 M de objetos y 1 M de solicitudes al mes sin costo
    "fargate_vcpu_hora": 0.04048,
    "fargate_gb_hora": 0.004445,
}

# Medido en este proyecto (4 días, reports/): crudo 321 MB + limpio 321 MB + agregados ~47 MB; dumps filtrados 262 MB
MB_POR_DIA_ICEBERG = (321.2 + 321.5 + 46.7) / 4
MB_POR_DIA_LANDING = 262.3 / 4
MB_HORA_PODADA = 0.28        # consulta de una hora e idioma (rendimiento.json)
MB_DIA_COMPLETO_LIMPIO = 321.5 / 4

ESCENARIOS = {
    "prueba (1 mes de datos)": {"dias_guardados": 30, "consultas_tablero_mes": 3_000, "consultas_analisis_mes": 50},
    "producción (1 año de datos)": {"dias_guardados": 365, "consultas_tablero_mes": 30_000,
                                    "consultas_analisis_mes": 300},
}


def costo(dias_guardados: int, consultas_tablero_mes: int, consultas_analisis_mes: int) -> dict:
    p = PRECIOS
    gb = dias_guardados * (MB_POR_DIA_ICEBERG + MB_POR_DIA_LANDING) / 1024
    s3_alm = gb * p["s3_gb_mes"]
    # 24 cargas al día: ~6 PUT por carga (datos + manifiestos + metadatos) y 1 compactación diaria (~200 PUT)
    puts = 30 * (24 * 6 + 200)
    s3_req = puts / 1000 * p["s3_put_por_1000"]
    # Tablero: consultas de una hora (mínimo facturable 10 MB). Análisis: 30 días del limpio sin poda horaria.
    mb_tablero = consultas_tablero_mes * max(MB_HORA_PODADA, p["athena_minimo_mb"])
    mb_analisis = consultas_analisis_mes * min(dias_guardados, 30) * MB_DIA_COMPLETO_LIMPIO
    gets = (consultas_tablero_mes * 5 + consultas_analisis_mes * 300)
    s3_req += gets / 1000 * p["s3_get_por_1000"]
    athena = (mb_tablero + mb_analisis) / 1024 / 1024 * p["athena_usd_por_tb"]
    # Cargador en Fargate: 0,5 vCPU y 1 GB, ~40 s por hora cargada
    horas_tarea = 30 * 24 * 40 / 3600
    fargate = horas_tarea * (0.5 * p["fargate_vcpu_hora"] + 1 * p["fargate_gb_hora"])
    total = s3_alm + s3_req + athena + fargate
    return {"gb_almacenados": round(gb, 1), "s3_almacenamiento": round(s3_alm, 2),
            "s3_solicitudes": round(s3_req, 2), "athena": round(athena, 2),
            "glue_catalog": 0.0, "fargate_cargador": round(fargate, 2), "total_usd_mes": round(total, 2),
            "tb_escaneados_mes": round((mb_tablero + mb_analisis) / 1024 / 1024, 4)}


def main() -> None:
    res = {"precios_referencia_usd": PRECIOS,
           "supuestos": {"mb_por_dia_iceberg": round(MB_POR_DIA_ICEBERG, 1),
                         "mb_por_dia_landing": round(MB_POR_DIA_LANDING, 1),
                         "mb_consulta_hora_podada": MB_HORA_PODADA,
                         "mb_dia_limpio": round(MB_DIA_COMPLETO_LIMPIO, 1),
                         "nota": "Glue: ~10 tablas y < 1 M de solicitudes al mes, dentro del nivel gratuito."},
           "escenarios": {k: costo(**v) | {"parametros": v} for k, v in ESCENARIOS.items()}}
    (RAIZ / "reports" / "costo_nube.json").write_text(json.dumps(res, indent=2, ensure_ascii=False),
                                                      encoding="utf-8")
    for k, v in res["escenarios"].items():
        print(k, v)


if __name__ == "__main__":
    main()
