"""Conversión CSV → Parquet, parseo de ancho fijo y controles de calidad (DuckDB local, sin MinIO)."""

from clima_mds import almacen, calidad

LINEA_ESTACION = (
    "EC000084370  -0.1500  -78.4800 2812.0    QUITO/MARISCAL SUCRE           GSN     84370"
)


def nombres_fallidos(controles):
    return {c.nombre for c in controles if not c.aprobado}


def test_parsear_estaciones_y_paises():
    df = almacen.parsear_estaciones(LINEA_ESTACION + "\n")
    fila = df.iloc[0]
    assert fila.id == "EC000084370" and fila.nombre == "QUITO/MARISCAL SUCRE"
    assert (fila.latitud, fila.longitud, fila.elevacion_m) == (-0.15, -78.48, 2812.0)
    assert fila.gsn == "GSN" and fila.wmo_id == "84370" and fila.estado is None
    paises = almacen.parsear_paises("EC Ecuador\nSP Spain\n")
    assert paises.to_dict("records") == [{"codigo": "EC", "nombre": "Ecuador"}, {"codigo": "SP", "nombre": "Spain"}]


def test_csv_a_parquet_y_controles_aprobados(tmp_path, csv_valido):
    con = almacen.conectar(s3=False)
    destino = (tmp_path / "datos.parquet").as_posix()
    almacen.csv_a_parquet(con, csv_valido.as_posix(), destino)
    filas, nulos_q = con.execute(f"SELECT count(*), count(q_flag) FROM read_parquet('{destino}')").fetchone()
    assert filas == 28 * 3 * 4 + 1 and nulos_q == 1  # las banderas vacías llegan como NULL
    controles = calidad.controles_crudo(con, f"read_parquet('{destino}')", 2024, filas_minimas=100)
    assert nombres_fallidos(controles) == set()


def test_datos_malos_hacen_fallar_los_controles(tmp_path, csv_valido):
    con = almacen.conectar(s3=False)
    destino = (tmp_path / "malos.parquet").as_posix()
    almacen.csv_a_parquet(con, csv_valido.as_posix(), destino, simular_datos_malos=True)
    # con 3 estaciones puede que ningún hash caiga en el 2 %: se fuerza una TMAX absurda adicional
    con.execute(f"COPY (SELECT * REPLACE (CASE WHEN elemento = 'TMAX' AND fecha = 20240210 THEN 9999 "
                f"ELSE valor END AS valor) FROM read_parquet('{destino}')) TO '{tmp_path / 'm2.parquet'}'")
    controles = calidad.controles_crudo(con, f"read_parquet('{tmp_path / 'm2.parquet'}')", 2024, filas_minimas=100)
    assert nombres_fallidos(controles) == {"temperatura_en_rango_fisico"}
    # partición equivocada y volumen insuficiente
    controles = calidad.controles_crudo(con, f"read_parquet('{destino}')", 2023, filas_minimas=10_000)
    assert {"fechas_dentro_del_anio", "filas_minimas"} <= nombres_fallidos(controles)


def test_falta_un_elemento_basico(tmp_path, csv_valido):
    con = almacen.conectar(s3=False)
    destino = (tmp_path / "sin_prcp.parquet").as_posix()
    con.execute(f"COPY (SELECT * FROM {almacen.sql_leer_csv(csv_valido.as_posix())} WHERE elemento <> 'PRCP') "
                f"TO '{destino}'")
    controles = calidad.controles_crudo(con, f"read_parquet('{destino}')", 2024, filas_minimas=100)
    assert nombres_fallidos(controles) == {"elementos_basicos_presentes"}
