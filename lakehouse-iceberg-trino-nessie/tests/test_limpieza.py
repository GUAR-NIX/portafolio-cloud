from pathlib import Path

from lakehouse.limpieza import columnas_limpias, es_no_articulo, limpiar, normalizar_titulo

MUESTRA = Path(__file__).parent / "datos" / "muestra_20260719-19.txt"


def test_normalizar_titulo_decodifica_y_capitaliza():
    assert normalizar_titulo("Caf%C3%A9") == "Café"
    assert normalizar_titulo("lionel Messi") == "Lionel_Messi"
    assert normalizar_titulo("_Argentina_") == "Argentina"
    assert normalizar_titulo("%E2%82%AC") == "€"


def test_no_articulos_en_ambos_idiomas():
    for t in ["Special:Search", "Especial:Buscar", "Archivo:Logo.svg", "File:X.jpg", "Usuario:Erick",
              "Wikipedia:Portada", "Main_Page", "Categoría:Fútbol", "Plantilla:Ficha", "Talk:Foo"]:
        assert es_no_articulo(t), t
    for t in ["Lionel_Messi", "Copa_Mundial_de_Fútbol_de_2026", "Anexo:Finales_de_la_Copa_Mundial",
              "Star_Wars:_Episode_IV", "Special", "Portada_(arquitectura)", "Re:Zero"]:
        assert not es_no_articulo(t), t


def test_limpiar_cuenta_y_deduplica():
    lineas = [
        "en.m Caf%C3%A9 3 0",
        "en.m Café 2 0",        # misma clave tras normalizar → se suma
        "en Café 1 0",          # otra plataforma → otra fila
        "es Especial:Buscar 50 0",
        "es Main_Page 9 0",
        "es.m - 4 0",           # título desconocido
        "en.m Foo x 0",         # vistas no numéricas
        "en.m Foo 0 0",         # cero vistas
        "de Berlin 5 0",        # dominio fuera de alcance
        "en.m roto",            # campos de menos
    ]
    r = limpiar(lineas)
    assert r.lineas == 10
    assert r.filas == {("en.m", "Café"): 5, ("en", "Café"): 1}
    assert r.fusionadas == 1
    assert r.no_articulo == 2 and r.vistas_no_articulo == 59
    assert dict(r.invalidas) == {"titulo": 1, "vistas": 2, "dominio": 1, "campos": 1}
    cols = columnas_limpias(r)
    assert cols["idioma"] == ["en", "en"] and sorted(cols["plataforma"]) == ["escritorio", "movil"]


def test_conciliacion_en_muestra_real():
    lineas = MUESTRA.read_text(encoding="utf-8").splitlines()
    r = limpiar(lineas)
    vistas_validas = sum(int(l.split(" ")[2]) for l in lineas if l.split(" ")[2].isdigit()
                         and normalizar_titulo(l.split(" ")[1]) not in ("", "-"))
    assert r.vistas_limpias + r.vistas_no_articulo == vistas_validas
    assert r.filas_limpias + r.no_articulo + r.fusionadas + sum(r.invalidas.values()) == r.lineas
    assert r.filas[("en.m", "Lamine_Yamal")] > 10_000
