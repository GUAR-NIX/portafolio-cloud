"""Núcleo del productor con un reloj simulado: ritmo, orden e inyección reproducible de duplicados y tardíos."""

import gzip
import json
from datetime import UTC, datetime

import pytest

from streaming_gh.productor import Mensaje, a_epoch, archivos_en_rango, leer_eventos, reproducir


class RelojFalso:
    def __init__(self):
        self.t = 1000.0
        self.dormido = 0.0

    def __call__(self) -> float:
        return self.t

    def dormir(self, segundos: float) -> None:
        self.t += segundos
        self.dormido += segundos


def mensajes(eventos_reducidos) -> list[Mensaje]:
    ms = [Mensaje(a_epoch(e["created_at"]), int(e["id"]), json.dumps(e).encode(), b"") for e in eventos_reducidos]
    return sorted(ms)


def correr(ms, factor, **kw):
    reloj, enviados = RelojFalso(), []

    def enviar(valor, clave, cabeceras):
        enviados.append((reloj(), json.loads(valor), dict(cabeceras)))

    r = reproducir(ms, enviar, factor, reloj=reloj, dormir=reloj.dormir, **kw)
    return r, enviados, reloj


def test_respeta_el_ritmo_con_el_factor(eventos_reducidos):
    ms = mensajes(eventos_reducidos)
    duracion_evento = ms[-1].ts_evento - ms[0].ts_evento
    r, enviados, reloj = correr(ms, factor=60)
    assert r.enviados == r.originales == len(ms)
    assert reloj.dormido == pytest.approx(duracion_evento / 60, rel=0.01)
    # cada mensaje sale en su momento: tiempo real transcurrido = tiempo de evento transcurrido / factor
    t0_real, t0_ev = enviados[0][0], a_epoch(enviados[0][1]["created_at"])
    for t, e, _ in enviados:
        assert t - t0_real == pytest.approx((a_epoch(e["created_at"]) - t0_ev) / 60, abs=0.01)


def test_factor_cero_no_espera(eventos_reducidos):
    r, _, reloj = correr(mensajes(eventos_reducidos), factor=0)
    assert reloj.dormido == 0
    assert r.enviados == len(eventos_reducidos)


def test_inyeccion_reproducible_y_contabilizada(eventos_reducidos):
    ms = mensajes(eventos_reducidos)
    r1, env1, _ = correr(ms, factor=0, prob_duplicado=0.05, prob_tardio=0.03, semilla=7)
    r2, env2, _ = correr(ms, factor=0, prob_duplicado=0.05, prob_tardio=0.03, semilla=7)
    assert [e["id"] for _, e, _ in env1] == [e["id"] for _, e, _ in env2]
    assert r1.duplicados > 0 and r1.tardios_dentro_watermark > 0 and r1.tardios_fuera_watermark > 0
    assert r1.enviados == r1.originales + r1.duplicados
    assert r1.ids_unicos == len({e["id"] for e in eventos_reducidos})
    reenvios = [e for _, e, c in env1 if c.get("reenvio") == b"1"]
    assert len(reenvios) == r1.duplicados
    assert len({e["id"] for _, e, _ in env1}) == r1.ids_unicos  # nada se pierde


def test_los_tardios_salen_despues_de_su_hora(eventos_reducidos):
    ms = mensajes(eventos_reducidos)
    _, enviados, _ = correr(ms, factor=0, prob_tardio=0.05, semilla=3)
    normales = [(i, a_epoch(e["created_at"])) for i, (_, e, cab) in enumerate(enviados) if "retraso_s" not in cab]
    tardios = [(i, a_epoch(e["created_at"]), int(cab["retraso_s"]))
               for i, (_, e, cab) in enumerate(enviados) if "retraso_s" in cab]
    assert tardios
    for pos, ts, retraso in tardios:
        assert retraso >= 180
        # todo evento normal con created_at anterior a (hora + retraso) ya se había publicado
        assert all(i < pos for i, ts_normal in normales if ts_normal < ts + retraso - 1)
        # y el tardío no se publicó antes de su hora: algún evento normal posterior a él salió primero
        assert any(i < pos and ts_normal > ts for i, ts_normal in normales)


def test_lee_horas_fusionadas_y_filtra_rango(tmp_path, eventos_reducidos):
    # dos "horas" con eventos intercalados: la fusión debe devolverlos ordenados
    ordenados = sorted(eventos_reducidos, key=lambda e: (e["created_at"], int(e["id"])))
    for nombre, parte in [("2025-09-16-14.jsonl.gz", ordenados[::2]), ("2025-09-16-15.jsonl.gz", ordenados[1::2])]:
        with gzip.open(tmp_path / nombre, "wb") as f:
            for e in parte:
                f.write(json.dumps(e).encode() + b"\n")
    desde = datetime(2025, 9, 16, 14, 10, tzinfo=UTC)
    hasta = datetime(2025, 9, 16, 14, 40, tzinfo=UTC)
    rutas = archivos_en_rango(tmp_path, desde, hasta)
    assert [r.name for r in rutas] == ["2025-09-16-14.jsonl.gz"]
    rutas = archivos_en_rango(tmp_path, desde, datetime(2025, 9, 16, 16, tzinfo=UTC))
    assert len(rutas) == 2
    leidos = list(leer_eventos(rutas, desde.timestamp(), hasta.timestamp()))
    esperados = [e for e in ordenados if desde.timestamp() <= a_epoch(e["created_at"]) < hasta.timestamp()]
    assert [m.id for m in leidos] == [int(e["id"]) for e in esperados]
