from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from lakehouse.transforms import bronze_para_silver, gold_kpi_diario, silver_para_gold_entregas

COLUNAS = [
    "id_evento", "id_entrega", "status", "data_evento", "uf_destino",
    "cnpj_embarcador", "peso_kg", "valor_frete", "_ingerido_em",
]
CNPJ = "00.000.000/0001-00"


@pytest.fixture(scope="session")
def spark():
    sessao = SparkSession.builder.master("local[1]").appName("testes").getOrCreate()
    yield sessao
    sessao.stop()


def _bronze(spark, linhas):
    return spark.createDataFrame(linhas, COLUNAS)


def test_silver_normaliza_status_e_cnpj(spark):
    bronze = _bronze(spark, [
        ("E1", "ENT-1", " entregue ", "2026-01-10 10:00:00", "sc", CNPJ, "10", "5.5", "2026-01-11 00:00:00"),
    ])
    silver, quarentena = bronze_para_silver(bronze)
    linha = silver.collect()[0]

    assert quarentena.count() == 0
    assert linha.status == "ENTREGUE"
    assert linha.uf_destino == "SC"
    assert linha.cnpj_embarcador == "00000000000100"
    assert linha.data_evento == datetime(2026, 1, 10, 10, 0, 0)


def test_silver_mantem_ultima_versao_do_evento(spark):
    bronze = _bronze(spark, [
        ("E1", "ENT-1", "COLETADO", "2026-01-10 10:00:00", "SC", CNPJ, "10", "5", "2026-01-11 00:00:00"),
        ("E1", "ENT-1", "COLETADO", "2026-01-10 10:00:00", "SC", CNPJ, "12", "5", "2026-01-12 00:00:00"),
    ])
    silver, _ = bronze_para_silver(bronze)
    linhas = silver.collect()

    assert len(linhas) == 1
    assert linhas[0].peso_kg == 12.0


@pytest.mark.parametrize(
    "linha, motivo",
    [
        (("E1", "ENT-1", "PERDIDO", "2026-01-10 10:00:00", "SC", CNPJ, "1", "1", "2026-01-11 00:00:00"), "status desconhecido"),
        (("E2", "ENT-1", "ENTREGUE", "31/02/2026", "SC", CNPJ, "1", "1", "2026-01-11 00:00:00"), "data_evento inválida"),
        (("E3", "ENT-1", "ENTREGUE", "2026-01-10 10:00:00", "SC", "123", "1", "1", "2026-01-11 00:00:00"), "cnpj inválido"),
        (("E4", "ENT-1", "ENTREGUE", "2026-01-10 10:00:00", "SC", CNPJ, "-1", "1", "2026-01-11 00:00:00"), "peso negativo"),
    ],
)
def test_linhas_invalidas_vao_para_quarentena(spark, linha, motivo):
    silver, quarentena = bronze_para_silver(_bronze(spark, [linha]))

    assert silver.count() == 0
    assert quarentena.collect()[0].motivo_rejeicao == motivo


def test_gold_calcula_status_atual_lead_time_e_ocorrencias(spark):
    bronze = _bronze(spark, [
        ("E1", "ENT-1", "COLETADO", "2026-01-10 08:00:00", "SC", CNPJ, "10", "5", "2026-01-12 00:00:00"),
        ("E2", "ENT-1", "OCORRENCIA", "2026-01-10 20:00:00", "SC", CNPJ, "10", "5", "2026-01-12 00:00:00"),
        ("E3", "ENT-1", "ENTREGUE", "2026-01-11 08:00:00", "SC", CNPJ, "10", "5", "2026-01-12 00:00:00"),
        ("E4", "ENT-2", "COLETADO", "2026-01-10 08:00:00", "PR", CNPJ, "10", "5", "2026-01-12 00:00:00"),
    ])
    silver, _ = bronze_para_silver(bronze)
    gold = {r.id_entrega: r for r in silver_para_gold_entregas(silver).collect()}

    assert gold["ENT-1"].status_atual == "ENTREGUE"
    assert gold["ENT-1"].lead_time_horas == 24.0
    assert gold["ENT-1"].qtd_ocorrencias == 1
    assert gold["ENT-2"].status_atual == "COLETADO"
    assert gold["ENT-2"].entregue_em is None


def test_kpi_diario_considera_sla(spark):
    bronze = _bronze(spark, [
        ("E1", "ENT-1", "COLETADO", "2026-01-10 08:00:00", "SC", CNPJ, "10", "5", "2026-01-20 00:00:00"),
        ("E2", "ENT-1", "ENTREGUE", "2026-01-11 08:00:00", "SC", CNPJ, "10", "5", "2026-01-20 00:00:00"),
        ("E3", "ENT-2", "COLETADO", "2026-01-07 08:00:00", "SC", CNPJ, "10", "5", "2026-01-20 00:00:00"),
        ("E4", "ENT-2", "ENTREGUE", "2026-01-11 08:00:00", "SC", CNPJ, "10", "5", "2026-01-20 00:00:00"),
    ])
    silver, _ = bronze_para_silver(bronze)
    kpi = gold_kpi_diario(silver_para_gold_entregas(silver), sla_horas=72).collect()[0]

    assert kpi.entregas == 2
    assert kpi.pct_no_prazo == 50.0
