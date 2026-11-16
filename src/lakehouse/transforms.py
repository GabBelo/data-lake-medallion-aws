"""Transformações puras (DataFrame -> DataFrame) de cada camada do data lake.

Ficam separadas dos jobs para poderem ser testadas localmente com pytest,
sem depender do AWS Glue.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Status aceitos na camada Silver; qualquer outro vai para quarentena.
STATUS_VALIDOS = ["COLETADO", "EM_TRANSITO", "EM_ENTREGA", "ENTREGUE", "OCORRENCIA"]


def bronze_para_silver(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Limpa, tipa e deduplica os eventos de entrega.

    Retorna (silver, quarentena):
    - silver: um registro por evento, tipado e com o último envio de cada evento;
    - quarentena: linhas descartadas, com o motivo, para auditoria.
    """
    tipado = (
        bronze.select(
            F.trim("id_evento").alias("id_evento"),
            F.trim("id_entrega").alias("id_entrega"),
            F.upper(F.trim("status")).alias("status"),
            F.to_timestamp("data_evento", "yyyy-MM-dd HH:mm:ss").alias("data_evento"),
            F.upper(F.trim("uf_destino")).alias("uf_destino"),
            F.regexp_replace("cnpj_embarcador", r"\D", "").alias("cnpj_embarcador"),
            F.col("peso_kg").cast("double").alias("peso_kg"),
            F.col("valor_frete").cast("decimal(12,2)").alias("valor_frete"),
            F.to_timestamp("_ingerido_em").alias("_ingerido_em"),
        )
        .withColumn(
            "motivo_rejeicao",
            F.when(F.col("id_evento").isNull() | (F.col("id_evento") == ""), "id_evento vazio")
            .when(F.col("data_evento").isNull(), "data_evento inválida")
            .when(~F.col("status").isin(STATUS_VALIDOS), "status desconhecido")
            .when(F.length("cnpj_embarcador") != 14, "cnpj inválido")
            .when(F.col("peso_kg") < 0, "peso negativo"),
        )
    )

    quarentena = tipado.filter(F.col("motivo_rejeicao").isNotNull())

    # O mesmo evento pode chegar mais de uma vez (reenvio do parceiro):
    # fica a versão ingerida por último.
    ultima_versao = Window.partitionBy("id_evento").orderBy(F.col("_ingerido_em").desc())
    silver = (
        tipado.filter(F.col("motivo_rejeicao").isNull())
        .withColumn("_rn", F.row_number().over(ultima_versao))
        .filter("_rn = 1")
        .drop("_rn", "motivo_rejeicao")
        .withColumn("dt_evento", F.to_date("data_evento"))
    )
    return silver, quarentena


def silver_para_gold_entregas(silver: DataFrame) -> DataFrame:
    """Uma linha por entrega, com o status atual e o lead time até a entrega."""
    por_entrega = Window.partitionBy("id_entrega")
    mais_recente = Window.partitionBy("id_entrega").orderBy(F.col("data_evento").desc())

    return (
        silver.withColumn("primeiro_evento", F.min("data_evento").over(por_entrega))
        .withColumn(
            "entregue_em",
            F.max(F.when(F.col("status") == "ENTREGUE", F.col("data_evento"))).over(por_entrega),
        )
        .withColumn("qtd_ocorrencias", F.sum((F.col("status") == "OCORRENCIA").cast("int")).over(por_entrega))
        .withColumn("_rn", F.row_number().over(mais_recente))
        .filter("_rn = 1")
        .select(
            "id_entrega",
            "cnpj_embarcador",
            "uf_destino",
            F.col("status").alias("status_atual"),
            "primeiro_evento",
            "entregue_em",
            F.round(
                (F.unix_timestamp("entregue_em") - F.unix_timestamp("primeiro_evento")) / 3600, 1
            ).alias("lead_time_horas"),
            "qtd_ocorrencias",
            "peso_kg",
            "valor_frete",
        )
    )


def gold_kpi_diario(gold_entregas: DataFrame, sla_horas: float = 72.0) -> DataFrame:
    """KPIs por dia de entrega e UF: volume, % no prazo e lead time médio."""
    entregues = gold_entregas.filter(F.col("entregue_em").isNotNull())
    return (
        entregues.groupBy(F.to_date("entregue_em").alias("dt_entrega"), "uf_destino")
        .agg(
            F.count("*").alias("entregas"),
            F.round(F.avg((F.col("lead_time_horas") <= sla_horas).cast("int")) * 100, 1).alias("pct_no_prazo"),
            F.round(F.avg("lead_time_horas"), 1).alias("lead_time_medio_h"),
            F.sum("valor_frete").alias("receita_frete"),
        )
        .orderBy("dt_entrega", "uf_destino")
    )
