"""Job AWS Glue: Silver -> Gold (tabelas prontas para o Athena / Power BI).

Gera duas tabelas:
  gold/entregas     uma linha por entrega, com status atual e lead time
  gold/kpi_diario   KPIs por dia e UF (volume, % no prazo, lead time médio)

Rodando localmente:  python jobs/silver_to_gold.py --local output/silver/entregas
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession

from lakehouse.transforms import gold_kpi_diario, silver_para_gold_entregas


def executar(spark: SparkSession, origem_silver: str, destino_gold: str) -> None:
    silver = spark.read.parquet(origem_silver)
    entregas = silver_para_gold_entregas(silver)
    kpis = gold_kpi_diario(entregas)

    entregas.write.mode("overwrite").parquet(f"{destino_gold}/entregas")
    kpis.write.mode("overwrite").parquet(f"{destino_gold}/kpi_diario")
    kpis.show(truncate=False)


def main() -> None:
    if "--local" in sys.argv:
        origem = sys.argv[sys.argv.index("--local") + 1]
        spark = SparkSession.builder.master("local[*]").appName("silver_to_gold").getOrCreate()
        executar(spark, origem, "output/gold")
        return

    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["bucket"])
    spark = GlueContext(SparkContext.getOrCreate()).spark_session
    base = f"s3://{args['bucket']}"
    executar(spark, origem_silver=f"{base}/silver/entregas/", destino_gold=f"{base}/gold")


if __name__ == "__main__":
    main()
