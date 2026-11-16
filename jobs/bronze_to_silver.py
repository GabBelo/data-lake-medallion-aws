"""Job AWS Glue: Bronze (CSV bruto) -> Silver (Parquet particionado por data).

Parâmetros do job:
  --bucket   bucket do data lake (ex.: meu-datalake-dev)
  --dt       data de ingestão a processar (yyyy-MM-dd)

Rodando localmente (sem Glue):  python jobs/bronze_to_silver.py --local sample_data/entregas_raw.csv
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession

from lakehouse.transforms import bronze_para_silver


def executar(spark: SparkSession, origem: str, destino_silver: str, destino_quarentena: str) -> None:
    bronze = spark.read.option("header", True).csv(origem)
    silver, quarentena = bronze_para_silver(bronze)

    # Sobrescreve só as partições de data presentes no lote (reprocessamento idempotente).
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    silver.write.mode("overwrite").partitionBy("dt_evento").parquet(destino_silver)
    quarentena.write.mode("append").parquet(destino_quarentena)

    print(f"silver: {silver.count()} linhas | quarentena: {quarentena.count()} linhas")


def main() -> None:
    if "--local" in sys.argv:
        origem = sys.argv[sys.argv.index("--local") + 1]
        spark = SparkSession.builder.master("local[*]").appName("bronze_to_silver").getOrCreate()
        executar(spark, origem, "output/silver/entregas", "output/quarentena/entregas")
        return

    from awsglue.context import GlueContext  # disponível apenas no runtime do Glue
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["bucket", "dt"])
    spark = GlueContext(SparkContext.getOrCreate()).spark_session
    base = f"s3://{args['bucket']}"
    executar(
        spark,
        origem=f"{base}/bronze/entregas/dt_ingestao={args['dt']}/",
        destino_silver=f"{base}/silver/entregas/",
        destino_quarentena=f"{base}/quarentena/entregas/",
    )


if __name__ == "__main__":
    main()
