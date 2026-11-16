-- Tabelas externas do Athena apontando para a camada Gold no S3.
-- Troque <bucket> pelo nome do seu bucket.

CREATE EXTERNAL TABLE IF NOT EXISTS gold.entregas (
  id_entrega        string,
  cnpj_embarcador   string,
  uf_destino        string,
  status_atual      string,
  primeiro_evento   timestamp,
  entregue_em       timestamp,
  lead_time_horas   double,
  qtd_ocorrencias   bigint,
  peso_kg           double,
  valor_frete       decimal(12,2)
)
STORED AS PARQUET
LOCATION 's3://<bucket>/gold/entregas/';

CREATE EXTERNAL TABLE IF NOT EXISTS gold.kpi_diario (
  dt_entrega         date,
  uf_destino         string,
  entregas           bigint,
  pct_no_prazo       double,
  lead_time_medio_h  double,
  receita_frete      decimal(22,2)
)
STORED AS PARQUET
LOCATION 's3://<bucket>/gold/kpi_diario/';
