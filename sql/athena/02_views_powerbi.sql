-- Views consumidas pelo Power BI (conector ODBC do Athena).
-- A lógica pesada fica no Glue; aqui só recortes leves e nomes amigáveis.

-- Entregas em aberto há mais de 72 h: base do alerta de atraso.
CREATE OR REPLACE VIEW gold.vw_entregas_atrasadas AS
SELECT
  id_entrega,
  uf_destino,
  status_atual,
  primeiro_evento,
  date_diff('hour', primeiro_evento, current_timestamp) AS horas_em_aberto,
  qtd_ocorrencias
FROM gold.entregas
WHERE entregue_em IS NULL
  AND date_diff('hour', primeiro_evento, current_timestamp) > 72;

-- Visão mensal por UF, com variação contra o mês anterior.
CREATE OR REPLACE VIEW gold.vw_kpi_mensal_uf AS
WITH mensal AS (
  SELECT
    date_trunc('month', dt_entrega)        AS mes,
    uf_destino,
    sum(entregas)                          AS entregas,
    round(avg(pct_no_prazo), 1)            AS pct_no_prazo,
    round(avg(lead_time_medio_h), 1)       AS lead_time_medio_h
  FROM gold.kpi_diario
  GROUP BY 1, 2
)
SELECT
  mes,
  uf_destino,
  entregas,
  pct_no_prazo,
  lead_time_medio_h,
  pct_no_prazo - lag(pct_no_prazo) OVER (PARTITION BY uf_destino ORDER BY mes) AS variacao_pct_no_prazo
FROM mensal;
