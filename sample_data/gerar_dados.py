"""Gera um CSV fictício de eventos de entrega para testar o pipeline localmente.

Todos os dados são inventados: os CNPJs seguem o padrão 0000000000XXXX e não
correspondem a empresas reais.

Uso:  python sample_data/gerar_dados.py  (gera sample_data/entregas_raw.csv)
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

UFS = ["SC", "PR", "RS", "SP", "MG", "RJ"]
EMBARCADORES = [f"0000000000{n:04d}" for n in range(1, 9)]
FLUXO = ["COLETADO", "EM_TRANSITO", "EM_ENTREGA", "ENTREGUE"]
INICIO = datetime(2026, 1, 1, 6, 0, 0)


def gerar(qtd_entregas: int = 500) -> list[dict]:
    linhas: list[dict] = []
    seq = 0
    for i in range(1, qtd_entregas + 1):
        id_entrega = f"ENT-{i:06d}"
        momento = INICIO + timedelta(hours=random.randint(0, 24 * 60))
        uf = random.choice(UFS)
        embarcador = random.choice(EMBARCADORES)
        peso = round(random.uniform(50, 28000), 1)
        frete = round(peso * random.uniform(0.08, 0.35), 2)
        # Parte das entregas ainda não terminou o fluxo.
        etapas = FLUXO if random.random() < 0.85 else FLUXO[: random.randint(1, 3)]
        for status in etapas:
            if status != "COLETADO" and random.random() < 0.08:
                seq += 1
                linhas.append(_linha(seq, id_entrega, "OCORRENCIA", momento, uf, embarcador, peso, frete))
                momento += timedelta(hours=random.randint(2, 24))
            seq += 1
            linhas.append(_linha(seq, id_entrega, status, momento, uf, embarcador, peso, frete))
            momento += timedelta(hours=random.randint(4, 40))

    # Reenvios duplicados e linhas inválidas, para exercitar dedupe e quarentena.
    for linha in random.sample(linhas, 20):
        dup = dict(linha)
        dup["_ingerido_em"] = "2026-03-02 03:00:00"
        linhas.append(dup)
    linhas += [
        _linha(900001, "ENT-999991", "PERDIDO", INICIO, "SC", EMBARCADORES[0], 10, 5),
        _linha(900002, "ENT-999992", "ENTREGUE", INICIO, "SC", "123", 10, 5),
        {**_linha(900003, "ENT-999993", "ENTREGUE", INICIO, "SC", EMBARCADORES[0], 10, 5), "data_evento": "31/02/2026"},
        _linha(900004, "ENT-999994", "COLETADO", INICIO, "SC", EMBARCADORES[0], -3, 5),
    ]
    return linhas


def _linha(seq, id_entrega, status, momento, uf, embarcador, peso, frete) -> dict:
    return {
        "id_evento": f"EVT-{seq:08d}",
        "id_entrega": id_entrega,
        "status": status.lower() if random.random() < 0.1 else status,  # sujeira proposital
        "data_evento": momento.strftime("%Y-%m-%d %H:%M:%S"),
        "uf_destino": uf,
        "cnpj_embarcador": f"{embarcador[:2]}.{embarcador[2:5]}.{embarcador[5:8]}/{embarcador[8:12]}-{embarcador[12:]}"
        if len(embarcador) == 14
        else embarcador,
        "peso_kg": peso,
        "valor_frete": frete,
        "_ingerido_em": "2026-03-01 03:00:00",
    }


if __name__ == "__main__":
    destino = Path(__file__).with_name("entregas_raw.csv")
    dados = gerar()
    with destino.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(dados[0].keys()))
        writer.writeheader()
        writer.writerows(dados)
    print(f"{len(dados)} linhas em {destino}")
