"""Leitura e validação compartilhada da janela temporal dos ETLs."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Sequence


@dataclass(frozen=True)
class PeriodoProcessamento:
    inicio: date
    fim: date

    @property
    def fim_exclusivo(self) -> date:
        return self.fim + timedelta(days=1)

    @property
    def competencia_inicio(self) -> int:
        return self.inicio.year * 100 + self.inicio.month

    @property
    def competencia_fim(self) -> int:
        return self.fim.year * 100 + self.fim.month


def _converter_data(valor: str, nome: str) -> date:
    try:
        return datetime.strptime(valor, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{nome} inválido: {valor!r}. Use YYYY-MM-DD.") from exc


def obter_periodo(
    argv: Sequence[str] | None = None,
    *,
    inicio_padrao: date = date(2021, 1, 1),
    ano_fim_padrao: int = 2025,
) -> PeriodoProcessamento:
    """
    Lê ``--inicio``/``--fim`` sem interferir nos demais argumentos do ETL.

    ``--ano`` é mantido por compatibilidade: quando não há intervalo explícito,
    representa 01/01/2021 a 31/12 do ano informado.
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--inicio")
    parser.add_argument("--fim")
    parser.add_argument("--ano", type=int)
    args, _ = parser.parse_known_args(argv)

    if bool(args.inicio) ^ bool(args.fim):
        raise ValueError("Informe --inicio e --fim juntos.")

    if args.inicio and args.fim:
        inicio = _converter_data(args.inicio, "Data inicial")
        fim = _converter_data(args.fim, "Data final")
    elif args.ano is not None:
        inicio = inicio_padrao
        fim = date(args.ano, 12, 31)
    else:
        inicio_texto = os.getenv("DATA_INICIO")
        fim_texto = os.getenv("DATA_FIM")
        if bool(inicio_texto) ^ bool(fim_texto):
            raise ValueError("Defina DATA_INICIO e DATA_FIM juntas.")
        if inicio_texto and fim_texto:
            inicio = _converter_data(inicio_texto, "DATA_INICIO")
            fim = _converter_data(fim_texto, "DATA_FIM")
        else:
            ano_fim = int(os.getenv("ANO", str(ano_fim_padrao)))
            inicio = inicio_padrao
            fim = date(ano_fim, 12, 31)

    if fim.year < inicio_padrao.year and not (args.inicio and args.fim):
        raise ValueError(
            f"Ano final {fim.year} é anterior ao início padrão {inicio_padrao.year}."
        )

    if fim < inicio:
        raise ValueError(f"Data final menor que a inicial: {inicio} > {fim}.")

    return PeriodoProcessamento(inicio=inicio, fim=fim)
