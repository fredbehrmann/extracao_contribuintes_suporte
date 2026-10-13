"""Orquestra a carga completa do DW para uma janela temporal inclusiva.

Uso padrão (jan/2021 a ago/2025):
    python executar_pipeline_periodo.py

Outro período:
    python executar_pipeline_periodo.py --inicio 2021-01-01 --fim 2026-08-31

Conferir o plano sem executar:
    python executar_pipeline_periodo.py --dry-run
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
LOG_ROOT = BASE_DIR / "logs_pipeline_periodo"


@dataclass(frozen=True)
class Tarefa:
    arquivo: str
    recebe_periodo: bool = False


@dataclass(frozen=True)
class Fase:
    nome: str
    tarefas: tuple[Tarefa, ...]
    paralelo: bool = False


FASES = (
    Fase(
        "FASE 1 - Identificação dos contribuintes suporte",
        (Tarefa("etl_identificar.py", recebe_periodo=True),),
    ),
    Fase(
        "FASE 2 - Stagings e dimensão tempo",
        (
            Tarefa("etl_stage.py", recebe_periodo=True),
            Tarefa("etl_stage_fiscalizacoes.py", recebe_periodo=True),
            Tarefa("etl_stage_rdt.py", recebe_periodo=True),
            Tarefa("etl_stage_rdt_inadimplencia.py", recebe_periodo=True),
            Tarefa("etl_stage_consolidados_municipio.py", recebe_periodo=True),
            Tarefa("etl_dim_tempo.py"),
        ),
        paralelo=True,
    ),
    Fase(
        "FASE 3 - Complemento RDT e dimensões iniciais",
        (
            Tarefa("etl_stage_rdt_complemento_por_sq.py"),
            Tarefa("etl_dim_empresa.py"),
            Tarefa("etl_dim_fiscalizacao.py"),
        ),
        paralelo=True,
    ),
    Fase(
        "FASE 4 - Dimensões dependentes",
        (
            Tarefa("etl_dim_estabelecimento.py"),
            Tarefa("etl_dim_simples.py"),
            Tarefa("etl_dim_rdt_atributos.py"),
        ),
        paralelo=True,
    ),
    Fase(
        "FASE 5 - Dimensões cadastrais finais",
        (
            Tarefa("etl_dim_contribuinte_municipal.py"),
            Tarefa("etl_dim_socio.py"),
        ),
        paralelo=True,
    ),
    Fase(
        "FASE 6 - Fatos dos contribuintes suporte",
        (
            Tarefa("etl_fato_arrecadacao.py"),
            Tarefa("etl_fato_nfse.py"),
            Tarefa("etl_fato_inadimplencia.py"),
        ),
        paralelo=True,
    ),
    Fase(
        "FASE 7 - Fatos consolidados, fiscalização e RDT",
        (
            Tarefa("etl_fatos_consolidados_municipio.py"),
            Tarefa("etl_fatos_fiscalizacao.py"),
            Tarefa("etl_fatos_rdt.py"),
        ),
        paralelo=True,
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa toda a pipeline para uma janela temporal inclusiva."
    )
    parser.add_argument(
        "--inicio",
        default="2021-01-01",
        help="Data inicial inclusiva em YYYY-MM-DD (padrão: 2021-01-01).",
    )
    parser.add_argument(
        "--fim",
        default="2025-08-31",
        help="Data final inclusiva em YYYY-MM-DD (padrão: 2025-08-31).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=3,
        help="Máximo de ETLs simultâneos nas fases paralelas (padrão: 3).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida e mostra todos os comandos sem acessar os bancos.",
    )
    return parser.parse_args()


def converter_data(valor: str, nome: str) -> dt.date:
    try:
        return dt.datetime.strptime(valor, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{nome} inválido: {valor!r}. Use YYYY-MM-DD.") from exc


def validar_configuracao(args: argparse.Namespace) -> tuple[dt.date, dt.date]:
    inicio = converter_data(args.inicio, "--inicio")
    fim = converter_data(args.fim, "--fim")

    if fim < inicio:
        raise ValueError(f"--fim ({fim}) não pode ser anterior a --inicio ({inicio}).")
    if args.max_workers < 1:
        raise ValueError("--max-workers deve ser maior ou igual a 1.")

    ausentes = sorted(
        {
            tarefa.arquivo
            for fase in FASES
            for tarefa in fase.tarefas
            if not (BASE_DIR / tarefa.arquivo).is_file()
        }
    )
    if ausentes:
        raise FileNotFoundError("ETLs não encontrados: " + ", ".join(ausentes))

    return inicio, fim


def montar_ambiente(inicio: dt.date, fim: dt.date) -> dict[str, str]:
    ambiente = os.environ.copy()
    ambiente.update(
        {
            "DATA_INICIO": inicio.isoformat(),
            "DATA_FIM": fim.isoformat(),
            "ANO_INICIO": str(inicio.year),
            "ANO": str(fim.year),
            "ANO_REFERENCIA": str(fim.year),
            "COMPETENCIA_INICIO": f"{inicio.year:04d}{inicio.month:02d}",
            "COMPETENCIA_FIM": f"{fim.year:04d}{fim.month:02d}",
            "BEGIN_DATE": inicio.strftime("%d/%m/%Y"),
            "END_DATE": fim.strftime("%d/%m/%Y"),
        }
    )
    return ambiente


def montar_comando(tarefa: Tarefa, inicio: dt.date, fim: dt.date) -> list[str]:
    comando = [sys.executable, str(BASE_DIR / tarefa.arquivo)]
    if tarefa.recebe_periodo:
        comando.extend(["--inicio", inicio.isoformat(), "--fim", fim.isoformat()])
    return comando


def exibir_comando(comando: list[str]) -> str:
    return subprocess.list2cmdline(comando)


def executar_tarefa(
    tarefa: Tarefa,
    inicio: dt.date,
    fim: dt.date,
    ambiente: dict[str, str],
    log_dir: Path,
) -> dict[str, object]:
    comando = montar_comando(tarefa, inicio, fim)
    log_path = log_dir / f"{Path(tarefa.arquivo).stem}.log"
    instante_inicio = dt.datetime.now()

    print(f"[{instante_inicio:%Y-%m-%d %H:%M:%S}] INICIANDO: {tarefa.arquivo}", flush=True)
    print(f"    Log: {log_path}", flush=True)

    with log_path.open("w", encoding="utf-8") as log_file:
        log_file.write(f"Script: {tarefa.arquivo}\n")
        log_file.write(f"Comando: {exibir_comando(comando)}\n")
        log_file.write(f"Período: {inicio} a {fim}\n")
        log_file.write(f"Início: {instante_inicio}\n")
        log_file.write("=" * 100 + "\n\n")
        log_file.flush()

        processo = subprocess.run(
            comando,
            cwd=BASE_DIR,
            env=ambiente,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )

        instante_fim = dt.datetime.now()
        duracao = instante_fim - instante_inicio
        log_file.write("\n" + "=" * 100 + "\n")
        log_file.write(f"Fim: {instante_fim}\n")
        log_file.write(f"Duração: {duracao}\n")
        log_file.write(f"Return code: {processo.returncode}\n")

    if processo.returncode != 0:
        raise RuntimeError(
            f"Falha em {tarefa.arquivo} (código {processo.returncode}). "
            f"Consulte {log_path}."
        )

    print(
        f"[{instante_fim:%Y-%m-%d %H:%M:%S}] CONCLUÍDO: "
        f"{tarefa.arquivo} | {duracao}",
        flush=True,
    )
    return {"script": tarefa.arquivo, "duracao": duracao, "log": log_path}


def executar_fase(
    fase: Fase,
    inicio: dt.date,
    fim: dt.date,
    ambiente: dict[str, str],
    log_dir: Path,
    max_workers: int,
) -> list[dict[str, object]]:
    print("\n" + "=" * 100)
    print(fase.nome)
    print("=" * 100, flush=True)

    if not fase.paralelo or len(fase.tarefas) == 1:
        return [
            executar_tarefa(tarefa, inicio, fim, ambiente, log_dir)
            for tarefa in fase.tarefas
        ]

    resultados: list[dict[str, object]] = []
    workers = min(max_workers, len(fase.tarefas))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futuros = {
            executor.submit(
                executar_tarefa, tarefa, inicio, fim, ambiente, log_dir
            ): tarefa
            for tarefa in fase.tarefas
        }
        for futuro in as_completed(futuros):
            try:
                resultados.append(futuro.result())
            except Exception:
                for outro in futuros:
                    outro.cancel()
                raise

    return resultados


def mostrar_plano(inicio: dt.date, fim: dt.date) -> None:
    print(f"Período: {inicio} a {fim} (inclusive)")
    print(f"Competências: {inicio:%Y%m} a {fim:%Y%m}")
    for fase in FASES:
        print(f"\n{fase.nome}")
        for tarefa in fase.tarefas:
            print("  " + exibir_comando(montar_comando(tarefa, inicio, fim)))


def main() -> int:
    args = parse_args()
    try:
        inicio, fim = validar_configuracao(args)
    except (ValueError, FileNotFoundError) as exc:
        print(f"[ERRO] {exc}", file=sys.stderr)
        return 2

    if args.dry_run:
        mostrar_plano(inicio, fim)
        return 0

    run_id = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = LOG_ROOT / run_id
    log_dir.mkdir(parents=True, exist_ok=False)
    ambiente = montar_ambiente(inicio, fim)

    print("=" * 100)
    print("PIPELINE COMPLETA POR PERÍODO")
    print("=" * 100)
    print(f"Período inclusivo: {inicio} a {fim}")
    print(f"Competências: {inicio:%Y%m} a {fim:%Y%m}")
    print(f"Python: {sys.executable}")
    print(f"Logs: {log_dir}")
    print(f"Paralelismo máximo: {args.max_workers}")

    inicio_pipeline = dt.datetime.now()
    resultados: list[dict[str, object]] = []
    try:
        for fase in FASES:
            resultados.extend(
                executar_fase(
                    fase,
                    inicio,
                    fim,
                    ambiente,
                    log_dir,
                    args.max_workers,
                )
            )
    except Exception as exc:
        duracao = dt.datetime.now() - inicio_pipeline
        print("\n" + "=" * 100, file=sys.stderr)
        print("PIPELINE INTERROMPIDA COM ERRO", file=sys.stderr)
        print(f"Duração até a falha: {duracao}", file=sys.stderr)
        print(f"Erro: {exc}", file=sys.stderr)
        return 1

    duracao_total = dt.datetime.now() - inicio_pipeline
    print("\n" + "=" * 100)
    print("PIPELINE CONCLUÍDA COM SUCESSO")
    print(f"Duração total: {duracao_total}")
    print(f"Logs: {log_dir}")
    print("Resumo:")
    for resultado in resultados:
        print(f"  {resultado['script']}: {resultado['duracao']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
