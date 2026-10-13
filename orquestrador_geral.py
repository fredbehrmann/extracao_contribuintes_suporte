# -*- coding: utf-8 -*-
"""Orquestra a carga do DW e o monitoramento de notícias em uma única sequência.

Fases:
  1. Identificação da carteira (somente no primeiro processamento do ano ou sob demanda)
  2-7. Stagings, dimensões e fatos do DW
  8. Montagem da base de notícias
  9. Busca de notícias

A Fase 1 roda automaticamente quando stg_contribuintes_suporte não tem registros
do ano corrente (ano da data final). Use --com-identificacao para forçar e
--sem-identificacao para impedir.

Uso:
    python orquestrador_geral.py
    python orquestrador_geral.py --com-identificacao
    python orquestrador_geral.py --escopo dw
    python orquestrador_geral.py --escopo noticias
    python orquestrador_geral.py --dry-run

Agendamento semanal (somente no servidor de produção):
    python orquestrador_geral.py --agendar --confirmar-producao --hora 06:00
    python orquestrador_geral.py --agendar --confirmar-producao --dia-semana sexta
    python orquestrador_geral.py --desagendar --confirmar-producao

Códigos de saída: 0 sucesso, 1 falha, 2 configuração inválida,
3 DW concluído mas fases de notícias com falha.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import os
import shlex
import socket
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
LOG_ROOT = BASE_DIR / "logs_orquestrador_geral"
ARQUIVO_LOCK = LOG_ROOT / "orquestrador_geral.lock"
ARQUIVO_LOCK_NOTICIAS = BASE_DIR / "saida_noticias" / "orquestrador.lock"

INICIO_PADRAO = dt.date(2021, 1, 1)
SCHEMA = "suporte_arrecadacao"
TABELA_CARTEIRA = "stg_contribuintes_suporte"

PG_CONFIG = {
    "dbname": "db_rfb",
    "user": "cif_user",
    "password": os.getenv("RFB_PG_PASS", ""),
    "host": "valedosrios",
    "port": 5432,
}

TAREFA_AGENDADA = "Orquestrador Geral Contribuintes Suporte"
MARCADOR_CRON = "orquestrador-geral-contribuintes"
HORA_PADRAO = "06:00"
DIA_SEMANA_PADRAO = "segunda"
LIMITE_HORAS_PADRAO = 23
HOST = socket.gethostname()

DIAS_SEMANA = {
    "domingo": ("Sunday", 0),
    "segunda": ("Monday", 1),
    "terca": ("Tuesday", 2),
    "quarta": ("Wednesday", 3),
    "quinta": ("Thursday", 4),
    "sexta": ("Friday", 5),
    "sabado": ("Saturday", 6),
}

CODIGO_SUCESSO = 0
CODIGO_FALHA = 1
CODIGO_CONFIG = 2
CODIGO_AVISOS = 3


@dataclass(frozen=True)
class Tarefa:
    arquivo: str
    recebe_periodo: bool = False


@dataclass(frozen=True)
class Fase:
    nome: str
    tarefas: tuple[Tarefa, ...]
    paralelo: bool = False
    grupo: str = "dw"


FASES = (
    Fase(
        "FASE 1 - Identificação dos contribuintes suporte",
        (Tarefa("etl_identificar.py", recebe_periodo=True),),
        grupo="identificacao",
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
    Fase(
        "FASE 8 - Montagem da base de notícias",
        (Tarefa("montar_entrada_db.py"),),
        grupo="noticias",
    ),
    Fase(
        "FASE 9 - Busca de notícias",
        (Tarefa("scrapper_noticias_v2.py"),),
        grupo="noticias",
    ),
)

GRUPOS_POR_ESCOPO = {
    "tudo": {"identificacao", "dw", "noticias"},
    "dw": {"identificacao", "dw"},
    "noticias": {"noticias"},
}


@dataclass(frozen=True)
class Configuracao:
    inicio: dt.date
    fim: dt.date
    inicio_identificacao: dt.date
    escopo: str
    max_workers: int


# =========================================================
# LOG
# =========================================================

class Registro:
    """Escreve no console e, quando houver pasta de execução, no resumo.log."""

    def __init__(self) -> None:
        self._arquivo = None
        self._trava = threading.Lock()

    def abrir(self, caminho: Path) -> None:
        self._arquivo = caminho.open("a", encoding="utf-8")

    def fechar(self) -> None:
        if self._arquivo:
            self._arquivo.close()
            self._arquivo = None

    def __call__(self, msg: str = "", erro: bool = False) -> None:
        linha = f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}" if msg else ""
        with self._trava:
            print(linha, file=sys.stderr if erro else sys.stdout, flush=True)
            if self._arquivo:
                self._arquivo.write(linha + "\n")
                self._arquivo.flush()


log = Registro()


# =========================================================
# CONFIGURAÇÃO
# =========================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa identificação, DW e notícias em ordem de dependência."
    )
    parser.add_argument("--inicio", default=INICIO_PADRAO.isoformat(),
                        help=f"Data inicial inclusiva YYYY-MM-DD (padrão: {INICIO_PADRAO}).")
    parser.add_argument("--fim", default=dt.date.today().isoformat(),
                        help="Data final inclusiva YYYY-MM-DD (padrão: hoje).")
    parser.add_argument("--escopo", choices=tuple(GRUPOS_POR_ESCOPO), default="tudo",
                        help="tudo (fases 1-9), dw (fases 1-7) ou noticias (fases 8-9).")
    parser.add_argument("--max-workers", type=int, default=3,
                        help="Máximo de ETLs simultâneos nas fases paralelas (padrão: 3).")
    parser.add_argument("--com-identificacao", action="store_true",
                        help="Força a execução da Fase 1.")
    parser.add_argument("--sem-identificacao", action="store_true",
                        help="Impede a execução automática da Fase 1.")
    parser.add_argument("--identificacao-completa", action="store_true",
                        help="Na Fase 1, reconstrói todos os exercícios da janela, não só o corrente.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Mostra a decisão da Fase 1 e os comandos sem executar os ETLs.")

    agendamento = parser.add_argument_group("agendamento")
    agendamento.add_argument("--agendar", action="store_true",
                             help="Cria o agendamento semanal neste servidor.")
    agendamento.add_argument("--desagendar", action="store_true",
                             help="Remove o agendamento deste servidor.")
    agendamento.add_argument("--confirmar-producao", action="store_true",
                             help="Confirma que o comando está sendo executado no servidor de produção.")
    agendamento.add_argument("--hora", default=HORA_PADRAO,
                             help=f"Horário HH:MM da tarefa (padrão: {HORA_PADRAO}).")
    agendamento.add_argument("--dia-semana", choices=tuple(DIAS_SEMANA), default=DIA_SEMANA_PADRAO,
                             help=f"Dia da semana da tarefa (padrão: {DIA_SEMANA_PADRAO}).")
    agendamento.add_argument("--limite-horas", type=int, default=LIMITE_HORAS_PADRAO,
                             help=f"Tempo máximo da tarefa no Agendador (padrão: {LIMITE_HORAS_PADRAO}).")
    return parser.parse_args()


def converter_data(valor: str, nome: str) -> dt.date:
    try:
        return dt.datetime.strptime(valor, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{nome} inválido: {valor!r}. Use YYYY-MM-DD.") from exc


def validar_configuracao(args: argparse.Namespace) -> Configuracao:
    if args.agendar and args.desagendar:
        raise ValueError("Use apenas um entre --agendar e --desagendar.")
    if args.com_identificacao and args.sem_identificacao:
        raise ValueError("Use apenas um entre --com-identificacao e --sem-identificacao.")
    if args.identificacao_completa and args.sem_identificacao:
        raise ValueError("--identificacao-completa não combina com --sem-identificacao.")
    if args.escopo == "noticias" and (args.com_identificacao or args.identificacao_completa):
        raise ValueError("A Fase 1 não faz parte do escopo 'noticias'. Use --escopo tudo ou dw.")
    if args.max_workers < 1:
        raise ValueError("--max-workers deve ser maior ou igual a 1.")
    if args.limite_horas < 1:
        raise ValueError("--limite-horas deve ser maior ou igual a 1.")
    try:
        dt.datetime.strptime(args.hora, "%H:%M")
    except ValueError as exc:
        raise ValueError(f"--hora inválida: {args.hora!r}. Use HH:MM.") from exc

    inicio = converter_data(args.inicio, "--inicio")
    fim = converter_data(args.fim, "--fim")
    if fim < inicio:
        raise ValueError(f"--fim ({fim}) não pode ser anterior a --inicio ({inicio}).")

    if args.identificacao_completa:
        inicio_identificacao = inicio
    else:
        inicio_identificacao = max(inicio, dt.date(fim.year, 1, 1))

    grupos = GRUPOS_POR_ESCOPO[args.escopo]
    ausentes = sorted(
        tarefa.arquivo
        for fase in FASES if fase.grupo in grupos
        for tarefa in fase.tarefas
        if not (BASE_DIR / tarefa.arquivo).is_file()
    )
    if ausentes:
        raise FileNotFoundError("Scripts não encontrados: " + ", ".join(ausentes))

    return Configuracao(
        inicio=inicio,
        fim=fim,
        inicio_identificacao=inicio_identificacao,
        escopo=args.escopo,
        max_workers=args.max_workers,
    )


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
            "PYTHONIOENCODING": "utf-8",
            "PYTHONUNBUFFERED": "1",
        }
    )
    return ambiente


def montar_comando(tarefa: Tarefa, fase: Fase, cfg: Configuracao) -> list[str]:
    comando = [sys.executable, "-u", str(BASE_DIR / tarefa.arquivo)]
    if tarefa.recebe_periodo:
        inicio = cfg.inicio_identificacao if fase.grupo == "identificacao" else cfg.inicio
        comando.extend(["--inicio", inicio.isoformat(), "--fim", cfg.fim.isoformat()])
    return comando


def exibir_comando(comando: list[str]) -> str:
    return subprocess.list2cmdline(comando)


# =========================================================
# DECISÃO DA FASE 1
# =========================================================

def contar_carteira(ano: int | None) -> int:
    """Conta registros da carteira; ano=None conta todos os exercícios."""
    from sqlalchemy import create_engine, text

    url = (
        f"postgresql+psycopg2://{PG_CONFIG['user']}:{PG_CONFIG['password']}"
        f"@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['dbname']}"
    )
    engine = create_engine(url, connect_args={"connect_timeout": 15})
    try:
        with engine.connect() as conn:
            existe = conn.execute(
                text("SELECT to_regclass(:tabela)"),
                {"tabela": f"{SCHEMA}.{TABELA_CARTEIRA}"},
            ).scalar()
            if existe is None:
                return 0
            if ano is None:
                sql = text(f"SELECT count(*) FROM {SCHEMA}.{TABELA_CARTEIRA}")
                return int(conn.execute(sql).scalar() or 0)
            sql = text(f"SELECT count(*) FROM {SCHEMA}.{TABELA_CARTEIRA} WHERE ano = :ano")
            return int(conn.execute(sql, {"ano": ano}).scalar() or 0)
    finally:
        engine.dispose()


def decidir_identificacao(args: argparse.Namespace, cfg: Configuracao) -> tuple[bool, str]:
    """Retorna (executar_fase_1, motivo). Propaga erro de conexão ao banco."""
    if "identificacao" not in GRUPOS_POR_ESCOPO[cfg.escopo]:
        return False, f"fora do escopo '{cfg.escopo}'"
    if args.com_identificacao:
        return True, "solicitada por --com-identificacao"
    if args.sem_identificacao:
        return False, "suprimida por --sem-identificacao"

    ano = cfg.fim.year
    total = contar_carteira(ano)
    if total == 0:
        return True, f"primeiro processamento de {ano}: carteira do ano ainda vazia"
    return False, f"carteira de {ano} já identificada ({total:,} registros)"


# =========================================================
# TRAVA DE EXECUÇÃO
# =========================================================

def pid_em_execucao(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        resultado = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        return str(pid) in (resultado.stdout or "")
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def ler_pid(caminho: Path) -> int:
    try:
        return int(caminho.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return 0


def adquirir_lock() -> str | None:
    """Retorna None se adquiriu a trava; caso contrário, o motivo da recusa."""
    LOG_ROOT.mkdir(exist_ok=True)
    if ARQUIVO_LOCK.exists():
        pid = ler_pid(ARQUIVO_LOCK)
        if pid_em_execucao(pid):
            return f"orquestrador geral já em execução (PID {pid})"
        log(f"Trava antiga do PID {pid} descartada.")
    if ARQUIVO_LOCK_NOTICIAS.exists():
        pid = ler_pid(ARQUIVO_LOCK_NOTICIAS)
        if pid_em_execucao(pid):
            return f"orquestrador_noticias.py em execução (PID {pid})"
    ARQUIVO_LOCK.write_text(str(os.getpid()), encoding="utf-8")
    return None


def liberar_lock() -> None:
    if ler_pid(ARQUIVO_LOCK) == os.getpid():
        try:
            ARQUIVO_LOCK.unlink()
        except OSError:
            pass


# =========================================================
# EXECUÇÃO
# =========================================================

def executar_tarefa(
    tarefa: Tarefa, fase: Fase, cfg: Configuracao, ambiente: dict[str, str], log_dir: Path
) -> dict[str, object]:
    comando = montar_comando(tarefa, fase, cfg)
    log_path = log_dir / f"{Path(tarefa.arquivo).stem}.log"
    instante_inicio = dt.datetime.now()

    log(f"INICIANDO: {tarefa.arquivo} | log: {log_path.name}")

    with log_path.open("w", encoding="utf-8") as log_file:
        log_file.write(f"Script: {tarefa.arquivo}\n")
        log_file.write(f"Comando: {exibir_comando(comando)}\n")
        log_file.write(f"Início: {instante_inicio}\n")
        log_file.write("=" * 100 + "\n\n")
        log_file.flush()

        processo = subprocess.run(
            comando,
            cwd=BASE_DIR,
            env=ambiente,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            check=False,
        )

        duracao = dt.datetime.now() - instante_inicio
        log_file.write("\n" + "=" * 100 + "\n")
        log_file.write(f"Fim: {dt.datetime.now()}\n")
        log_file.write(f"Duração: {duracao}\n")
        log_file.write(f"Return code: {processo.returncode}\n")

    if processo.returncode != 0:
        raise RuntimeError(
            f"Falha em {tarefa.arquivo} (código {processo.returncode}). Consulte {log_path}."
        )

    log(f"CONCLUÍDO: {tarefa.arquivo} | {duracao}")
    return {"script": tarefa.arquivo, "duracao": duracao}


def executar_fase(
    fase: Fase, cfg: Configuracao, ambiente: dict[str, str], log_dir: Path
) -> list[dict[str, object]]:
    log("=" * 80)
    log(fase.nome)
    log("=" * 80)

    if not fase.paralelo or len(fase.tarefas) == 1:
        return [executar_tarefa(t, fase, cfg, ambiente, log_dir) for t in fase.tarefas]

    resultados: list[dict[str, object]] = []
    workers = min(cfg.max_workers, len(fase.tarefas))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futuros = {
            executor.submit(executar_tarefa, t, fase, cfg, ambiente, log_dir): t
            for t in fase.tarefas
        }
        for futuro in as_completed(futuros):
            try:
                resultados.append(futuro.result())
            except Exception:
                for outro in futuros:
                    outro.cancel()
                raise
    return resultados


def fases_selecionadas(cfg: Configuracao, rodar_identificacao: bool) -> list[Fase]:
    grupos = GRUPOS_POR_ESCOPO[cfg.escopo]
    return [
        fase for fase in FASES
        if fase.grupo in grupos and (fase.grupo != "identificacao" or rodar_identificacao)
    ]


def mostrar_plano(args: argparse.Namespace, cfg: Configuracao) -> int:
    print(f"Período: {cfg.inicio} a {cfg.fim} (inclusive) | escopo: {cfg.escopo}")
    try:
        rodar_identificacao, motivo = decidir_identificacao(args, cfg)
        decisao = "EXECUTAR" if rodar_identificacao else "PULAR"
    except Exception as exc:
        rodar_identificacao, motivo = True, f"banco inacessível ({exc.__class__.__name__}: {exc})"
        decisao = "INDETERMINADA"
    print(f"Fase 1: {decisao} - {motivo}")

    for fase in fases_selecionadas(cfg, rodar_identificacao):
        sufixo = " (condicional)" if fase.grupo == "identificacao" and decisao == "INDETERMINADA" else ""
        modo = "paralelo" if fase.paralelo and len(fase.tarefas) > 1 else "sequencial"
        print(f"\n{fase.nome} [{modo}]{sufixo}")
        for tarefa in fase.tarefas:
            print("  " + exibir_comando(montar_comando(tarefa, fase, cfg)))
    return CODIGO_SUCESSO


def executar(args: argparse.Namespace, cfg: Configuracao) -> int:
    run_id = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = LOG_ROOT / run_id
    log_dir.mkdir(parents=True, exist_ok=False)
    log.abrir(log_dir / "resumo.log")

    try:
        log("=" * 80)
        log(f"ORQUESTRADOR GERAL | servidor {HOST}")
        log(f"Período: {cfg.inicio} a {cfg.fim} | escopo: {cfg.escopo}")
        log(f"Python: {sys.executable}")
        log(f"Logs: {log_dir}")

        recusa = adquirir_lock()
        if recusa:
            log(f"Execução não iniciada: {recusa}.", erro=True)
            return CODIGO_SUCESSO if recusa.startswith("orquestrador geral") else CODIGO_FALHA

        try:
            return _executar_fases(args, cfg, log_dir)
        finally:
            liberar_lock()
    finally:
        log.fechar()


def _executar_fases(args: argparse.Namespace, cfg: Configuracao, log_dir: Path) -> int:
    try:
        rodar_identificacao, motivo = decidir_identificacao(args, cfg)
    except Exception as exc:
        log(f"Não foi possível verificar a carteira no banco: {exc}", erro=True)
        log("Execução interrompida antes das cargas.", erro=True)
        return CODIGO_FALHA
    log(f"Fase 1: {'EXECUTAR' if rodar_identificacao else 'PULAR'} - {motivo}")
    if rodar_identificacao:
        log(f"Fase 1 usará o período {cfg.inicio_identificacao} a {cfg.fim}.")

    if cfg.escopo == "noticias":
        try:
            if contar_carteira(None) == 0:
                log("stg_contribuintes_suporte está vazia. Rode o escopo dw antes.", erro=True)
                return CODIGO_FALHA
        except Exception as exc:
            log(f"Não foi possível verificar a carteira no banco: {exc}", erro=True)
            return CODIGO_FALHA

    ambiente = montar_ambiente(cfg.inicio, cfg.fim)
    inicio_pipeline = dt.datetime.now()
    resultados: list[dict[str, object]] = []
    avisos: list[str] = []

    for fase in fases_selecionadas(cfg, rodar_identificacao):
        try:
            resultados.extend(executar_fase(fase, cfg, ambiente, log_dir))
        except Exception as exc:
            if fase.grupo == "noticias":
                avisos.append(f"{fase.nome}: {exc}")
                log(f"AVISO: {exc}", erro=True)
                log("Fases de notícias seguintes não serão executadas.", erro=True)
                break
            log("=" * 80, erro=True)
            log("ORQUESTRADOR INTERROMPIDO COM ERRO", erro=True)
            log(f"Duração até a falha: {dt.datetime.now() - inicio_pipeline}", erro=True)
            log(f"Erro: {exc}", erro=True)
            return CODIGO_FALHA

    log("=" * 80)
    log("CONCLUÍDO COM AVISOS" if avisos else "CONCLUÍDO COM SUCESSO")
    log(f"Duração total: {dt.datetime.now() - inicio_pipeline}")
    for resultado in resultados:
        log(f"  {resultado['script']}: {resultado['duracao']}")
    for aviso in avisos:
        log(f"  AVISO - {aviso}")
    return CODIGO_AVISOS if avisos else CODIGO_SUCESSO


# =========================================================
# AGENDAMENTO
# =========================================================

def _powershell(script: str) -> subprocess.CompletedProcess:
    codificado = base64.b64encode(script.encode("utf-16-le")).decode("ascii")
    return subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-EncodedCommand", codificado],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )


def _ps_literal(valor: str) -> str:
    return "'" + valor.replace("'", "''") + "'"


def _argumentos_agendados(escopo: str) -> list[str]:
    return [] if escopo == "tudo" else ["--escopo", escopo]


def _recusar_fora_do_servidor() -> int:
    print(f"Agendamento recusado nesta máquina ({HOST}).")
    print("Os scripts rodam no servidor de produção. Copie a pasta para lá e execute")
    print("o mesmo comando com --confirmar-producao.")
    return CODIGO_FALHA


def agendar(args: argparse.Namespace) -> int:
    if not args.confirmar_producao:
        return _recusar_fora_do_servidor()
    descricao = f"toda {args.dia_semana} às {args.hora}"
    if os.name == "nt":
        return _agendar_windows(args, descricao)
    return _agendar_cron(args, descricao)


def _agendar_windows(args: argparse.Namespace, descricao: str) -> int:
    dia_ps, _ = DIAS_SEMANA[args.dia_semana]
    script = BASE_DIR / "orquestrador_geral.py"
    argumentos = subprocess.list2cmdline(["-u", str(script), *_argumentos_agendados(args.escopo)])
    comando = f"""
$ErrorActionPreference = 'Stop'
$action = New-ScheduledTaskAction -Execute {_ps_literal(sys.executable)} -Argument {_ps_literal(argumentos)} -WorkingDirectory {_ps_literal(str(BASE_DIR))}
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek {dia_ps} -At '{args.hora}'
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours {args.limite_horas}) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName {_ps_literal(TAREFA_AGENDADA)} -Action $action -Trigger $trigger -Settings $settings -Description 'Executa identificacao, DW e noticias dos contribuintes suporte.' -Force | Out-Null
Write-Output 'AGENDADO'
"""
    resultado = _powershell(comando)
    if resultado.returncode != 0 or "AGENDADO" not in (resultado.stdout or ""):
        print(resultado.stdout or "")
        print(resultado.stderr or "")
        print("Não foi possível criar a tarefa agendada.")
        return resultado.returncode or CODIGO_FALHA
    print(f"Tarefa '{TAREFA_AGENDADA}' criada no servidor {HOST}: {descricao}.")
    print("A conta usada no agendamento precisa acessar os bancos Oracle, o Postgres e a internet.")
    return CODIGO_SUCESSO


def _ler_crontab() -> str:
    resultado = subprocess.run(
        ["crontab", "-l"], capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    return (resultado.stdout or "") if resultado.returncode == 0 else ""


def _gravar_crontab(conteudo: str) -> int:
    resultado = subprocess.run(
        ["crontab", "-"], input=conteudo, capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    )
    if resultado.returncode != 0:
        print(resultado.stdout or "")
        print(resultado.stderr or "")
        print("Não foi possível atualizar o crontab.")
        return resultado.returncode or CODIGO_FALHA
    return CODIGO_SUCESSO


def _linhas_crontab_sem_marcador() -> list[str]:
    return [l for l in _ler_crontab().splitlines() if MARCADOR_CRON not in l and l.strip()]


def _agendar_cron(args: argparse.Namespace, descricao: str) -> int:
    hora, minuto = (int(parte) for parte in args.hora.split(":"))
    _, dia_cron = DIAS_SEMANA[args.dia_semana]
    comando = " ".join(
        shlex.quote(parte)
        for parte in [sys.executable, "-u", str(BASE_DIR / "orquestrador_geral.py"),
                      *_argumentos_agendados(args.escopo)]
    )
    linha = f"{minuto} {hora} * * {dia_cron} cd {shlex.quote(str(BASE_DIR))} && {comando} # {MARCADOR_CRON}"
    codigo = _gravar_crontab("\n".join([*_linhas_crontab_sem_marcador(), linha]) + "\n")
    if codigo == CODIGO_SUCESSO:
        print(f"Crontab atualizado no servidor {HOST}: {descricao}.")
    return codigo


def desagendar(args: argparse.Namespace) -> int:
    if not args.confirmar_producao:
        return _recusar_fora_do_servidor()
    if os.name == "nt":
        comando = f"""
$ErrorActionPreference = 'Stop'
Unregister-ScheduledTask -TaskName {_ps_literal(TAREFA_AGENDADA)} -Confirm:$false
Write-Output 'REMOVIDO'
"""
        resultado = _powershell(comando)
        if resultado.returncode != 0 or "REMOVIDO" not in (resultado.stdout or ""):
            print(resultado.stdout or "")
            print(resultado.stderr or "")
            print("Não foi possível remover a tarefa agendada.")
            return resultado.returncode or CODIGO_FALHA
        print(f"Tarefa '{TAREFA_AGENDADA}' removida do servidor {HOST}.")
        return CODIGO_SUCESSO

    linhas = _linhas_crontab_sem_marcador()
    codigo = _gravar_crontab(("\n".join(linhas) + "\n") if linhas else "")
    if codigo == CODIGO_SUCESSO:
        print(f"Agendamento removido do crontab no servidor {HOST}.")
    return codigo


# =========================================================
# MAIN
# =========================================================

def main() -> int:
    args = parse_args()
    try:
        cfg = validar_configuracao(args)
    except (ValueError, FileNotFoundError) as exc:
        print(f"[ERRO] {exc}", file=sys.stderr)
        return CODIGO_CONFIG

    if args.agendar:
        return agendar(args)
    if args.desagendar:
        return desagendar(args)
    if args.dry_run:
        return mostrar_plano(args, cfg)
    return executar(args, cfg)


if __name__ == "__main__":
    raise SystemExit(main())
