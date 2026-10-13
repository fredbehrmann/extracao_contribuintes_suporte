# -*- coding: utf-8 -*-
"""
Orquestra o monitoramento de notícias em sequência no servidor de produção.

Ordem de cada execução:
  1. montar_entrada_db.py   — monta a base no banco RFB
  2. scrapper_noticias_v2.py — busca notícias e grava os resultados

A Fase 2 só começa se a Fase 1 terminar com código 0.
Os dois scripts usam pastas relativas (cache e CSV), então a execução
sempre parte da pasta deste arquivo.

Este processo não deve ser agendado na estação de trabalho. Copie a pasta
para o servidor de produção e, lá, registre o horário:

  python orquestrador_noticias.py --agendar --confirmar-producao
  python orquestrador_noticias.py --agendar --confirmar-producao --hora 07:00 --frequencia diaria
  python orquestrador_noticias.py --agendar --confirmar-producao --hora 07:00 --frequencia semanal
  python orquestrador_noticias.py --desagendar --confirmar-producao

No Windows o comando cria uma tarefa do Agendador. No Linux, uma linha no crontab
do usuário que executar o comando.
"""

import argparse
import os
import shlex
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


PASTA = Path(__file__).resolve().parent
PASTA_SAIDA = PASTA / "saida_noticias"
ARQUIVO_LOCK = PASTA_SAIDA / "orquestrador.lock"

TAREFA_AGENDADA = "Monitoramento Noticias Contribuintes Suporte"
MARCADOR_CRON = "monitoramento-noticias-contribuintes"
HORA_PADRAO = "07:00"
FREQUENCIA_PADRAO = "diaria"
LIMITE_HORAS = 12
HOST = socket.gethostname()

ETAPAS = [
    ("Fase 1 — montagem da base", "montar_entrada_db.py"),
    ("Fase 2 — busca de notícias", "scrapper_noticias_v2.py"),
]


def log(msg: str, arquivo) -> None:
    linha = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(linha, flush=True)
    arquivo.write(linha + "\n")
    arquivo.flush()


def pid_em_execucao(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        resultado = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        return str(pid) in (resultado.stdout or "")
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def adquirir_lock(arquivo_log) -> bool:
    PASTA_SAIDA.mkdir(exist_ok=True)
    if ARQUIVO_LOCK.exists():
        try:
            pid = int(ARQUIVO_LOCK.read_text(encoding="utf-8").strip())
        except ValueError:
            pid = 0
        if pid_em_execucao(pid):
            log(
                f"Execução já em andamento (PID {pid}). Esta rodada foi ignorada.",
                arquivo_log,
            )
            return False
        log(f"Lock antigo do PID {pid} descartado.", arquivo_log)
    ARQUIVO_LOCK.write_text(str(os.getpid()), encoding="utf-8")
    return True


def liberar_lock() -> None:
    try:
        if ARQUIVO_LOCK.exists():
            pid = int(ARQUIVO_LOCK.read_text(encoding="utf-8").strip())
            if pid == os.getpid():
                ARQUIVO_LOCK.unlink()
    except (OSError, ValueError):
        pass


def executar_etapa(nome: str, script: str, arquivo_log) -> int:
    caminho = PASTA / script
    if not caminho.exists():
        log(f"Arquivo não encontrado: {caminho}", arquivo_log)
        return 1

    log(f"Iniciando {nome}: {script}", arquivo_log)
    inicio = time.time()
    ambiente = os.environ.copy()
    ambiente["PYTHONIOENCODING"] = "utf-8"
    ambiente["PYTHONUNBUFFERED"] = "1"

    processo = subprocess.Popen(
        [sys.executable, "-u", str(caminho)],
        cwd=str(PASTA),
        env=ambiente,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    assert processo.stdout is not None
    for linha in processo.stdout:
        texto = linha.rstrip("\n")
        print(texto, flush=True)
        arquivo_log.write(texto + "\n")
        arquivo_log.flush()

    codigo = processo.wait()
    duracao = time.time() - inicio
    if codigo == 0:
        log(f"{nome} concluída em {duracao:.1f}s.", arquivo_log)
    else:
        log(f"{nome} falhou com código {codigo} após {duracao:.1f}s.", arquivo_log)
    return codigo


def executar() -> int:
    PASTA_SAIDA.mkdir(exist_ok=True)
    arquivo_log_path = PASTA_SAIDA / f"log_orquestrador_{datetime.now():%Y-%m-%d}.txt"
    with arquivo_log_path.open("a", encoding="utf-8") as arquivo_log:
        log(f"Orquestrador iniciado no servidor {HOST}.", arquivo_log)
        if not adquirir_lock(arquivo_log):
            return 0

        codigo_final = 0
        try:
            for nome, script in ETAPAS:
                codigo = executar_etapa(nome, script, arquivo_log)
                if codigo != 0:
                    log("Sequência interrompida. A etapa seguinte não será executada.", arquivo_log)
                    codigo_final = codigo
                    break
            else:
                log("Sequência concluída com sucesso.", arquivo_log)
        finally:
            liberar_lock()
        return codigo_final


def _powershell(script: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", "-"],
        input=script,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def _validar_horario(hora: str, frequencia: str) -> str:
    datetime.strptime(hora, "%H:%M")
    if frequencia == "semanal":
        return f"toda segunda-feira às {hora}"
    if frequencia == "diaria":
        return f"todos os dias às {hora}"
    raise ValueError("Frequência inválida. Use diaria ou semanal.")


def _recusar_fora_do_servidor() -> int:
    print(f"Agendamento recusado nesta máquina ({HOST}).")
    print("Os scripts rodam no servidor de produção. Copie a pasta para lá e execute")
    print("o mesmo comando com --confirmar-producao.")
    return 1


def agendar(hora: str, frequencia: str, confirmar_producao: bool) -> int:
    if not confirmar_producao:
        return _recusar_fora_do_servidor()
    try:
        descricao_horario = _validar_horario(hora, frequencia)
    except ValueError as exc:
        print(exc)
        return 1
    if os.name == "nt":
        return _agendar_windows(hora, frequencia, descricao_horario)
    return _agendar_cron(hora, frequencia, descricao_horario)


def _agendar_windows(hora: str, frequencia: str, descricao_horario: str) -> int:
    if frequencia == "semanal":
        gatilho = f"New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday -At '{hora}'"
    else:
        gatilho = f"New-ScheduledTaskTrigger -Daily -At '{hora}'"

    script = str(PASTA / "orquestrador_noticias.py")
    python = sys.executable
    pasta = str(PASTA)
    comando = f"""
$ErrorActionPreference = 'Stop'
$action = New-ScheduledTaskAction -Execute '{python}' -Argument '-u "{script}"' -WorkingDirectory '{pasta}'
$trigger = {gatilho}
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours {LIMITE_HORAS}) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName '{TAREFA_AGENDADA}' -Action $action -Trigger $trigger -Settings $settings -Description 'Roda a montagem da base e a busca de noticias em sequencia.' -Force | Out-Null
Write-Output 'AGENDADO'
"""
    resultado = _powershell(comando)
    if resultado.returncode != 0 or "AGENDADO" not in (resultado.stdout or ""):
        print(resultado.stdout or "")
        print(resultado.stderr or "")
        print("Não foi possível criar a tarefa agendada.")
        return resultado.returncode or 1

    print(f"Tarefa '{TAREFA_AGENDADA}' criada no servidor {HOST}: {descricao_horario}.")
    print("A conta usada no agendamento precisa conseguir acessar o banco RFB.")
    return 0


def _linha_cron(hora: str, frequencia: str) -> str:
    hora_h, minuto = (int(parte) for parte in hora.split(":"))
    dias = "1" if frequencia == "semanal" else "*"
    agenda = f"{minuto} {hora_h} * * {dias}" if frequencia == "semanal" else f"{minuto} {hora_h} * * *"
    pasta = shlex.quote(str(PASTA))
    python = shlex.quote(sys.executable)
    script = shlex.quote(str(PASTA / "orquestrador_noticias.py"))
    return (
        f"{agenda} cd {pasta} && {python} -u {script}"
        f" # {MARCADOR_CRON}"
    )


def _ler_crontab() -> str:
    resultado = subprocess.run(
        ["crontab", "-l"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if resultado.returncode != 0:
        return ""
    return resultado.stdout or ""


def _gravar_crontab(conteudo: str) -> int:
    resultado = subprocess.run(
        ["crontab", "-"],
        input=conteudo,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if resultado.returncode != 0:
        print(resultado.stdout or "")
        print(resultado.stderr or "")
        print("Não foi possível atualizar o crontab.")
        return resultado.returncode or 1
    return 0


def _agendar_cron(hora: str, frequencia: str, descricao_horario: str) -> int:
    linhas = [
        linha for linha in _ler_crontab().splitlines()
        if MARCADOR_CRON not in linha and linha.strip()
    ]
    linhas.append(_linha_cron(hora, frequencia))
    codigo = _gravar_crontab("\n".join(linhas) + "\n")
    if codigo != 0:
        return codigo
    print(f"Crontab atualizado no servidor {HOST}: {descricao_horario}.")
    return 0


def desagendar(confirmar_producao: bool) -> int:
    if not confirmar_producao:
        return _recusar_fora_do_servidor()
    if os.name == "nt":
        return _desagendar_windows()
    return _desagendar_cron()


def _desagendar_windows() -> int:
    comando = f"""
$ErrorActionPreference = 'Stop'
Unregister-ScheduledTask -TaskName '{TAREFA_AGENDADA}' -Confirm:$false
Write-Output 'REMOVIDO'
"""
    resultado = _powershell(comando)
    if resultado.returncode != 0 or "REMOVIDO" not in (resultado.stdout or ""):
        print(resultado.stdout or "")
        print(resultado.stderr or "")
        print("Não foi possível remover a tarefa agendada.")
        return resultado.returncode or 1
    print(f"Tarefa '{TAREFA_AGENDADA}' removida do servidor {HOST}.")
    return 0


def _desagendar_cron() -> int:
    linhas = [
        linha for linha in _ler_crontab().splitlines()
        if MARCADOR_CRON not in linha and linha.strip()
    ]
    codigo = _gravar_crontab(("\n".join(linhas) + "\n") if linhas else "")
    if codigo != 0:
        return codigo
    print(f"Agendamento removido do crontab no servidor {HOST}.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Executa a Fase 1 e a busca V2 em sequência no servidor de produção."
    )
    parser.add_argument(
        "--agendar",
        action="store_true",
        help=f"Cria o agendamento neste servidor (padrão: {FREQUENCIA_PADRAO} às {HORA_PADRAO}).",
    )
    parser.add_argument(
        "--desagendar",
        action="store_true",
        help="Remove o agendamento deste servidor.",
    )
    parser.add_argument(
        "--confirmar-producao",
        action="store_true",
        help="Confirma que este comando está sendo executado no servidor de produção.",
    )
    parser.add_argument("--hora", default=HORA_PADRAO, help="Horário HH:MM da tarefa agendada.")
    parser.add_argument(
        "--frequencia",
        choices=("diaria", "semanal"),
        default=FREQUENCIA_PADRAO,
        help="diaria ou semanal (segunda-feira).",
    )
    args = parser.parse_args()

    if args.agendar and args.desagendar:
        print("Use apenas um entre --agendar e --desagendar.")
        return 1
    if args.agendar:
        return agendar(args.hora, args.frequencia, args.confirmar_producao)
    if args.desagendar:
        return desagendar(args.confirmar_producao)
    return executar()


if __name__ == "__main__":
    raise SystemExit(main())
