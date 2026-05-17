# client.py
"""Cliente da multiplicação distribuída — também o modo DEMONSTRAÇÃO AO VIVO.

Fluxo da apresentação:
  1. Em um ou mais terminais: `python server.py`  (cada um escuta numa porta)
  2. Aqui: `python client.py 256`  -> acha os servidores, gera A e B, divide
     A por linhas, despacha os blocos em paralelo, recompõe C e confere a
     corretude contra o resultado serial.
  3. `python client.py 256 --comparar` -> além do distribuído, roda serial e
     paralelo-local NA HORA e mostra um quadro comparativo com o speedup
     (ótimo para a defesa: a plateia vê o ganho ao vivo).

Arquivo autocontido (protocolo e kernel repetidos do server.py de propósito,
projeto acadêmico — clareza > DRY). O benchmark.py importa as funções daqui.
"""
import argparse
import concurrent.futures
import os
import pickle
import random
import socket
import struct
import sys
import time
from multiprocessing import Pool


# ==========================================================================
# Protocolo de mensagens (idêntico ao server.py)
# ==========================================================================
_CAB = struct.Struct("!Q")


def enviar_msg(sock, objeto):
    corpo = pickle.dumps(objeto, protocol=pickle.HIGHEST_PROTOCOL)
    sock.sendall(_CAB.pack(len(corpo)) + corpo)


def _receber_exato(sock, n):
    buffer = bytearray()
    while len(buffer) < n:
        pedaco = sock.recv(min(n - len(buffer), 1 << 20))
        if not pedaco:
            raise ConnectionError(f"conexão encerrada ({len(buffer)}/{n} bytes)")
        buffer.extend(pedaco)
    return bytes(buffer)


def receber_msg(sock):
    (tamanho,) = _CAB.unpack(_receber_exato(sock, _CAB.size))
    return pickle.loads(_receber_exato(sock, tamanho))


# ==========================================================================
# Kernel + modos de execução (Python puro). O resultado SERIAL é o gabarito
# de corretude: paralelo-local e distribuído têm que devolver o mesmo C.
# ==========================================================================
def multiplicar(bloco_A, B):
    n = len(bloco_A)
    m = len(B)
    p = len(B[0])
    C = [[0] * p for _ in range(n)]
    for i in range(n):
        linha_A = bloco_A[i]
        linha_C = C[i]
        for k in range(m):
            a = linha_A[k]
            linha_B = B[k]
            for j in range(p):
                linha_C[j] += a * linha_B[j]
    return C


def dividir_linhas(total_linhas, n_partes):
    base, resto = divmod(total_linhas, n_partes)
    fatias = []
    inicio = 0
    for i in range(n_partes):
        tamanho = base + (1 if i < resto else 0)
        if tamanho > 0:
            fatias.append((inicio, inicio + tamanho))
            inicio += tamanho
    return fatias


def _worker_bloco(args):
    bloco_A, B = args
    return multiplicar(bloco_A, B)


def multiplicar_serial(A, B):
    """Baseline serial (1 processo). É o gabarito de corretude."""
    return multiplicar(A, B)


def multiplicar_paralelo_local(A, B, n_workers=None):
    """Baseline paralelo NÃO distribuído: vários processos na mesma máquina
    (multiprocessing), sem rede. multiprocessing e não threading porque o
    kernel é CPU-bound em Python puro (o GIL impediria ganho com threads)."""
    if n_workers is None:
        n_workers = os.cpu_count() or 2
    fatias = dividir_linhas(len(A), n_workers)
    tarefas = [(A[ini:fim], B) for ini, fim in fatias]
    with Pool(processes=len(tarefas)) as pool:
        partes = pool.map(_worker_bloco, tarefas)
    C = []
    for parte in partes:
        C.extend(parte)
    return C


# ==========================================================================
# Núcleo distribuído (reutilizado pelo benchmark.py)
# ==========================================================================
def gerar_matriz(linhas, colunas, semente=None):
    rng = random.Random(semente)
    return [[rng.randint(1, 10) for _ in range(colunas)] for _ in range(linhas)]


def descobrir_servidores(host="127.0.0.1", porta_inicio=5000, limite=10,
                         timeout=0.3, tentativas=1):
    """Escaneia portas procurando servidores vivos (handshake PING/PONG).

    `tentativas` > 1 repete a varredura e une os resultados — útil na
    demonstração ao vivo, caso um servidor demore alguns ms a mais para
    ficar pronto (a tabela não fica faltando um nó na frente da turma).
    """
    ativos = {}
    for t in range(tentativas):
        for porta in range(porta_inicio, porta_inicio + limite):
            if (host, porta) in ativos:
                continue
            try:
                with socket.create_connection((host, porta), timeout=timeout) as s:
                    s.settimeout(timeout)
                    enviar_msg(s, {"tipo": "PING"})
                    if receber_msg(s).get("tipo") == "PONG":
                        ativos[(host, porta)] = True
            except (OSError, ConnectionError, EOFError):
                pass
        if t + 1 < tentativas:
            time.sleep(0.4)
    return list(ativos)


def _falar_com_servidor(host, porta, bloco_A, B, paralelo, linha_inicio):
    with socket.create_connection((host, porta)) as s:
        enviar_msg(s, {
            "tipo": "MULTIPLICAR", "bloco_A": bloco_A, "matriz_B": B,
            "paralelo": paralelo, "linha_inicio": linha_inicio,
        })
        return receber_msg(s)


def executar_distribuido(A, B, servidores, paralelo=True,
                         ao_despachar=None, ao_receber=None):
    """Distribui a multiplicação entre os servidores e recompõe C.

    Devolve (C, metricas):
      - tempo_total: relógio de parede do despacho até recompor C
      - tempo_max_servidor: maior tempo de cálculo entre os nós
      - overhead: tempo_total - tempo_max_servidor (rede + serialização)
      - por_servidor: porta, linhas e tempo de cálculo de cada nó
    """
    n = len(servidores)
    fatias = dividir_linhas(len(A), n)

    resultados = {}
    por_servidor = []
    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=n) as executor:
        futuros = {}
        for idx, ((host, porta), (ini, fim)) in enumerate(zip(servidores, fatias)):
            if ao_despachar:
                ao_despachar(idx, host, porta, ini, fim)
            fut = executor.submit(_falar_com_servidor, host, porta,
                                   A[ini:fim], B, paralelo, ini)
            futuros[fut] = (idx, porta, fim - ini)

        for fut in concurrent.futures.as_completed(futuros):
            idx, porta, n_linhas = futuros[fut]
            resp = fut.result()
            if resp.get("tipo") != "RESULTADO":
                raise RuntimeError(f"servidor {porta} respondeu: {resp}")
            resultados[resp["linha_inicio"]] = resp["bloco_C"]
            por_servidor.append({"porta": porta, "linhas": n_linhas,
                                 "tempo_calculo": resp.get("tempo_calculo", 0.0)})
            if ao_receber:
                ao_receber(idx, resp)

    C = []
    for ini in sorted(resultados):
        C.extend(resultados[ini])
    tempo_total = time.perf_counter() - t0

    tempo_max = max((s["tempo_calculo"] for s in por_servidor), default=0.0)
    metricas = {
        "tempo_total": tempo_total,
        "tempo_max_servidor": tempo_max,
        "overhead": tempo_total - tempo_max,
        "por_servidor": sorted(por_servidor, key=lambda s: s["porta"]),
    }
    return C, metricas


def verificar(C, A, B):
    """Confere C contra o gabarito serial (Python puro)."""
    return multiplicar_serial(A, B) == C


# ==========================================================================
# UI / Demonstração ao vivo (rich). Medição de tempo fica no núcleo.
# ==========================================================================
def _executar_com_ui(tamanho, paralelo, servidores_cli, host, porta_inicio,
                     limite, verificar_resultado, comparar):
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.live import Live
    from rich import box

    console = Console()
    console.rule("[bold cyan]Multiplicação de Matrizes Distribuída[/]")

    cab = Table.grid(padding=(0, 2))
    cab.add_column(justify="right", style="cyan")
    cab.add_column()
    cab.add_row("Dimensão", f"{tamanho} x {tamanho}")
    cab.add_row("Paralelismo no nó", "sim" if paralelo else "não")
    console.print(Panel(cab, title="Parâmetros", border_style="cyan"))

    if servidores_cli:
        servidores = servidores_cli
        console.print(f"[green]Servidores informados:[/] {servidores}")
    else:
        with console.status("[cyan]Procurando servidores ativos..."):
            servidores = descobrir_servidores(host, porta_inicio, limite,
                                               tentativas=2)
    if not servidores:
        console.print("[bold red]Nenhum servidor encontrado. "
                      "Suba o server.py primeiro.[/]")
        sys.exit(1)
    console.print(f"[bold green]{len(servidores)} servidor(es):[/] "
                  + ", ".join(f"{h}:{p}" for h, p in servidores))

    console.print(f"[cyan]Gerando matrizes {tamanho}x{tamanho}...[/]")
    A = gerar_matriz(tamanho, tamanho, semente=1)
    B = gerar_matriz(tamanho, tamanho, semente=2)

    estado = {i: {"porta": p, "linhas": "-", "status": "—", "tempo": "-"}
              for i, (_, p) in enumerate(servidores)}

    def tabela():
        t = Table(box=box.ROUNDED, title="Nós de processamento")
        t.add_column("Servidor", style="cyan")
        t.add_column("Linhas de A", justify="right")
        t.add_column("Status")
        t.add_column("Tempo cálculo", justify="right")
        for i in sorted(estado):
            s = estado[i]
            t.add_row(f'porta {s["porta"]}', str(s["linhas"]),
                      s["status"], str(s["tempo"]))
        return t

    with Live(tabela(), console=console, refresh_per_second=8) as live:
        def ao_despachar(idx, h, p, ini, fim):
            estado[idx]["linhas"] = f"{ini}–{fim - 1}"
            estado[idx]["status"] = "[yellow]calculando...[/]"
            live.update(tabela())

        def ao_receber(idx, resp):
            estado[idx]["status"] = "[green]concluído[/]"
            estado[idx]["tempo"] = f'{resp.get("tempo_calculo", 0):.4f}s'
            live.update(tabela())

        C, m = executar_distribuido(A, B, servidores, paralelo,
                                    ao_despachar, ao_receber)
        live.update(tabela())

    resumo = Table.grid(padding=(0, 2))
    resumo.add_column(justify="right", style="cyan")
    resumo.add_column()
    resumo.add_row("Dimensão de C", f"{len(C)} x {len(C[0])}")
    resumo.add_row("Tempo total distribuído", f'{m["tempo_total"]:.4f} s')
    resumo.add_row("Maior cálculo num nó", f'{m["tempo_max_servidor"]:.4f} s')
    resumo.add_row("Overhead (rede+serial.)", f'{m["overhead"]:.4f} s')
    if verificar_resultado:
        ok = verificar(C, A, B)
        resumo.add_row("Corretude (vs serial)",
                       "[bold green]CORRETO[/]" if ok else "[bold red]INCORRETO[/]")
    console.print(Panel(resumo, title="[bold]Resultado distribuído[/]",
                        border_style="green"))

    # Demonstração ao vivo: roda serial e paralelo-local AGORA e compara.
    if comparar:
        td = m["tempo_total"]
        with console.status("[cyan]Rodando serial localmente para comparar..."):
            t0 = time.perf_counter()
            multiplicar_serial(A, B)
            ts = time.perf_counter() - t0
        nw = os.cpu_count() or 2
        with console.status(f"[cyan]Rodando paralelo-local ({nw} processos)..."):
            t0 = time.perf_counter()
            multiplicar_paralelo_local(A, B, nw)
            tp = time.perf_counter() - t0

        comp = Table(box=box.ROUNDED, title="Comparação ao vivo (mesmo A e B)")
        comp.add_column("Modo", style="cyan")
        comp.add_column("Tempo (s)", justify="right")
        comp.add_column("Speedup vs serial", justify="right")
        comp.add_row("serial", f"{ts:.4f}", "1.00x")
        comp.add_row(f"paralelo-local ({nw} proc.)", f"{tp:.4f}",
                     f"{ts / tp:.2f}x" if tp else "-")
        comp.add_row(f"distribuído ({len(servidores)} nós)", f"{td:.4f}",
                     f"{ts / td:.2f}x" if td else "-")
        console.print(comp)

    if tamanho <= 8:
        console.print("[dim]A =[/]", A)
        console.print("[dim]B =[/]", B)
        console.print("[dim]C =[/]", C)


def _parse_servidores(texto):
    if not texto:
        return None
    saida = []
    for item in texto.split(","):
        host, porta = item.strip().rsplit(":", 1)
        saida.append((host, int(porta)))
    return saida


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Cliente / demonstração ao vivo da multiplicação distribuída")
    ap.add_argument("tamanho", nargs="?", type=int, default=100,
                    help="dimensão N das matrizes NxN (padrão 100)")
    ap.add_argument("--comparar", action="store_true",
                    help="também roda serial e paralelo-local e mostra o speedup")
    ap.add_argument("--sem-paralelo-no-no", action="store_true",
                    help="servidor não subdivide o bloco entre núcleos")
    ap.add_argument("--servers", default=None,
                    help='lista fixa "host:porta,host:porta" (pula a descoberta)')
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port-inicial", type=int, default=5000)
    ap.add_argument("--limite", type=int, default=10)
    ap.add_argument("--no-verify", action="store_true",
                    help="não comparar com o gabarito serial")
    args = ap.parse_args()

    try:
        _executar_com_ui(
            tamanho=args.tamanho,
            paralelo=not args.sem_paralelo_no_no,
            servidores_cli=_parse_servidores(args.servers),
            host=args.host,
            porta_inicio=args.port_inicial,
            limite=args.limite,
            verificar_resultado=not args.no_verify,
            comparar=args.comparar,
        )
    except KeyboardInterrupt:
        print("\n[!] Cancelado pelo usuário.")
        sys.exit(1)
