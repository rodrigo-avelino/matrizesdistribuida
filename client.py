# client.py
"""Cliente da multiplicação distribuída.

Responsável por: gerar A e B, descobrir os servidores, particionar A por
linhas, despachar os blocos em paralelo (uma thread por servidor, pois a
espera é de rede), recompor C na ordem certa e verificar a corretude contra
o resultado serial (gabarito em Python puro).

O núcleo (`descobrir_servidores`, `executar_distribuido`) é importável sem
nenhuma UI — é exatamente o que o benchmark.py reaproveita.

Uso:
    python client.py 256
    python client.py 256 --sem-paralelo-no-no
    python client.py 128 --servers 127.0.0.1:5000,127.0.0.1:5001
"""
import argparse
import concurrent.futures
import random
import socket
import sys
import time

import compute
from protocol import enviar_msg, receber_msg


# --------------------------------------------------------------------------
# Núcleo reutilizável (sem UI)
# --------------------------------------------------------------------------
def gerar_matriz(linhas, colunas, semente=None):
    rng = random.Random(semente)
    return [[rng.randint(1, 10) for _ in range(colunas)] for _ in range(linhas)]


def descobrir_servidores(host="127.0.0.1", porta_inicio=5000, limite=10, timeout=0.3):
    """Escaneia portas procurando servidores vivos (handshake PING/PONG)."""
    ativos = []
    for porta in range(porta_inicio, porta_inicio + limite):
        try:
            with socket.create_connection((host, porta), timeout=timeout) as s:
                s.settimeout(timeout)
                enviar_msg(s, {"tipo": "PING"})
                resp = receber_msg(s)
                if resp.get("tipo") == "PONG":
                    ativos.append((host, porta))
        except (OSError, ConnectionError, EOFError):
            pass
    return ativos


def _falar_com_servidor(host, porta, bloco_A, B, paralelo, linha_inicio):
    """Abre conexão, envia o bloco e devolve a resposta do servidor."""
    with socket.create_connection((host, porta)) as s:
        enviar_msg(s, {
            "tipo": "MULTIPLICAR",
            "bloco_A": bloco_A,
            "matriz_B": B,
            "paralelo": paralelo,
            "linha_inicio": linha_inicio,
        })
        return receber_msg(s)


def executar_distribuido(A, B, servidores, paralelo=True,
                         ao_despachar=None, ao_receber=None):
    """Distribui a multiplicação entre os servidores e recompõe C.

    `ao_despachar(idx, host, porta, ini, fim)` e
    `ao_receber(idx, resposta)` são callbacks opcionais para a UI.

    Devolve (C, metricas). Métricas:
      - tempo_total: relógio de parede do despacho até recompor C
      - tempo_max_servidor: maior tempo de cálculo entre os servidores
      - overhead: tempo_total - tempo_max_servidor (rede + serialização)
      - por_servidor: lista com porta, linhas e tempo de cálculo de cada nó
    """
    n = len(servidores)
    fatias = compute.dividir_linhas(len(A), n)

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
            por_servidor.append({
                "porta": porta,
                "linhas": n_linhas,
                "tempo_calculo": resp.get("tempo_calculo", 0.0),
            })
            if ao_receber:
                ao_receber(idx, resp)

    # Recompõe C na ordem das linhas (Composição do resultado final).
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
    return compute.multiplicar_serial(A, B) == C


# --------------------------------------------------------------------------
# UI (rich) — só apresentação; medição de tempo fica no núcleo
# --------------------------------------------------------------------------
def _executar_com_ui(tamanho, paralelo, servidores_cli,
                     host, porta_inicio, limite, verificar_resultado):
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich import box

    console = Console()
    console.rule("[bold cyan]Multiplicação de Matrizes Distribuída[/]")

    cab = Table.grid(padding=(0, 2))
    cab.add_column(justify="right", style="cyan")
    cab.add_column()
    cab.add_row("Dimensão", f"{tamanho} x {tamanho}")
    cab.add_row("Paralelismo no nó", "sim" if paralelo else "não")
    console.print(Panel(cab, title="Parâmetros", border_style="cyan"))

    # Descoberta
    if servidores_cli:
        servidores = servidores_cli
        console.print(f"[green]Servidores informados:[/] {servidores}")
    else:
        with console.status("[cyan]Procurando servidores ativos..."):
            servidores = descobrir_servidores(host, porta_inicio, limite)
    if not servidores:
        console.print("[bold red]Nenhum servidor encontrado. Suba o server.py primeiro.[/]")
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

    from rich.live import Live
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
    console.print(Panel(resumo, title="[bold]Resultado[/]", border_style="green"))

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
    ap = argparse.ArgumentParser(description="Cliente da multiplicação distribuída")
    ap.add_argument("tamanho", nargs="?", type=int, default=100,
                    help="dimensão N das matrizes NxN (padrão 100)")
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
        )
    except KeyboardInterrupt:
        print("\n[!] Cancelado pelo usuário.")
        sys.exit(1)
