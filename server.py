# server.py
"""Servidor (nó de processamento) da multiplicação distribuída.

Recebe um bloco de linhas de A e a matriz B, multiplica (Python puro) e
devolve o bloco resultante. Quando o cliente pede execução paralela, o
servidor ainda subdivide o bloco entre os núcleos locais via multiprocessing
(paralelismo dentro do nó, como pedem os slides).

Uso:
    python server.py                 # acha porta livre a partir de 5000, com painel
    python server.py --port 5001     # porta fixa
    python server.py --port 5001 -q  # silencioso (usado pelo benchmark)
"""
import argparse
import os
import socket
import sys
import time
from multiprocessing import Pool

import compute
from protocol import enviar_msg, receber_msg


def _achar_porta(server_socket, host, porta_inicial):
    """Faz bind na primeira porta livre a partir de porta_inicial."""
    porta = porta_inicial
    while True:
        try:
            server_socket.bind((host, porta))
            return porta
        except OSError:
            porta += 1
            if porta > porta_inicial + 200:
                raise RuntimeError("nenhuma porta livre encontrada")


def _processar(req, pool, n_workers):
    """Executa a multiplicação do bloco e devolve (bloco_C, tempo_calculo)."""
    bloco_A = req["bloco_A"]
    B = req["matriz_B"]
    paralelo = req.get("paralelo", True)

    inicio = time.perf_counter()
    # Paralelismo DENTRO do nó: subdivide o bloco entre os núcleos locais
    # (aglomeração de Foster aplicada também no servidor).
    if paralelo and pool is not None and len(bloco_A) >= n_workers > 1:
        fatias = compute.dividir_linhas(len(bloco_A), n_workers)
        tarefas = [(bloco_A[ini:fim], B) for ini, fim in fatias]
        partes = pool.map(compute._worker_bloco, tarefas)
        bloco_C = []
        for parte in partes:
            bloco_C.extend(parte)
    else:
        bloco_C = compute.multiplicar(bloco_A, B)
    return bloco_C, time.perf_counter() - inicio


def iniciar_servidor(host, porta_inicial, porta_fixa, quiet, n_workers):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if porta_fixa is not None:
        server_socket.bind((host, porta_fixa))
        porta = porta_fixa
    else:
        porta = _achar_porta(server_socket, host, porta_inicial)
    server_socket.listen(8)

    pool = Pool(processes=n_workers) if n_workers > 1 else None

    estado = {
        "porta": porta,
        "host": host,
        "workers": n_workers,
        "atendidos": 0,
        "ultimo_bloco": "-",
        "ultimo_tempo": "-",
        "status": "aguardando conexão...",
    }

    ui = None
    if not quiet:
        ui = _UI(estado)
        ui.start()
    else:
        # Linha única para quem captura stdout saber que subiu.
        print(f"[server] pronto host={host} porta={porta} workers={n_workers}", flush=True)

    try:
        while True:
            conn, _ = server_socket.accept()
            try:
                with conn:
                    req = receber_msg(conn)
                    tipo = req.get("tipo")

                    if tipo == "PING":
                        enviar_msg(conn, {"tipo": "PONG", "porta": porta,
                                          "cpus": os.cpu_count(), "workers": n_workers})
                        continue

                    if tipo != "MULTIPLICAR":
                        enviar_msg(conn, {"tipo": "ERRO",
                                          "msg": f"tipo desconhecido: {tipo!r}"})
                        continue

                    estado["status"] = "calculando..."
                    if ui:
                        ui.refresh()
                    bloco_C, t_calc = _processar(req, pool, n_workers)
                    enviar_msg(conn, {
                        "tipo": "RESULTADO",
                        "bloco_C": bloco_C,
                        "linha_inicio": req.get("linha_inicio", 0),
                        "tempo_calculo": t_calc,
                        "porta": porta,
                    })
                    estado["atendidos"] += 1
                    estado["ultimo_bloco"] = f'{len(req["bloco_A"])} linhas'
                    estado["ultimo_tempo"] = f"{t_calc:.4f}s"
                    estado["status"] = "aguardando conexão..."
                    if ui:
                        ui.refresh()
            except (ConnectionError, EOFError) as e:
                estado["status"] = f"conexão perdida ({e})"
                if ui:
                    ui.refresh()
    except KeyboardInterrupt:
        if not quiet:
            print("\n[!] Desligamento manual (Ctrl+C). Encerrando servidor...")
    finally:
        if ui:
            ui.stop()
        if pool is not None:
            pool.terminate()
        server_socket.close()


class _UI:
    """Painel ao vivo do servidor (rich). Atualiza só em eventos — nada de
    renderizar dentro do cálculo, para não contaminar a medição de tempo."""

    def __init__(self, estado):
        from rich.live import Live
        self.estado = estado
        self._Live = Live
        self._live = None

    def _render(self):
        from rich.panel import Panel
        from rich.table import Table
        e = self.estado
        t = Table.grid(padding=(0, 2))
        t.add_column(justify="right", style="cyan")
        t.add_column()
        t.add_row("Endereço", f'{e["host"]}:{e["porta"]}')
        t.add_row("Workers (núcleos)", str(e["workers"]))
        t.add_row("Blocos atendidos", str(e["atendidos"]))
        t.add_row("Último bloco", str(e["ultimo_bloco"]))
        t.add_row("Último tempo cálculo", str(e["ultimo_tempo"]))
        t.add_row("Status", f'[bold yellow]{e["status"]}[/]')
        return Panel(t, title="[bold green]Servidor de Multiplicação Distribuída[/]",
                     border_style="green")

    def start(self):
        self._live = self._Live(self._render(), refresh_per_second=8, screen=False)
        self._live.start()

    def refresh(self):
        if self._live:
            self._live.update(self._render())

    def stop(self):
        if self._live:
            self._live.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Servidor de multiplicação distribuída")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=None,
                    help="porta fixa; se omitida, busca livre a partir de 5000")
    ap.add_argument("--port-inicial", type=int, default=5000)
    ap.add_argument("--workers", type=int, default=os.cpu_count() or 2,
                    help="processos para paralelismo interno do nó")
    ap.add_argument("-q", "--quiet", action="store_true",
                    help="sem painel; só uma linha de pronto (uso em automação)")
    args = ap.parse_args()
    try:
        iniciar_servidor(args.host, args.port_inicial, args.port,
                          args.quiet, max(1, args.workers))
    except KeyboardInterrupt:
        sys.exit(0)
