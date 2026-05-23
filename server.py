# server.py
"""Servidor (nó de processamento) da multiplicação distribuída.

Demonstração ao vivo: rode este arquivo e o servidor sobe e fica escutando
numa porta. Quando o client.py for executado, ele acha este servidor, manda
um bloco de A + a matriz B, o servidor multiplica (Python puro) e devolve o
bloco resultante. Com --workers > 1 o servidor ainda subdivide o bloco entre
os núcleos locais (paralelismo dentro do nó / aglomeração de Foster).

Arquivo autocontido de propósito: o protocolo e o kernel são pequenos e
estão repetidos no client.py para que cada arquivo rode sozinho, sem
módulos auxiliares (projeto acadêmico — clareza > DRY).

Uso:
    python server.py                 # acha porta livre a partir de 5000, com painel
    python server.py --port 5001     # porta fixa
    python server.py --port 5001 -q  # silencioso (usado pelo benchmark)
"""
import argparse
import os
import pickle
import socket
import struct
import sys
import threading
import time
from multiprocessing import Pool


# ==========================================================================
# Protocolo de mensagens (framing por length-prefix de 8 bytes).
# Mesma implementação do client.py — mantida aqui para o arquivo ser
# autocontido. Evita depender de fechar a conexão para delimitar a mensagem.
# ==========================================================================
_CAB = struct.Struct("!Q")  # inteiro de 8 bytes, big-endian


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
# Kernel de multiplicação (Python puro, O(n^3)). Também presente no
# client.py. Ordem de laços i-k-j: igual à i-j-k matematicamente, porém com
# acesso mais sequencial à memória — bem mais rápida em Python puro.
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
    """Divide total_linhas em até n_partes fatias equilibradas (sobra
    espalhada uma linha por parte, sem gargalo na última)."""
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


# ==========================================================================
# Servidor
# ==========================================================================
def _achar_porta(server_socket, host, porta_inicial):
    porta = porta_inicial
    while True:
        try:
            server_socket.bind((host, porta))
            return porta
        except OSError:
            porta += 1
            if porta > porta_inicial + 200:
                raise RuntimeError("nenhuma porta livre encontrada")


def _processar(req, obter_pool, n_workers):
    """Multiplica o bloco recebido e devolve (bloco_C, tempo_calculo)."""
    bloco_A = req["bloco_A"]
    B = req["matriz_B"]
    paralelo = req.get("paralelo", True)

    inicio = time.perf_counter()
    if paralelo and n_workers > 1 and len(bloco_A) >= n_workers:
        # Paralelismo DENTRO do nó: subdivide o bloco entre os núcleos.
        pool = obter_pool()
        fatias = dividir_linhas(len(bloco_A), n_workers)
        tarefas = [(bloco_A[ini:fim], B) for ini, fim in fatias]
        partes = pool.map(_worker_bloco, tarefas)
        bloco_C = []
        for parte in partes:
            bloco_C.extend(parte)
    else:
        bloco_C = multiplicar(bloco_A, B)
    return bloco_C, time.perf_counter() - inicio


def iniciar_servidor(host, porta_inicial, porta_fixa, quiet, n_workers):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if porta_fixa is not None:
        server_socket.bind((host, porta_fixa))
        porta = porta_fixa
    else:
        porta = _achar_porta(server_socket, host, porta_inicial)
    server_socket.listen(8)

    # Pool criado sob demanda (lazy) e protegido por lock: o servidor já
    # aceita conexões e responde PING logo após listen(), sem esperar o
    # spawn dos processos.
    pool = None
    pool_lock = threading.Lock()
    estado_lock = threading.Lock()

    def obter_pool():
        nonlocal pool
        with pool_lock:
            if pool is None:
                pool = Pool(processes=n_workers)
            return pool

    estado = {
        "porta": porta, "host": host, "workers": n_workers,
        "atendidos": 0, "ultimo_bloco": "-", "ultimo_tempo": "-",
        "status": "aguardando conexão...",
    }

    ui = None
    if not quiet:
        ui = _UI(estado)
        ui.start()
    else:
        print(f"[server] pronto host={host} porta={porta} workers={n_workers}",
              flush=True)

    def _set(**kv):
        with estado_lock:
            estado.update(kv)
        if ui:
            ui.refresh()

    def _atender(conn):
        """Atende UMA conexão. Roda em thread própria: uma conexão lenta ou
        ociosa nunca trava o servidor (PING continua respondendo na hora,
        mesmo durante um cálculo grande)."""
        try:
            with conn:
                conn.settimeout(300)  # rede local; evita travar para sempre
                req = receber_msg(conn)
                tipo = req.get("tipo")

                if tipo == "PING":
                    enviar_msg(conn, {"tipo": "PONG", "porta": porta,
                                      "cpus": os.cpu_count(),
                                      "workers": n_workers})
                    return
                if tipo != "MULTIPLICAR_V2":
                    enviar_msg(conn, {"tipo": "ERRO",
                                      "msg": f"tipo desconhecido: {tipo!r}"})
                    return

                # MULTIPLICAR_V2 chega em DOIS frames: o primeiro (já lido)
                # traz bloco_A e metadados; o segundo traz a matriz B (que o
                # cliente pickla uma única vez e reusa entre todos os nós).
                req["matriz_B"] = receber_msg(conn)
                _set(status="calculando...")
                bloco_C, t_calc = _processar(req, obter_pool, n_workers)
                enviar_msg(conn, {
                    "tipo": "RESULTADO", "bloco_C": bloco_C,
                    "linha_inicio": req.get("linha_inicio", 0),
                    "tempo_calculo": t_calc, "porta": porta,
                })
                with estado_lock:
                    estado["atendidos"] += 1
                _set(ultimo_bloco=f'{len(req["bloco_A"])} linhas',
                     ultimo_tempo=f"{t_calc:.4f}s",
                     status="aguardando conexão...")
        except (ConnectionError, EOFError, OSError) as e:
            _set(status=f"conexão perdida ({e})")

    try:
        while True:
            conn, _ = server_socket.accept()
            threading.Thread(target=_atender, args=(conn,),
                             daemon=True).start()
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
    renderizar durante o cálculo, para não contaminar a medição."""

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
