# benchmark.py
"""Automação dos testes para o relatório.

Para cada tamanho executa e cronometra:
  - serial            (1 processo)  -> é o GABARITO de corretude
  - paralelo-local    (multiprocessing, sem rede)
  - distribuido-k     (k servidores via socket, k em --servers)

Repete R vezes, confere que paralelo-local e distribuído devolvem o MESMO C
que o serial, agrega média/desvio, calcula speedup e eficiência e exporta:
  - resultados_brutos.csv   (cada repetição)
  - resultados_resumo.csv   (médias + speedup + eficiência)
  - grafico_tempo.png, grafico_speedup.png, grafico_eficiencia.png

Tudo em Python puro (sem numpy): o objetivo é mostrar o ganho de distribuir
um cálculo CPU-bound, então o serial precisa ser realmente serial.

Exemplos:
    python benchmark.py
    python benchmark.py --sizes 128,256,384 --repeats 5 --servers 1,2,4
    python benchmark.py --sizes 256,384,512 --server-workers 2
"""
import argparse
import csv
import os
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

import compute
from client import descobrir_servidores, executar_distribuido, gerar_matriz


def _esperar_servidores(host, porta_base, k, timeout=20.0):
    """Aguarda os k servidores responderem PING antes de medir."""
    fim = time.time() + timeout
    ativos = []
    while time.time() < fim:
        ativos = descobrir_servidores(host, porta_base, k, timeout=0.2)
        if len(ativos) >= k:
            return ativos[:k]
        time.sleep(0.2)
    raise RuntimeError(f"apenas {len(ativos)}/{k} servidores subiram a tempo")


def _subir_servidores(host, porta_base, k, server_workers):
    """Sobe k servidores como subprocessos (portas fixas, silenciosos)."""
    procs = []
    for i in range(k):
        p = subprocess.Popen(
            [sys.executable, "server.py",
             "--host", host, "--port", str(porta_base + i),
             "--workers", str(server_workers), "--quiet"],
            cwd=str(Path(__file__).parent),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        procs.append(p)
    return procs


def _derrubar(procs):
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def _porta_base_livre(inicio=5000):
    """Acha um bloco de portas livre para não colidir com servidor já aberto."""
    porta = inicio
    while porta < inicio + 500:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", porta)) != 0:
                return porta
        porta += 10
    return inicio


def rodar(sizes, repeats, server_counts, server_workers, host, out_dir):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    brutos = []  # (tamanho, modo, n_nos, rep, tempo, t_max_no, overhead, correto)

    from rich.console import Console
    from rich.progress import (Progress, SpinnerColumn, TextColumn,
                               BarColumn, TimeElapsedColumn)
    console = Console()
    console.rule("[bold cyan]Benchmark — Python puro (sem numpy)[/]")

    total = len(sizes) * repeats * (2 + len(server_counts))
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  BarColumn(), TextColumn("{task.completed}/{task.total}"),
                  TimeElapsedColumn(), console=console) as prog:
        tarefa = prog.add_task("medindo...", total=total)

        for tamanho in sizes:
            for rep in range(repeats):
                A = gerar_matriz(tamanho, tamanho, semente=100 + rep)
                B = gerar_matriz(tamanho, tamanho, semente=200 + rep)

                # Serial — é o gabarito de corretude para os demais modos.
                prog.update(tarefa, description=f"N={tamanho} rep={rep+1} serial")
                t0 = time.perf_counter()
                gabarito = compute.multiplicar_serial(A, B)
                ts = time.perf_counter() - t0
                brutos.append((tamanho, "serial", 1, rep, ts, ts, 0.0, True))
                prog.advance(tarefa)

                # Paralelo local (sem rede)
                prog.update(tarefa, description=f"N={tamanho} rep={rep+1} paralelo-local")
                nw = os.cpu_count() or 2
                t0 = time.perf_counter()
                C = compute.multiplicar_paralelo_local(A, B, nw)
                tp = time.perf_counter() - t0
                brutos.append((tamanho, "paralelo-local", nw, rep, tp, tp, 0.0,
                               C == gabarito))
                prog.advance(tarefa)

                # Distribuído com k servidores
                for k in server_counts:
                    prog.update(tarefa,
                                description=f"N={tamanho} rep={rep+1} distribuido-{k}")
                    porta_base = _porta_base_livre()
                    procs = _subir_servidores(host, porta_base, k, server_workers)
                    try:
                        servidores = _esperar_servidores(host, porta_base, k)
                        C, m = executar_distribuido(A, B, servidores,
                                                    paralelo=server_workers > 1)
                    finally:
                        _derrubar(procs)
                    brutos.append((tamanho, "distribuido", k, rep,
                                   m["tempo_total"], m["tempo_max_servidor"],
                                   m["overhead"], C == gabarito))
                    prog.advance(tarefa)

    _exportar(brutos, out, console)
    return brutos


def _agregar(brutos):
    """Agrupa por (modo, n_nos, tamanho) e calcula média/desvio + speedup."""
    chaves = {}
    for n, modo, nos, rep, t, tmax, ov, ok in brutos:
        chaves.setdefault((modo, nos, n), []).append((t, ok))

    serial_medio = {}
    for (modo, nos, n), vals in chaves.items():
        if modo == "serial":
            serial_medio[n] = statistics.mean(t for t, _ in vals)

    linhas = []
    for (modo, nos, n), vals in sorted(chaves.items(),
                                       key=lambda x: (x[0][2], x[0][0], x[0][1])):
        tempos = [t for t, _ in vals]
        media = statistics.mean(tempos)
        desvio = statistics.pstdev(tempos) if len(tempos) > 1 else 0.0
        correto = all(ok for _, ok in vals)
        base = serial_medio.get(n)
        speedup = (base / media) if base and media > 0 else float("nan")
        eficiencia = speedup / nos if nos else float("nan")
        linhas.append({
            "tamanho": n, "modo": modo, "n_nos": nos,
            "tempo_medio": media, "desvio": desvio,
            "speedup": speedup, "eficiencia": eficiencia, "correto": correto,
        })
    return linhas


def _exportar(brutos, out, console):
    bruto_csv = out / "resultados_brutos.csv"
    with open(bruto_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["tamanho", "modo", "n_nos", "repeticao",
                    "tempo_s", "tempo_max_no_s", "overhead_s", "correto"])
        w.writerows(brutos)

    agregado = _agregar(brutos)
    resumo_csv = out / "resultados_resumo.csv"
    with open(resumo_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["tamanho", "modo", "n_nos", "tempo_medio_s",
                    "desvio_s", "speedup", "eficiencia", "correto"])
        for r in agregado:
            w.writerow([r["tamanho"], r["modo"], r["n_nos"],
                        f'{r["tempo_medio"]:.6f}', f'{r["desvio"]:.6f}',
                        f'{r["speedup"]:.3f}', f'{r["eficiencia"]:.3f}',
                        r["correto"]])

    _tabela_resumo(agregado, console)
    _graficos(agregado, out, console)
    console.print(f"[green]CSV salvo:[/] {bruto_csv.name}, {resumo_csv.name}")


def _tabela_resumo(agregado, console):
    from rich.table import Table
    from rich import box
    t = Table(box=box.ROUNDED, title="Resumo dos testes")
    for c in ("N", "Modo", "Nós", "Tempo médio (s)", "Desvio",
              "Speedup", "Eficiência", "OK"):
        t.add_column(c, justify="left" if c == "Modo" else "right")
    for r in agregado:
        t.add_row(str(r["tamanho"]), r["modo"], str(r["n_nos"]),
                  f'{r["tempo_medio"]:.4f}', f'{r["desvio"]:.4f}',
                  f'{r["speedup"]:.2f}x', f'{r["eficiencia"]:.2f}',
                  "[green]sim[/]" if r["correto"] else "[red]NÃO[/]")
    console.print(t)


def _graficos(agregado, out, console):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    tamanhos = sorted({r["tamanho"] for r in agregado})

    def serie(modo, nos=None):
        pts = {}
        for r in agregado:
            if r["modo"] == modo and (nos is None or r["n_nos"] == nos):
                pts[r["tamanho"]] = r
        xs = [n for n in tamanhos if n in pts]
        return ([pts[n]["tempo_medio"] for n in xs],
                [pts[n]["speedup"] for n in xs], xs)

    nos_dist = sorted({r["n_nos"] for r in agregado if r["modo"] == "distribuido"})

    # 1) Tempo x tamanho
    plt.figure(figsize=(8, 5))
    y, _, xs = serie("serial")
    plt.plot(xs, y, "o-", label="serial")
    y, _, xs = serie("paralelo-local")
    plt.plot(xs, y, "s-", label="paralelo-local")
    for k in nos_dist:
        y, _, xs = serie("distribuido", k)
        plt.plot(xs, y, "^-", label=f"distribuído ({k} nós)")
    plt.xlabel("Dimensão N (matriz NxN)")
    plt.ylabel("Tempo médio (s)")
    plt.title("Tempo de execução — serial × paralelo × distribuído")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out / "grafico_tempo.png", dpi=130)
    plt.close()

    # 2) Speedup x tamanho
    plt.figure(figsize=(8, 5))
    _, sp, xs = serie("paralelo-local")
    plt.plot(xs, sp, "s-", label="paralelo-local")
    for k in nos_dist:
        _, sp, xs = serie("distribuido", k)
        plt.plot(xs, sp, "^-", label=f"distribuído ({k} nós)")
    plt.axhline(1.0, color="gray", ls="--", alpha=0.6, label="sem ganho (1x)")
    plt.xlabel("Dimensão N")
    plt.ylabel("Speedup (T_serial / T)")
    plt.title("Speedup em relação ao serial")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out / "grafico_speedup.png", dpi=130)
    plt.close()

    # 3) Eficiência x nº de nós (maior tamanho)
    maior = max(tamanhos)
    dist = sorted([r for r in agregado
                   if r["modo"] == "distribuido" and r["tamanho"] == maior],
                  key=lambda r: r["n_nos"])
    if dist:
        plt.figure(figsize=(8, 5))
        plt.plot([r["n_nos"] for r in dist], [r["eficiencia"] for r in dist],
                 "^-", label=f"N={maior}")
        plt.axhline(1.0, color="gray", ls="--", alpha=0.6, label="ideal (1.0)")
        plt.xlabel("Número de servidores (nós)")
        plt.ylabel("Eficiência (speedup / nº nós)")
        plt.title(f"Eficiência paralela — N={maior}")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(out / "grafico_eficiencia.png", dpi=130)
        plt.close()

    console.print(f"[green]Gráficos salvos em[/] {out}/")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Benchmark serial x paralelo x distribuído (Python puro)")
    ap.add_argument("--sizes", default="64,128,192",
                    help="dimensões NxN separadas por vírgula")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--servers", default="1,2,4",
                    help="quantidades de servidores distribuídos a testar")
    ap.add_argument("--server-workers", type=int, default=1,
                    help="processos internos por servidor (1 = sem paralelismo no nó)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--out", default="resultados")
    args = ap.parse_args()

    sizes = [int(x) for x in args.sizes.split(",")]
    server_counts = [int(x) for x in args.servers.split(",")]

    rodar(sizes, args.repeats, server_counts,
          max(1, args.server_workers), args.host, args.out)
