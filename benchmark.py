# benchmark.py
"""Automação dos testes para o relatório.

Para cada tamanho cronometra:
  - serial            (1 processo)        -> GABARITO de corretude
  - paralelo-local    (multiprocessing, sem rede)
  - distribuido-k     (k servidores via socket, k em --servers)

Robustez estatística (corrige a eficiência > 100% / pico espúrio):
  - --warmup repetições de aquecimento são executadas e DESCARTADAS (tira o
    custo da primeira execução "fria": alocação de memória, SO, cache);
  - a métrica central é a MEDIANA (não a média) — imune a um serial lento
    isolado, que antes inflava o speedup de todos os modos naquele N.

Métricas (também explicadas no rodapé da tabela):
  speedup    = tempo_serial_mediano / tempo_mediano_do_modo
  eficiência = speedup / nº_de_unidades   (nº de nós, ou nº de processos
               no paralelo-local). Eficiência > 1 (superlinear) pode ser
               REAL: blocos menores cabem melhor na cache da CPU.

Saídas em --out: resultados_brutos.csv, resultados_resumo.csv,
grafico_tempo.png, grafico_speedup.png, grafico_eficiencia.png

Tudo Python puro (sem numpy): o objetivo é mostrar o ganho de DISTRIBUIR
um cálculo CPU-bound, então o serial precisa ser realmente serial.

Exemplos:
    python benchmark.py
    python benchmark.py --sizes 256,384,512 --repeats 5 --servers 1,2,4,8,16
    python benchmark.py --sizes 256,512,768 --servers 1,2,4 --local-workers 4
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

from client import (descobrir_servidores, enviar_msg, executar_distribuido,
                     gerar_matriz, multiplicar_paralelo_local,
                     multiplicar_serial, receber_msg)


def _ping(host, porta, timeout=0.6):
    """PING direcionado a um servidor específico (host:porta arbitrário)."""
    try:
        with socket.create_connection((host, porta), timeout=timeout) as s:
            s.settimeout(timeout)
            enviar_msg(s, {"tipo": "PING"})
            return receber_msg(s).get("tipo") == "PONG"
    except (OSError, ConnectionError, EOFError):
        return False


def _esperar_pool(pool, timeout=25.0):
    """Espera TODOS os servidores externos (locais + remotos) responderem."""
    fim = time.time() + timeout
    while time.time() < fim:
        if all(_ping(h, p) for h, p in pool):
            return
        time.sleep(0.5)
    faltando = [f"{h}:{p}" for h, p in pool if not _ping(h, p)]
    raise RuntimeError(
        "servidores externos inacessíveis: " + ", ".join(faltando) +
        "\n  Verifique: server.py rodando lá com --host 0.0.0.0 --quiet, "
        "IP/porta corretos e firewall liberado na outra máquina.")


def _esperar_servidores(host, porta_base, k, timeout=20.0):
    fim = time.time() + timeout
    ativos = []
    while time.time() < fim:
        ativos = descobrir_servidores(host, porta_base, k, timeout=0.2)
        if len(ativos) >= k:
            return ativos[:k]
        time.sleep(0.2)
    raise RuntimeError(f"apenas {len(ativos)}/{k} servidores subiram a tempo")


def _subir_servidores(host, porta_base, k, server_workers):
    procs = []
    for i in range(k):
        p = subprocess.Popen(
            [sys.executable, "server.py", "--host", host,
             "--port", str(porta_base + i),
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


def _bloco_portas_livre(k, inicio=5000):
    """Acha um bloco de k portas TCP consecutivas livres.

    Necessário para sweeps grandes (ex.: 32 nós): subir 32 servidores em
    portas fixas exige garantir as 32 portas, não só a primeira.
    """
    base = inicio
    while base < inicio + 4000:
        if all(_porta_livre(base + off) for off in range(k)):
            return base
        base += k
    raise RuntimeError(f"não achei {k} portas livres consecutivas")


def _porta_livre(porta):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", porta)) != 0


def _uma_passada(tamanho, rep, server_counts, server_workers, local_workers,
                 host, gravar, brutos, external_pool=None):
    """Roda serial + paralelo-local + distribuído-k uma vez.
    Só grava em `brutos` se gravar=True (passadas de warmup são descartadas).

    external_pool != None: usa os primeiros k servidores JÁ RODANDO do pool
    (locais + remotos), sem subir/derrubar nada.
    """
    A = gerar_matriz(tamanho, tamanho, semente=1000 + rep)
    B = gerar_matriz(tamanho, tamanho, semente=2000 + rep)

    t0 = time.perf_counter()
    gabarito = multiplicar_serial(A, B)
    ts = time.perf_counter() - t0
    if gravar:
        brutos.append((tamanho, "serial", 1, rep, ts, ts, 0.0, True))

    t0 = time.perf_counter()
    C = multiplicar_paralelo_local(A, B, local_workers)
    tp = time.perf_counter() - t0
    if gravar:
        brutos.append((tamanho, "paralelo-local", local_workers, rep, tp, tp,
                       0.0, C == gabarito))

    for k in server_counts:
        if external_pool is not None:
            servidores = external_pool[:k]
            C, m = executar_distribuido(A, B, servidores, paralelo=True)
        else:
            porta_base = _bloco_portas_livre(k)
            procs = _subir_servidores(host, porta_base, k, server_workers)
            try:
                servidores = _esperar_servidores(host, porta_base, k)
                C, m = executar_distribuido(A, B, servidores,
                                            paralelo=server_workers > 1)
            finally:
                _derrubar(procs)
        if gravar:
            brutos.append((tamanho, "distribuido", k, rep, m["tempo_total"],
                           m["tempo_max_servidor"], m["overhead"],
                           C == gabarito))


def rodar(sizes, repeats, warmup, server_counts, server_workers,
          local_workers, host, out_dir, external_pool=None):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    brutos = []  # (tamanho, modo, n_nos, rep, tempo, t_max_no, overhead, ok)

    from rich.console import Console
    from rich.progress import (Progress, SpinnerColumn, TextColumn,
                               BarColumn, TimeElapsedColumn)
    console = Console()
    console.rule("[bold cyan]Benchmark — Python puro (sem numpy)[/]")

    if external_pool is not None:
        console.print(f"[cyan]Pool externo ({len(external_pool)} servidores), "
                      "aguardando todos responderem...[/]")
        _esperar_pool(external_pool)
        validos = [k for k in server_counts if k <= len(external_pool)]
        descartados = [k for k in server_counts if k > len(external_pool)]
        if descartados:
            console.print(f"[yellow]Ignorando {descartados}: pool só tem "
                          f"{len(external_pool)} servidores.[/]")
        server_counts = validos
        console.print("[green]Pool ok:[/] "
                      + ", ".join(f"{h}:{p}" for h, p in external_pool))

    console.print(f"[dim]warmup={warmup} (descartado) · repeats={repeats} "
                  f"(mediana) · servidores={server_counts} · "
                  f"paralelo-local={local_workers} proc.[/]")

    por_passada = 2 + len(server_counts)
    total = len(sizes) * (warmup + repeats) * por_passada
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  BarColumn(), TextColumn("{task.completed}/{task.total}"),
                  TimeElapsedColumn(), console=console) as prog:
        tarefa = prog.add_task("medindo...", total=total)
        for tamanho in sizes:
            for passo in range(warmup + repeats):
                gravar = passo >= warmup
                rotulo = "warmup" if not gravar else f"rep {passo - warmup + 1}"
                prog.update(tarefa, description=f"N={tamanho} {rotulo}")
                _uma_passada(tamanho, passo, server_counts, server_workers,
                             local_workers, host, gravar, brutos,
                             external_pool)
                prog.advance(tarefa, por_passada)

    _exportar(brutos, out, console)
    return brutos


def _stats(xs):
    """min, máx, média, mediana, desvio padrão (populacional) de uma lista."""
    if not xs:
        nan = float("nan")
        return dict(min=nan, max=nan, media=nan, mediana=nan, desvio=nan)
    return dict(
        min=min(xs), max=max(xs),
        media=statistics.mean(xs), mediana=statistics.median(xs),
        desvio=statistics.pstdev(xs) if len(xs) > 1 else 0.0,
    )


def _agregar(brutos):
    """Agrupa por (modo, n_nos, tamanho) e calcula estatísticas completas.

    Speedup é pareado por repetição: speedup_i = T_serial[N, rep_i] /
    T_modo[N, rep_i] (serial e modo medem o MESMO A,B na mesma repetição).
    A métrica central continua sendo a MEDIANA.
    """
    chaves = {}
    serial_por_rep = {}
    for n, modo, nos, rep, t, tmax, ov, ok in brutos:
        chaves.setdefault((modo, nos, n), []).append((rep, t, ok))
        if modo == "serial":
            serial_por_rep[(n, rep)] = t

    linhas = []
    for (modo, nos, n), vals in sorted(chaves.items(),
                                       key=lambda x: (x[0][2], x[0][0], x[0][1])):
        tempos = [t for _, t, _ in vals]
        speedups = [serial_por_rep[(n, rep)] / t
                    for rep, t, _ in vals
                    if (n, rep) in serial_por_rep and t > 0]
        correto = all(ok for _, _, ok in vals)
        st = _stats(tempos)
        sp = _stats(speedups)
        linhas.append({
            "tamanho": n, "modo": modo, "n_nos": nos, "correto": correto,
            "tempo": st, "speedup": sp,
            # chaves planas usadas pelos gráficos de linha (mantêm a mediana)
            "mediana": st["mediana"],
            "speedup_mediano": sp["mediana"],
            "eficiencia": sp["mediana"] / nos if nos else float("nan"),
        })
    return linhas


def _exportar(brutos, out, console):
    bruto_csv = out / "resultados_brutos.csv"
    with open(bruto_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["tamanho", "modo", "n_nos", "repeticao", "tempo_s",
                    "tempo_max_no_s", "overhead_s", "correto"])
        w.writerows(brutos)

    agregado = _agregar(brutos)
    resumo_csv = out / "resultados_resumo.csv"
    with open(resumo_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "tamanho", "modo", "n_nos",
            "tempo_min_s", "tempo_max_s", "tempo_medio_s",
            "tempo_mediano_s", "tempo_desvio_s",
            "speedup_min", "speedup_max", "speedup_medio",
            "speedup_mediano", "speedup_desvio",
            "eficiencia_mediana", "correto",
        ])
        for r in agregado:
            te, sp = r["tempo"], r["speedup"]
            w.writerow([
                r["tamanho"], r["modo"], r["n_nos"],
                f'{te["min"]:.6f}', f'{te["max"]:.6f}', f'{te["media"]:.6f}',
                f'{te["mediana"]:.6f}', f'{te["desvio"]:.6f}',
                f'{sp["min"]:.3f}', f'{sp["max"]:.3f}', f'{sp["media"]:.3f}',
                f'{sp["mediana"]:.3f}', f'{sp["desvio"]:.3f}',
                f'{r["eficiencia"]:.3f}', r["correto"],
            ])

    _tabela_resumo(agregado, console)
    _graficos(agregado, out, console)
    _boxplots(brutos, out, console)
    console.print(f"[green]CSV salvo:[/] {bruto_csv.name}, {resumo_csv.name}")
    console.print("[dim]speedup_i = T_serial[N,rep_i] / T_modo[N,rep_i] "
                  "(pareado por repetição) · centrais = mediana · "
                  "eficiência = speedup_mediano / nº unidades · "
                  ">1 = superlinear (cache).[/]")


def _tabela_resumo(agregado, console):
    from rich.table import Table
    from rich import box
    t = Table(box=box.ROUNDED,
              title="Resumo dos testes (tempo: mediana; faixa mín–máx)")
    for c in ("N", "Modo", "Unid.", "Tempo med. (s)", "Tempo mín–máx (s)",
              "Desvio (s)", "Speedup", "Efic.", "OK"):
        t.add_column(c, justify="left" if c == "Modo" else "right")
    for r in agregado:
        te, sp = r["tempo"], r["speedup"]
        t.add_row(
            str(r["tamanho"]), r["modo"], str(r["n_nos"]),
            f'{te["mediana"]:.4f}',
            f'{te["min"]:.4f}–{te["max"]:.4f}',
            f'{te["desvio"]:.4f}',
            f'{sp["mediana"]:.2f}x',
            f'{r["eficiencia"]:.2f}',
            "[green]sim[/]" if r["correto"] else "[red]NÃO[/]")
    console.print(t)


def _boxplots(brutos, out, console):
    """Um box plot por tamanho: distribuição do tempo de cada modo/config
    entre as repetições. Mostra de cara a variância e os outliers — exato
    para discutir estabilidade da medição no relatório."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    # tamanho -> { (ordem, rotulo) : [tempos...] }
    por_tamanho = {}
    for n, modo, nos, rep, t, tmax, ov, ok in brutos:
        if modo == "serial":
            chave = (0, 0, "serial")
        elif modo == "paralelo-local":
            chave = (1, nos, f"par-local\n({nos})")
        else:
            chave = (2, nos, f"dist\n({nos})")
        por_tamanho.setdefault(n, {}).setdefault(chave, []).append(t)

    gerados = 0
    for n in sorted(por_tamanho):
        configs = sorted(por_tamanho[n])
        dados = [por_tamanho[n][c] for c in configs]
        rotulos = [c[2] for c in configs]
        reps = max((len(d) for d in dados), default=0)

        plt.figure(figsize=(max(8, len(configs) * 1.1), 5))
        plt.boxplot(dados, tick_labels=rotulos, showmeans=True)
        plt.ylabel("Tempo (s)")
        plt.title(f"Distribuição do tempo — N={n} ({reps} repetições)")
        plt.grid(True, axis="y", alpha=0.3)
        plt.tight_layout()
        plt.savefig(out / f"grafico_boxplot_{n}.png", dpi=130)
        plt.close()
        gerados += 1

    console.print(f"[green]Box plots salvos:[/] {gerados} "
                  f"(grafico_boxplot_<N>.png)")


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
        return ([pts[n]["mediana"] for n in xs],
                [pts[n]["speedup_mediano"] for n in xs], xs)

    nos_dist = sorted({r["n_nos"] for r in agregado if r["modo"] == "distribuido"})

    plt.figure(figsize=(8, 5))
    y, _, xs = serie("serial")
    plt.plot(xs, y, "o-", label="serial")
    y, _, xs = serie("paralelo-local")
    plt.plot(xs, y, "s-", label="paralelo-local")
    for k in nos_dist:
        y, _, xs = serie("distribuido", k)
        plt.plot(xs, y, "^-", label=f"distribuído ({k} nós)")
    plt.xlabel("Dimensão N (matriz NxN)")
    plt.ylabel("Tempo mediano (s)")
    plt.title("Tempo de execução — serial × paralelo × distribuído")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out / "grafico_tempo.png", dpi=130)
    plt.close()

    plt.figure(figsize=(8, 5))
    _, sp, xs = serie("paralelo-local")
    plt.plot(xs, sp, "s-", label="paralelo-local")
    for k in nos_dist:
        _, sp, xs = serie("distribuido", k)
        plt.plot(xs, sp, "^-", label=f"distribuído ({k} nós)")
    plt.axhline(1.0, color="gray", ls="--", alpha=0.6, label="sem ganho (1x)")
    plt.xlabel("Dimensão N")
    plt.ylabel("Speedup (T_serial / T)")
    plt.title("Speedup em relação ao serial (medianas)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out / "grafico_speedup.png", dpi=130)
    plt.close()

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


def _parse_pool(texto):
    """ "host:porta" e faixas "host:ini-fim" -> lista ordenada de (host, porta).
    Ex: "127.0.0.1:5000-5015,192.168.0.50:5000-5011" -> 28 servidores.
    """
    pool = []
    for item in texto.split(","):
        item = item.strip()
        if not item:
            continue
        host, faixa = item.rsplit(":", 1)
        if "-" in faixa:
            ini, fim = faixa.split("-")
            for porta in range(int(ini), int(fim) + 1):
                pool.append((host, porta))
        else:
            pool.append((host, int(faixa)))
    return pool


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Benchmark serial x paralelo x distribuído (Python puro)")
    ap.add_argument("--sizes", default="128,256,384,512",
                    help="dimensões NxN separadas por vírgula")
    ap.add_argument("--repeats", type=int, default=5,
                    help="repetições medidas (mediana entre elas)")
    ap.add_argument("--warmup", type=int, default=1,
                    help="repetições de aquecimento descartadas (0 desliga)")
    ap.add_argument("--servers", default="1,2,4",
                    help="quantidades de servidores; ex: 1,2,4,8,16")
    ap.add_argument("--server-workers", type=int, default=1,
                    help="processos internos por servidor (1 = sem paralelismo no nó)")
    ap.add_argument("--local-workers", type=int, default=os.cpu_count() or 2,
                    help="processos do paralelo-local (use p/ comparar de "
                         "forma justa, ex: --local-workers 4 vs --servers 4)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--external", default=None,
                    help='usa servidores JÁ rodando (locais+remotos), sem '
                         'subir/derrubar. Ex: "127.0.0.1:5000-5015,'
                         '192.168.0.50:5000-5011" (16 locais + 12 remotos)')
    ap.add_argument("--out", default="resultados")
    args = ap.parse_args()

    sizes = [int(x) for x in args.sizes.split(",")]
    server_counts = [int(x) for x in args.servers.split(",")]
    external_pool = _parse_pool(args.external) if args.external else None

    rodar(sizes, args.repeats, max(0, args.warmup), server_counts,
          max(1, args.server_workers), max(1, args.local_workers),
          args.host, args.out, external_pool)
