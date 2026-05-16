# compute.py
"""Núcleo de cálculo compartilhado por cliente, servidor e benchmark.

Tudo em Python puro, sem numpy — de propósito. O objetivo do trabalho é
mostrar o ganho de DISTRIBUIR um cálculo CPU-bound; com numpy o cálculo
viraria quase instantâneo (BLAS) e o paralelismo deixaria de aparecer.
Esta é a mesma linha da AV2 (laço manual, granularidade de Foster), agora
estendida do bloco local para o bloco distribuído entre nós.

Um único kernel (laço triplo O(n^3)). O resultado SERIAL é o gabarito de
corretude: paralelo-local e distribuído têm que devolver exatamente o
mesmo C.
"""
import os
from multiprocessing import Pool


# --------------------------------------------------------------------------
# Kernel (laço triplo em Python puro)
# --------------------------------------------------------------------------
def multiplicar(bloco_A, B):
    """Multiplica bloco_A (lista de linhas) por B.

    Ordem de laços i-k-j: matematicamente idêntica à i-j-k, porém com acesso
    mais sequencial à memória — bem mais rápida em Python puro sem deixar de
    ser O(n^3).
    """
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


# --------------------------------------------------------------------------
# Particionamento (etapa de Aglomeração/Mapeamento da metodologia de Foster)
# --------------------------------------------------------------------------
def dividir_linhas(total_linhas, n_partes):
    """Divide `total_linhas` em até `n_partes` fatias o mais equilibradas
    possível. Devolve lista de (inicio, fim); fatias vazias são descartadas.

    Espalha o resto uma linha por parte em vez de jogar tudo na última,
    evitando que um nó vire gargalo quando a divisão não é exata.
    """
    base, resto = divmod(total_linhas, n_partes)
    fatias = []
    inicio = 0
    for i in range(n_partes):
        tamanho = base + (1 if i < resto else 0)
        if tamanho > 0:
            fatias.append((inicio, inicio + tamanho))
            inicio += tamanho
    return fatias


# --------------------------------------------------------------------------
# Modos de execução (baselines para o relatório)
# --------------------------------------------------------------------------
def multiplicar_serial(A, B):
    """Baseline serial: tudo em um único processo. É o gabarito de corretude."""
    return multiplicar(A, B)


def _worker_bloco(args):
    bloco_A, B = args
    return multiplicar(bloco_A, B)


def multiplicar_paralelo_local(A, B, n_workers=None):
    """Baseline paralelo NÃO distribuído: vários processos na mesma máquina
    (multiprocessing), sem rede. Isola o ganho de paralelismo do custo de
    comunicação.

    multiprocessing (e não threading) porque o kernel é CPU-bound em Python
    puro: o GIL impediria ganho real com threads.
    """
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
