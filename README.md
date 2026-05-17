# Multiplicação de Matrizes Distribuída

Sistema cliente–servidor em **Python puro** que distribui a multiplicação de
matrizes entre vários "nós" (processos/máquinas), comunicando-se por
**sockets TCP**, com paralelismo por **multiprocessing**. Inclui um
*benchmark* automatizado que compara **serial × paralelo-local × distribuído**
e gera tabelas estatísticas e gráficos para o relatório.

> Sem `numpy` **de propósito**: o objetivo é evidenciar o ganho de distribuir
> um cálculo CPU-bound. Com BLAS/numpy o cálculo seria quase instantâneo e o
> efeito do paralelismo desapareceria. O resultado **serial** é o gabarito de
> corretude (paralelo-local e distribuído precisam devolver exatamente o
> mesmo `C`).

---

## Sumário

- [Arquitetura](#arquitetura)
- [Requisitos e instalação](#requisitos-e-instalação)
- [1. Teste local (server.py + client.py)](#1-teste-local-serverpy--clientpy)
- [2. Modo demonstração ao vivo](#2-modo-demonstração-ao-vivo-apresentação)
- [3. Teste rápido em rede](#3-teste-rápido-em-rede-duas-máquinas)
- [4. Benchmark local](#4-benchmark-local-automático)
- [5. Benchmark em rede](#5-benchmark-em-rede-2-máquinas)
- [Saídas geradas](#saídas-geradas)
- [Métricas](#métricas)
- [Referência de parâmetros](#referência-de-parâmetros)
- [Solução de problemas](#solução-de-problemas)
- [Notas didáticas](#notas-didáticas)

---

## Arquitetura

São **apenas 3 arquivos**, cada um autocontido (o protocolo de rede e o
kernel de multiplicação são repetidos de propósito — clareza didática > DRY):

| Arquivo | Papel |
|---|---|
| `server.py` | Nó de processamento. Escuta numa porta, recebe um bloco de linhas de `A` + a matriz `B`, multiplica (subdividindo entre os núcleos locais com multiprocessing) e devolve o bloco resultante. Atende cada conexão em uma *thread* (responde `PING` na hora mesmo calculando). |
| `client.py` | Gera `A` e `B`, descobre os servidores, particiona `A` por linhas, despacha os blocos em paralelo, recompõe `C` e confere a corretude contra o serial. Também é o **modo demonstração ao vivo**. |
| `benchmark.py` | Automação dos testes: mede serial, paralelo-local e distribuído-k, repete, calcula estatísticas e gera CSVs + gráficos. Importa o núcleo do `client.py`. |

**Fluxo:** o cliente divide `A` em faixas de linhas equilibradas → envia
`(bloco_A, B)` para cada servidor → cada servidor calcula `bloco_A × B` →
o cliente recebe os blocos e concatena em `C`. Particionamento por linhas =
etapa de *Aglomeração/Mapeamento* da metodologia de Foster (PCAM).

```
.
├── server.py          # nó de processamento (sockets + multiprocessing + threads)
├── client.py          # cliente / demonstração ao vivo
├── benchmark.py       # testes automatizados + CSVs + gráficos
├── requirements.txt   # rich, matplotlib
├── .gitignore
└── resultados/        # saída do benchmark (gitignored; regenerável)
```

---

## Requisitos e instalação

- **Python 3.10+**
- Dependências: `rich` (interface no terminal) e `matplotlib` (gráficos).

```powershell
# na pasta do projeto
python -m venv venv
venv\Scripts\pip install -r requirements.txt
```

Linux/macOS: use `python3` e `venv/bin/python` no lugar de `venv\Scripts\python.exe`.

> **Máquina remota (modo em rede):** ela só precisa do arquivo `server.py` e
> de **Python 3** — rodando com `--quiet` o servidor usa apenas a biblioteca
> padrão (não precisa de venv nem `pip install`).

---

## 1. Teste local (server.py + client.py)

Demonstra o sistema funcionando em uma máquina só. Abra **terminais
separados**:

**Terminal 1 — sobe um servidor:**
```powershell
venv\Scripts\python.exe server.py --port 5000
```

**Terminal 2 — sobe um segundo servidor (mais nós = mais paralelismo):**
```powershell
venv\Scripts\python.exe server.py --port 5001
```

**Terminal 3 — roda o cliente (matriz 256×256):**
```powershell
venv\Scripts\python.exe client.py 256
```

O cliente descobre sozinho os servidores (varre as portas 5000–5009),
distribui o cálculo, mostra uma tabela ao vivo por nó e confere a corretude
contra o resultado serial.

Sem servidores fixos, dá pra subir um só sem `--port` (ele acha uma porta
livre a partir de 5000 automaticamente).

---

## 2. Modo demonstração ao vivo (apresentação)

Com os servidores rodando (passo 1), use `--comparar`: além do distribuído,
o cliente roda **serial** e **paralelo-local** na hora e mostra um quadro
comparativo com o *speedup* — ideal para a defesa, a plateia vê o ganho ao
vivo.

```powershell
venv\Scripts\python.exe client.py 400 --comparar
```

Saída inclui um quadro tipo:

```
serial                  3.95 s   1.00x
paralelo-local (16)     1.97 s   2.01x
distribuído (2 nós)     0.98 s   4.04x
```

> Use N ≥ 300 para o ganho aparecer com clareza (em matrizes pequenas o custo
> de criar processos/conexões domina o tempo).

---

## 3. Teste rápido em rede (duas máquinas)

Para apontar o cliente diretamente a servidores em **outra máquina** (prova
real de que distribuir cruza o limite de uma máquina):

**Na outra máquina** (descubra o IP com `ipconfig` → "Endereço IPv4", ex.
`192.168.0.50`) — libere o firewall **como administrador** e suba o servidor:
```powershell
New-NetFirewallRule -DisplayName "CPC AV3" -Direction Inbound -Protocol TCP -LocalPort 5000-5011 -Action Allow
python server.py --host 0.0.0.0 --port 5000 --quiet
```
> `--host 0.0.0.0` é obrigatório para aceitar conexões da rede (o padrão
> `127.0.0.1` só aceita conexões locais).

**Na sua máquina** — aponte o cliente para o IP dela (pula a descoberta):
```powershell
venv\Scripts\python.exe client.py 256 --comparar --servers 192.168.0.50:5000
```

Vários servidores remotos: `--servers 192.168.0.50:5000,192.168.0.50:5001`.

---

## 4. Benchmark local automático

O `benchmark.py` **sobe e derruba os servidores sozinho** — não precisa
abrir `server.py` à mão. Mede serial, paralelo-local e distribuído-k,
repete, e gera CSVs + gráficos.

```powershell
venv\Scripts\python.exe benchmark.py --sizes 256,384,512,768 --repeats 5 --warmup 1 --servers 1,2,4,8,16 --out resultados
```

- `--sizes` — dimensões NxN a testar.
- `--repeats` — repetições medidas (a métrica central é a **mediana**).
- `--warmup` — repetições de aquecimento **descartadas** (tira o custo da
  primeira execução fria; recomendado 1).
- `--servers` — quantidades de nós distribuídos. Suba até o nº de núcleos;
  acima disso a CPU fica *oversubscrita* (ótimo para discutir saturação).
- `--server-workers 1` — 1 processo por servidor (use **1** em sweeps com
  muitos nós para não estourar a CPU).
- `--local-workers N` — processos do paralelo-local; use para comparação
  justa, ex.: `--local-workers 4` contra `--servers 4`.

> Em Python puro N=768 já leva dezenas de segundos por execução serial; uma
> rodada completa pode levar de minutos a algumas horas. Ideal para deixar
> rodando. Reduza `--repeats`/`--sizes` se quiser mais rápido.

---

## 5. Benchmark em rede (2 máquinas)

Modo `--external`: o benchmark usa servidores **já rodando** (locais +
remotos), sem subir/derrubar nada. Permite somar os núcleos de duas
máquinas (ex.: 16 + 12 = 28) — *sem oversubscription*.

### Passo 1 — Máquina B (a "remota", ex. 12 núcleos)

```powershell
# como administrador, libere as portas:
New-NetFirewallRule -DisplayName "CPC AV3" -Direction Inbound -Protocol TCP -LocalPort 5000-5011 -Action Allow

# sobe 12 servidores (1 por núcleo, portas 5000–5011):
1..12 | % { Start-Process -WindowStyle Minimized python "server.py --host 0.0.0.0 --port $(4999+$_) --workers 1 --quiet" }
```
Anote o IP da máquina B (`ipconfig`).

### Passo 2 — Máquina A (a que roda o benchmark, ex. 16 núcleos)

```powershell
# sobe 16 servidores locais (portas 5000–5015):
1..16 | % { Start-Process -WindowStyle Minimized -FilePath venv\Scripts\python.exe -ArgumentList "server.py","--host","0.0.0.0","--port","$(4999+$_)","--workers","1","--quiet" }

# teste a conexão antes da rodada longa (troque pelo IP da máquina B):
venv\Scripts\python.exe client.py 256 --comparar --servers 192.168.0.50:5000
```

### Passo 3 — Rodar o benchmark cruzando as duas máquinas

```powershell
venv\Scripts\python.exe benchmark.py --sizes 256,384,512,768 --repeats 5 --warmup 1 --servers 1,2,4,8,16,24,28 --external "127.0.0.1:5000-5015,192.168.0.50:5000-5011" --out resultados_rede
```

- `--external` aceita faixas: `host:ini-fim`. **Locais primeiro** na lista:
  assim `k≤16` usa só a máquina A; `k=24` = 16 locais + 8 remotos; `k=28` =
  16 + 12. A curva de eficiência escala até 16, **continua escalando 16→28**
  (núcleos físicos de outra máquina) — a prova de que distribuir ultrapassa o
  limite de uma máquina. Compare com uma rodada local `--servers ...,32` (que
  satura/cai após 16).
- `--servers` maiores que o pool são descartados automaticamente com aviso.

### Parar os servidores (em cada máquina)

```powershell
Get-CimInstance Win32_Process -Filter "name='python.exe'" | ? { $_.CommandLine -match 'server.py' } | % { Stop-Process -Id $_.ProcessId -Force }
```

---

## Saídas geradas

Tudo vai para a pasta de `--out` (padrão `resultados/`):

| Arquivo | Conteúdo |
|---|---|
| `resultados_brutos.csv` | Cada repetição individual (dado cru para análise no Excel/Sheets). |
| `resultados_resumo.csv` | Por configuração: **mín, máx, média, mediana, desvio** do tempo e do speedup + eficiência + corretude. |
| `grafico_tempo.png` | Tempo (mediano) × dimensão N, uma curva por modo. |
| `grafico_speedup.png` | Speedup × N. |
| `grafico_eficiencia.png` | Eficiência × nº de nós (no maior N) — mostra a saturação. |
| `grafico_boxplot_<N>.png` | Um por tamanho: distribuição do tempo entre as repetições (variância/outliers). |

---

## Métricas

- **speedup** (pareado por repetição): `speedupᵢ = T_serial[N, repᵢ] /
  T_modo[N, repᵢ]` — serial e modo medem o **mesmo** `A,B` na mesma
  repetição. A métrica central reportada é a **mediana**.
- **eficiência** = `speedup_mediano / nº de unidades` (nº de nós no
  distribuído, ou nº de processos no paralelo-local).
- **mediana + warmup**: usamos mediana (não média) e descartamos a 1ª
  repetição (aquecimento) para que uma execução fria isolada não infle o
  resultado.
- **eficiência > 1 (superlinear)**: pode ser **real** — blocos menores cabem
  melhor na cache da CPU. Deve ser explicada assim, não escondida.
- A verificação de corretude **não entra** no tempo medido (é feita fora dos
  cronômetros, contra o gabarito serial).

---

## Referência de parâmetros

### `server.py`
| Flag | Padrão | Descrição |
|---|---|---|
| `--host` | `127.0.0.1` | Interface de bind. Use `0.0.0.0` para aceitar a rede. |
| `--port` | (auto) | Porta fixa. Se omitida, acha uma livre a partir de 5000. |
| `--port-inicial` | `5000` | Início da busca de porta livre. |
| `--workers` | nº de núcleos | Processos para paralelismo interno do nó. |
| `-q`, `--quiet` | desligado | Sem painel; só uma linha de "pronto" (uso em automação/rede). |

### `client.py`
| Flag | Padrão | Descrição |
|---|---|---|
| `tamanho` | `100` | Dimensão N das matrizes NxN (argumento posicional). |
| `--comparar` | desligado | Também roda serial e paralelo-local e mostra o speedup. |
| `--servers` | (descoberta) | Lista fixa `host:porta,host:porta` (pula a varredura). |
| `--sem-paralelo-no-no` | desligado | Servidor não subdivide o bloco entre núcleos. |
| `--host` / `--port-inicial` / `--limite` | `127.0.0.1` / `5000` / `10` | Parâmetros da descoberta automática. |
| `--no-verify` | desligado | Não comparar com o gabarito serial. |

### `benchmark.py`
| Flag | Padrão | Descrição |
|---|---|---|
| `--sizes` | `128,256,384,512` | Dimensões NxN, separadas por vírgula. |
| `--repeats` | `5` | Repetições medidas (mediana entre elas). |
| `--warmup` | `1` | Repetições de aquecimento descartadas (0 desliga). |
| `--servers` | `1,2,4` | Quantidades de nós distribuídos; ex.: `1,2,4,8,16`. |
| `--server-workers` | `1` | Processos internos por servidor (local). |
| `--local-workers` | nº de núcleos | Processos do paralelo-local. |
| `--external` | (nenhum) | Usa servidores já rodando: `"127.0.0.1:5000-5015,192.168.0.50:5000-5011"`. |
| `--host` | `127.0.0.1` | Host dos servidores locais (modo automático). |
| `--out` | `resultados` | Pasta de saída. |

---

## Solução de problemas

| Sintoma | Causa / solução |
|---|---|
| "Nenhum servidor encontrado" | Suba `server.py` antes do `client.py`; confira a porta (descoberta varre 5000–5009). |
| Cliente só acha alguns servidores | O cliente faz 2 passadas de descoberta; se ainda faltar, dê alguns segundos após subir os servidores. |
| Conexão recusada em rede | Servidor precisa de `--host 0.0.0.0`; libere a porta no firewall da máquina remota; cheque o IP. |
| Servidor não responde / "trava" | Já resolvido: o servidor atende em threads e responde `PING` mesmo calculando. Atualize o código. |
| `--servers 32` mas máquina tem 16 núcleos | Esperado: acima do nº de núcleos a CPU fica oversubscrita e a eficiência cai — é resultado para discutir, não bug. |
| Benchmark muito lento | Reduza `--sizes`/`--repeats`; Python puro é O(n³) por design. |
| Portas ocupadas no benchmark local | Ele procura um bloco de portas livre automaticamente; encerre servidores manuais antigos. |

---

## Notas didáticas

- **Por que Python puro (sem numpy):** o foco é demonstrar o ganho de
  distribuir um cálculo CPU-bound. numpy/BLAS tornaria o cálculo
  quase-instantâneo e multi-thread, mascarando o efeito.
- **Por que `multiprocessing` e não `threading` no kernel:** o cálculo é
  CPU-bound; o GIL do CPython impede ganho real com threads. Threads são
  usadas só para I/O de rede (cliente) e para atender conexões (servidor).
- **Saturação local vs. distribuído real:** numa máquina só, o speedup satura
  no nº de núcleos físicos; ultrapassar isso exige **hardware separado** — a
  motivação central da computação distribuída (relacionar com a Lei de
  Amdahl no relatório).
- **Custo de comunicação:** no modo em rede, `B` trafega pela rede para cada
  nó. Em N grande/Wi-Fi esse overhead pode dominar — é um resultado honesto e
  interessante para analisar (rede real vs. loopback).
