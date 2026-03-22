# agent.md

> Manual operacional compacto para um agente individual atuar com alta precisão no repositório **barrow**.
>
> Este arquivo complementa o `agents.md` com foco em execução imediata. Use-o como protocolo de decisão rápida antes, durante e depois de qualquer modificação. Para contexto arquitetural mais amplo, consulte `agents.md`. Para instruções específicas do Claude, consulte `claude.md`.

---

## 1. Modelo mental do projeto

O `barrow` está evoluindo de uma CLI simples para um motor analítico leve. A arquitetura-alvo organiza o fluxo de dados em oito camadas:

```
Frontend (CLI / SQL / API)
    ↓
Frontend Adapter (cli_to_plan / sql_to_plan)
    ↓
LogicalPlan (árvore de LogicalNode)
    ↓
Optimizer (pushdown, fusion, simplify, backend selection)
    ↓
ExecutionEngine (despacho para backends)
    ↓
Backends (Arrow nativo / DuckDB)
    ↓
I/O Adapters (scan: leitura por formato / sink: escrita por formato)
    ↓
Resultado (STDOUT, arquivo, ou ExecutionResult)
```

### Princípio central

Toda intenção do usuário — seja um comando CLI, uma query SQL ou uma chamada de API — deve ser traduzida em um `LogicalPlan`, otimizada e executada por um backend apropriado. As camadas anteriores (CLI direta → operações → writer) continuam funcionais durante a transição, mas o código novo deve seguir o fluxo plan-based.

### O que muda em relação ao modelo anterior

| Antes (v0.1.0) | Agora (arquitetura-alvo) |
|---|---|
| CLI chama operação diretamente | CLI → adapter → plano → otimizador → engine |
| I/O monolítico (reader.py/writer.py) | I/O segmentado em scan/ e sink/ por formato |
| Sem otimização | Regras de pushdown, fusion, simplify |
| Backend único (Arrow) | Arrow + DuckDB com seleção automática |
| Sem plano lógico | LogicalNode compõe LogicalPlan com propriedades |

---

## 2. Mapa do repositório

```
barrow/
├── __init__.py
├── cli.py                      # CLI principal (argparse, subcomandos)
├── errors.py                   # BarrowError, InvalidExpressionError, UnsupportedFormatError
│
├── core/                       # NOVO — fundações do motor analítico
│   ├── __init__.py
│   ├── logical_node.py         # LogicalNode (Scan, Filter, Select, Mutate, GroupBy, Sort, Join, etc.)
│   ├── logical_plan.py         # LogicalPlan (árvore de nós, validação, traversal)
│   ├── logical_properties.py   # LogicalProperties (schema estimado, cardinalidade, constraints)
│   ├── execution_result.py     # ExecutionResult (tabela + metadados + estatísticas)
│   └── errors.py               # Erros tipados: PlanError, ValidationError, OptimizationError
│
├── optimizer/                  # NOVO — otimização de planos
│   ├── __init__.py
│   ├── optimizer.py            # Optimizer (aplica regras em ordem)
│   └── rules/
│       ├── __init__.py
│       ├── pushdown.py         # Pushdown de filtros e projeções
│       ├── fusion.py           # Fusão de nós adjacentes compatíveis
│       ├── simplify.py         # Simplificação de expressões e eliminação de nós redundantes
│       └── backend_selection.py # Seleção de backend por heurística de custo
│
├── execution/                  # NOVO — motor de execução
│   ├── __init__.py
│   ├── engine.py               # ExecutionEngine (despacho de plano para backend)
│   ├── arrow_backend.py        # Backend Arrow nativo (PyArrow compute)
│   └── duckdb_backend.py       # Backend DuckDB (SQL via duckdb-python)
│
├── frontend/                   # NOVO — adaptadores de frontend
│   ├── __init__.py
│   ├── cli_adapter.py          # cli_to_plan: args do argparse → LogicalPlan
│   └── sql_adapter.py          # sql_to_plan: string SQL → LogicalPlan
│
├── operations/                 # Operações tabulares (legado + novas)
│   ├── __init__.py
│   ├── filter.py
│   ├── select.py
│   ├── mutate.py
│   ├── groupby.py
│   ├── summary.py
│   ├── ungroup.py
│   ├── join.py
│   ├── window.py               # Funções de janela (existente, não exposto na CLI ainda)
│   ├── sql.py                  # Execução SQL via DuckDB (existente, não exposto na CLI ainda)
│   ├── sort.py                 # NOVO — ordenação de tabelas
│   ├── _env.py                 # Ambiente de avaliação de expressões
│   └── _expr_eval.py           # Avaliação de expressões sobre tabelas
│
├── io/                         # Entrada e saída
│   ├── __init__.py
│   ├── reader.py               # Leitor unificado (legado)
│   ├── writer.py               # Escritor unificado (legado)
│   ├── scan/                   # NOVO — adaptadores de leitura por formato
│   │   ├── __init__.py
│   │   ├── csv_scan.py
│   │   ├── parquet_scan.py
│   │   ├── feather_scan.py
│   │   └── orc_scan.py
│   └── sink/                   # NOVO — adaptadores de escrita por formato
│       ├── __init__.py
│       ├── csv_sink.py
│       ├── parquet_sink.py
│       ├── feather_sink.py
│       └── orc_sink.py
│
├── expr/                       # Parser e AST de expressões
│   ├── __init__.py
│   └── parser.py               # Expression, Literal, Name, BinaryExpression, parse()
│
tests/                          # Testes por domínio funcional
├── conftest.py
├── test_cli.py
├── test_cli_completion.py
├── test_cli_io_defaults.py
├── cli/
│   └── test_view.py
├── operations/
│   ├── test_filter.py, test_select.py, test_mutate.py
│   ├── test_groupby_summary.py, test_ungroup.py
│   ├── test_join.py, test_window.py, test_sql.py
│   └── test_env.py
├── io/
│   ├── test_io.py, test_delimiters.py
│   ├── test_writer_reader_csv.py
│   ├── test_cli_output_formats.py
│   └── test_tmp_pipeline_feather.py
├── expr/
│   ├── test_parser.py
│   └── test_parse_errors.py
├── core/                       # NOVO
├── optimizer/                  # NOVO
├── execution/                  # NOVO
└── frontend/                   # NOVO

docs/                           # Documentação do produto
scripts/                        # Scripts auxiliares
```

---

## 3. Perguntas obrigatórias antes de codar

Antes de editar qualquer arquivo, responda mentalmente:

1. **Qual camada estou tocando?** — CLI, frontend, core, optimizer, execution, operations, I/O, expr, testes, docs?
2. **Qual é o contrato observável que está sendo mudado?** — Assinatura de função, formato de saída, comportamento de flag, schema de plano?
3. **A mudança respeita a separação de camadas?** — Frontend não deve conhecer backends; optimizer não deve fazer I/O; operations não devem importar argparse.
4. **Existe teste cobrindo algo parecido?** — Verifique `tests/` no subdiretório correspondente.
5. **A mudança pode quebrar pipeline shell existente?** — STDIN/STDOUT, delimitadores, formato intermediário Feather.
6. **A mudança afeta a integridade do LogicalPlan?** — Se sim, verifique validação, serialização e traversal.
7. **A mudança afeta despacho de backend?** — Se sim, verifique que ambos (Arrow e DuckDB) continuam funcionais.
8. **O comportamento deve ser documentado publicamente?** — Novos comandos, flags, formatos.

Se alguma resposta estiver nebulosa, investigue antes de editar.

---

## 4. Protocolo de implementação

### 4.1. Se a mudança for na CLI (`barrow/cli.py`)

- Preserve nomes de comandos e semântica de flags existentes.
- Mantenha `help`, `description` e `epilog` coerentes com o padrão atual.
- Novos comandos: `sort`, `sql`, `window`, `explain` — seguir padrão dos existentes.
- O comando `explain` deve imprimir o plano lógico (e opcionalmente o otimizado) sem executar.
- Trate defaults de formato com cuidado — leia a lógica em `_set_io_defaults`.
- Valide combinações inválidas de argumentos cedo, com mensagens claras.

### 4.2. Se a mudança for no core (`barrow/core/`)

- `LogicalNode` é a unidade atômica — cada tipo de nó (Scan, Filter, Select, Mutate, GroupBy, Sort, Join, Aggregate, Window, SQL) deve ser uma classe imutável ou dataclass frozen.
- `LogicalPlan` compõe nós em árvore — deve suportar traversal (walk, transform, fold), validação de schema e clonagem.
- `LogicalProperties` são inferidas, não atribuídas manualmente — schema, cardinalidade estimada, constraints.
- `ExecutionResult` encapsula `pa.Table` + metadados (tempo de execução, backend usado, linhas processadas).
- Erros tipados: nunca lance `Exception` genérica no core; use `PlanError`, `ValidationError`, etc.
- Imutabilidade: nós e planos não devem ser mutados após construção; transformações produzem novos objetos.

### 4.3. Se a mudança for em operações (`barrow/operations/`)

- Não introduza conhecimento de argparse ou CLI na camada de operações.
- Mantenha entradas e saídas como `pa.Table` puro.
- O novo `sort.py` deve aceitar lista de colunas e direções (asc/desc).
- `window.py` e `sql.py` já existem — ao integrá-los à CLI, não altere suas assinaturas sem necessidade.
- Cubra edge cases com testes pequenos e claros.

### 4.4. Se a mudança for no optimizer (`barrow/optimizer/`)

- Cada regra é uma função ou classe que recebe um `LogicalPlan` e retorna um `LogicalPlan` transformado.
- Regras devem ser puras: sem efeitos colaterais, sem I/O, sem estado compartilhado.
- Ordem importa: pushdown antes de fusion; simplify antes de backend_selection.
- `backend_selection` anota nós com o backend preferido, não executa.
- Toda regra deve ter testes que comprovem a transformação do plano (entrada → saída esperada).
- Regra que não se aplica deve retornar o plano inalterado.

### 4.5. Se a mudança for na execução (`barrow/execution/`)

- `ExecutionEngine` recebe um `LogicalPlan` (já otimizado) e produz um `ExecutionResult`.
- Cada backend implementa uma interface comum — receber nó ou subplano, retornar `pa.Table`.
- `arrow_backend.py` usa PyArrow compute kernels diretamente.
- `duckdb_backend.py` converte subplano em SQL e executa via duckdb-python.
- O despacho pode ser por nó (um nó no Arrow, outro no DuckDB) ou por subplano inteiro.
- Fallback: se o backend anotado falhar, tentar o outro antes de propagar erro.

### 4.6. Se a mudança for no frontend (`barrow/frontend/`)

- `cli_adapter.py` traduz `argparse.Namespace` → `LogicalPlan` — um adapter por subcomando.
- `sql_adapter.py` traduz string SQL → `LogicalPlan` — pode usar parsing do DuckDB ou parser próprio.
- Adapters não executam — apenas constroem planos.
- Adapters devem validar entrada e produzir erros claros antes de construir o plano.

### 4.7. Se a mudança for em I/O (`barrow/io/`)

- `reader.py` e `writer.py` continuam como interface legada — não quebre.
- Novos adapters em `scan/` e `sink/` isolam lógica por formato.
- Cada scan adapter retorna `pa.Table`; cada sink adapter recebe `pa.Table` e destino.
- Scan adapters devem suportar pushdown de colunas (projeção) quando o formato permitir.
- Teste STDIN/STDOUT, delimitadores, inferência de formato e round-trip.

### 4.8. Se a mudança for em expressões (`barrow/expr/`)

- Não quebre gramática existente sem necessidade forte.
- Produza `InvalidExpressionError` com mensagens legíveis.
- Teste tanto parsing correto quanto erros de parsing.
- Se a expressão precisar ser traduzida para SQL (para DuckDB backend), garanta que a conversão é fiel.
- Não amplie gramática de modo ambíguo.

---

## 5. Critérios de excelência

Uma mudança excelente neste projeto:

- É **curta no diff** e **forte em semântica**.
- Respeita a **separação de camadas** da arquitetura-alvo.
- É **coberta por testes** úteis e determinísticos.
- É **coerente com a documentação** pública.
- É **fácil de revisar** linha por linha.
- Não introduz **acoplamento** entre camadas que devem ser independentes.

### Sinais de baixa qualidade

- Abstração nova para esconder poucas linhas simples.
- Refactor paralelo sem relação direta com o pedido.
- Dependência nova sem ganho claro.
- Documentação desatualizada após mudar comportamento público.
- Ausência de teste para bug corrigido.
- LogicalNode que conhece detalhes de I/O ou de CLI.
- Regra de otimização com efeitos colaterais.
- Backend que modifica o plano em vez de apenas executá-lo.

---

## 6. Sequência de validação

Sempre que possível, execute nesta ordem:

```bash
make format       # Formatação (black + ruff via pre-commit)
make lint         # Linting (pre-commit run --all-files)
make test         # Suite completa (pytest)
```

Para escopo localizado, complemente com pytest direcionado:

```bash
# Exemplos
pytest tests/core/                    # Apenas core
pytest tests/optimizer/               # Apenas optimizer
pytest tests/operations/test_sort.py  # Apenas sort
pytest tests/test_cli.py -k "sort"    # Apenas testes de CLI relacionados a sort
```

---

## 7. Pontos sensíveis

Áreas que exigem atenção redobrada ao modificar:

| Área | Risco |
|---|---|
| `_set_io_defaults` em `cli.py` | Lógica sutil de inferência de formato que afeta todos os comandos |
| Metadados `grouped_by` em CSV | Persistidos como comentário no cabeçalho; fácil de quebrar round-trip |
| Pipe intermediário com `--tmp` | Formato Feather entre comandos encadeados; testar sempre o pipeline completo |
| Integridade do `LogicalPlan` | Nós órfãos, schemas incompatíveis, propriedades desatualizadas |
| Regras do optimizer | Ordem de aplicação, idempotência, preservação de semântica |
| Despacho de backend | Arrow vs DuckDB — garantir que ambos produzem resultados equivalentes |
| Expressões em SQL vs Arrow | Mesma expressão deve ter semântica idêntica em ambos os backends |
| Scan com pushdown | Pushdown de colunas não deve alterar semântica; colunas necessárias para filtro devem ser preservadas |

---

## 8. Regra final

Se estiver em dúvida entre uma solução "engenhosa" e uma solução "óbvia, legível e robusta", escolha a segunda. Se estiver em dúvida sobre qual camada deve conter a lógica, consulte o diagrama da seção 1 — a resposta quase sempre está na separação de responsabilidades.
