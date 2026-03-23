# AGENTS.md

> Guia operacional principal para agentes de IA que trabalham no repositório **barrow**.
>
> Este documento define o contexto técnico do projeto, expectativas de qualidade, fluxo de trabalho, critérios de validação e padrões de intervenção no código.

## 1. Missão e visão do projeto

**barrow** é uma ferramenta de linha de comando para manipulação tabular baseada em **Apache Arrow**, com foco em ergonomia Unix, composição por `STDIN`/`STDOUT` e interoperabilidade entre múltiplos formatos colunares e semiestruturados.

O projeto evoluiu de uma CLI simples para um **motor analítico leve** com planejamento lógico, otimização e múltiplos backends de execução.

### Objetivos centrais

1. Disponibilizar transformações tabulares simples e composáveis via CLI.
2. Preservar pipelines de shell curtos, legíveis e previsíveis.
3. Usar **Arrow** como representação tabular em memória.
4. Suportar entrada e saída em **CSV**, **Parquet**, **Feather** e **ORC**.
5. Manter uma base de código pequena, explícita e fácil de evoluir.
6. Unificar a execução em torno de um **plano lógico** único.
7. Otimizar pipelines automaticamente via regras de pushdown, fusão e simplificação.
8. Selecionar backend de execução (**Arrow** ou **DuckDB**) por operação.
9. Preparar o terreno para frontends adicionais: **SQL** (implementado), **API Python** (futuro).

---

## 2. Arquitetura do sistema

```
Frontend (CLI / SQL / API)
    ↓
Frontend Adapter (cli_to_plan / sql_to_plan)
    ↓
LogicalPlan (árvore de LogicalNode)
    ↓
Optimizer (pushdown, fusion, simplify, backend selection)
    ↓
Execution Engine (despacho para backends)
    ↓
Backends (Arrow nativo / DuckDB)
    ↓
I/O Adapters (scan: leitura por formato / sink: escrita por formato)
    ↓
Resultado (STDOUT, arquivo, ou ExecutionResult)
```

### Princípios arquiteturais

1. **Um motor, muitos frontends.** CLI, SQL e futura API Python compilam para o mesmo plano lógico.
2. **Lazy internamente, explícito externamente.** O usuário mantém a experiência CLI atual, mas a execução é diferida até o motor ter um plano completo para otimizar.
3. **Contratos Arrow-native.** Arrow é o formato canônico entre camadas.
4. **Polimorfismo de backend.** O planner escolhe o melhor backend por operação.
5. **Metadata é estado lógico.** Agrupamento, ordenação e particionamento vivem no plano lógico, não apenas em metadata de schema.
6. **Adapters de formato são isolados.** Detecção de formato, scan e sink ficam fora da lógica de planejamento.

---

## 3. Princípios mandatórios

### 3.1. Clareza antes de esperteza

Prefira soluções explícitas, previsíveis, fáceis de testar e alinhadas ao estilo existente.

### 3.2. Mudanças mínimas, porém completas

Cada intervenção deve ser mínima no raio de impacto, completa no fechamento do problema, coesa no escopo e consistente com testes, documentação e comportamento observável.

### 3.3. Compatibilidade comportamental

Preserve flags, defaults de I/O, pipelines shell e mensagens existentes. Breaking changes devem ser justificadas, documentadas e testadas.

### 3.4. Separação de camadas

Frontend não deve conhecer backends. Optimizer não deve fazer I/O. Operations não devem importar argparse. Core não deve depender da CLI.

---

## 4. Mapa do repositório

```
barrow/
├── cli.py                       # CLI principal (argparse, subcomandos)
├── errors.py                    # Re-exporta erros de core/errors.py
│
├── core/                        # Motor analítico
│   ├── errors.py                # Hierarquia tipada de erros
│   ├── nodes.py                 # LogicalNode e todos os tipos de nó
│   ├── plan.py                  # LogicalPlan (árvore, traversal, formatação)
│   ├── properties.py            # LogicalProperties (group_keys, ordering, etc.)
│   ├── result.py                # ExecutionResult (tabela + propriedades)
│   └── schema.py                # Utilitários de validação de schema
│
├── optimizer/                   # Otimização de planos
│   ├── optimizer.py             # optimize(plan) → plan
│   └── rules/
│       ├── simplify.py          # Remove nós redundantes
│       ├── fusion.py            # Funde mutates/projects adjacentes
│       ├── filter_pushdown.py   # Empurra filtros para perto do scan
│       ├── projection_pushdown.py # Empurra projeções para o scan
│       └── backend_selection.py # Anota backend preferido (hook futuro)
│
├── execution/                   # Motor de execução
│   ├── engine.py                # execute(node) → ExecutionResult
│   └── backends/
│       ├── arrow_backend.py     # Backend Arrow (delega para operations/)
│       └── duckdb_backend.py    # Backend DuckDB (SQL)
│
├── frontend/                    # Adaptadores de frontend
│   ├── cli_to_plan.py           # argparse.Namespace → LogicalPlan
│   └── sql_to_plan.py           # string SQL → LogicalPlan
│
├── operations/                  # Operações tabulares
│   ├── filter.py, select.py, mutate.py
│   ├── groupby.py, summary.py, ungroup.py
│   ├── join.py, window.py, sql.py, sort.py
│   ├── _env.py, _expr_eval.py
│
├── io/                          # Entrada e saída
│   ├── reader.py, writer.py     # Interface legada (mantida)
│   ├── formats.py               # Detecção de formato
│   ├── options.py               # ScanOptions, SinkOptions
│   ├── scan/                    # Adaptadores de leitura por formato
│   │   ├── csv.py, parquet.py, feather.py, orc.py
│   └── sink/                    # Adaptadores de escrita por formato
│       ├── csv.py, parquet.py, feather.py, orc.py
│
├── expr/                        # Parser e AST de expressões
│   ├── parser.py                # Expression, parse()
│   ├── analyzer.py              # referenced_names(), validate_expression()
│   └── compiler.py              # to_sql() — compila expressão para SQL
│
tests/                           # Testes por domínio funcional
├── core/, optimizer/, execution/, frontend/
├── operations/, io/, expr/, cli/
docs/                            # Documentação
```

---

## 5. Padrões de intervenção por área

### 5.1. CLI (`barrow/cli.py`)

- Todos os comandos seguem o padrão: `cli_to_plan → optimize → execute`.
- Preserve nomes, flags, help e epilog dos comandos existentes.
- Novos comandos: seguir o mesmo padrão dos existentes.
- `explain` imprime o plano lógico e otimizado sem executar.

### 5.2. Core (`barrow/core/`)

- `LogicalNode` e subclasses são **frozen dataclasses** — imutáveis após construção.
- Transformações produzem novos objetos via `dataclasses.replace()`.
- `LogicalPlan` é wrapper com `walk()` (bottom-up) e `format_plan()` (legível).
- `ExecutionResult` encapsula `pa.Table` + `LogicalProperties`.
- Erros tipados: `ExecutionError`, `PlanningError`, `FrontendError`, etc.

### 5.3. Optimizer (`barrow/optimizer/`)

- Cada regra é uma função pura: `node → node`.
- Regras não fazem I/O, não acessam estado global, não executam operações.
- Ordem de aplicação: simplify → fusion → filter_pushdown → projection_pushdown → backend_selection.
- Regra que não se aplica retorna o nó inalterado.

### 5.4. Execução (`barrow/execution/`)

- `execute(node)` caminha recursivamente pela árvore de nós.
- Despacha para `ArrowBackend` (maioria das operações) ou `DuckDBBackend` (SQL).
- `ArrowBackend` delega para funções de `barrow/operations/`.
- `_exec_scan` e `_exec_sink` tratam I/O usando `barrow.io`.

### 5.5. Frontend (`barrow/frontend/`)

- Adapters traduzem input do usuário em `LogicalPlan`.
- Não executam — apenas constroem planos.
- Validam entrada e produzem erros claros antes de construir o plano.

### 5.6. Operações (`barrow/operations/`)

- Funções puras: `pa.Table` → `pa.Table`.
- Não importam argparse, CLI ou detalhes de frontend.
- O `ArrowBackend` delega para estas funções.

### 5.7. I/O (`barrow/io/`)

- `reader.py` e `writer.py` continuam como interface legada — não quebre.
- `scan/` e `sink/` isolam lógica por formato.
- Teste STDIN/STDOUT, delimitadores e round-trip.

### 5.8. Expressões (`barrow/expr/`)

- `parser.py` converte string → AST de `Expression`.
- `analyzer.py` extrai nomes referenciados e valida contra schema.
- `compiler.py` converte AST → SQL string.
- Não quebre gramática existente.

---

## 6. Fluxo de trabalho recomendado

### 6.1. Antes de editar

1. Compreenda o objetivo real do pedido.
2. Identifique qual **camada** será afetada.
3. Verifique testes existentes mais próximos.
4. Planeje a menor mudança que resolva o problema com qualidade.

### 6.2. Durante a edição

- Diffs pequenos e intencionais.
- Preserve estilo e convenções do arquivo.
- Respeite separação de camadas.
- Atualize documentação se mudar contrato público.

### 6.3. Depois da edição

```bash
make format       # black + ruff
make lint         # pre-commit run --all-files
make test         # pytest
```

---

## 7. Estratégia de testes

| Tipo de mudança | O que testar |
|---|---|
| Core (nodes, plan) | Construção, imutabilidade, traversal, formatação |
| Optimizer (rules) | Transformação de plano: entrada → saída esperada |
| Execution (engine) | End-to-end: Scan → operação → resultado correto |
| Backend (arrow/duckdb) | Operação isolada com pa.Table de entrada |
| Frontend (cli_to_plan) | Namespace → plano com nós corretos |
| CLI | Subprocess ou main() com args, verificar output |
| I/O | Round-trip por formato, STDIN/STDOUT, delimitadores |
| Expressões | Parsing correto + erros sintáticos claros |

---

## 8. Roadmap técnico

### Fase 1 — Fundacional (✅ implementada)

- Core: `LogicalNode`, `LogicalPlan`, `ExecutionResult`, `LogicalProperties`.
- Optimizer com regras: simplify, fusion, filter pushdown, projection pushdown, backend selection.
- Execution engine com backends Arrow e DuckDB.
- Frontend adapters: `cli_to_plan`, `sql_to_plan`.
- I/O adapters: scan e sink por formato.
- Novos comandos CLI: `sort`, `sql`, `window`, `explain`.
- Hierarquia tipada de erros.
- Todas os comandos existentes re-wired via plan-based pipeline.

### Fase 2 — Performance

- Projection/filter pushdown ativo em scans (Parquet column pruning).
- Execução lazy por default.
- Seleção de backend baseada em custo estimado.
- Materialização inteligente.

### Fase 3 — Produto final

- Parser SQL completo → plano lógico.
- API Python fluent: `barrow.scan().filter().collect()`.
- Execução chunked/streaming para operações stateless.
- `explain` com estatísticas de execução.

### Fase 4 — Experiência avançada

- Dataset scans (diretórios Parquet).
- Partition-aware reads.
- Cache intermediário.
- Observabilidade detalhada.

---

## 9. Anti-padrões a evitar

- Alterar código sem ler os testes relacionados.
- Mudar contratos públicos sem atualizar documentação.
- Introduzir dependências novas por conveniência pontual.
- Misturar responsabilidades de parsing, execução e serialização.
- `LogicalNode` que conhece detalhes de I/O ou CLI.
- Regra de otimização com efeitos colaterais.
- Backend que modifica o plano em vez de apenas executá-lo.
- Corrigir um bug específico com refactor desnecessariamente amplo.

---

## 10. Checklist operacional

Antes de encerrar uma tarefa, confirme:

- [ ] A mudança resolve exatamente o pedido.
- [ ] O escopo permaneceu controlado.
- [ ] O código segue o padrão local.
- [ ] A separação de camadas foi respeitada.
- [ ] Testes relevantes foram executados.
- [ ] Documentação foi atualizada, se necessário.
- [ ] Mensagens de erro e ajuda continuam claras.
- [ ] A ergonomia Unix da ferramenta foi preservada.

---

## 11. Resumo executivo

1. Entenda a arquitetura: Frontend → Plan → Optimizer → Engine → Backend → I/O.
2. Respeite a separação de camadas.
3. Mude o mínimo necessário, com máxima clareza.
4. Preserve compatibilidade, especialmente em pipelines.
5. Escreva testes proporcionais ao impacto.
6. Atualize docs quando o comportamento público mudar.
7. Entregue mudanças sólidas, legíveis e revisáveis.
