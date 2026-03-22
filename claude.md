# claude.md

> Instruções especializadas para o agente **Claude** atuar no repositório **barrow** com alto rigor técnico, excelente disciplina editorial e mínima surpresa comportamental.

## 1. Postura esperada

- **Engenheiro de manutenção criterioso**, não autor de reescritas amplas.
- **Editor técnico preciso**, não redator prolixo sem densidade informacional.
- **Revisor preventivo**, antecipando regressões em CLI, I/O, planos e pipelines.

A prioridade é produzir mudanças confiáveis, localmente justificáveis e fáceis de auditar.

---

## 2. Como interpretar o design do barrow

O barrow é um motor analítico leve orientado por cinco compromissos:

1. **Ergonomia shell-first** — comandos pequenos, encadeáveis e previsíveis.
2. **Execução plan-based** — internamente lazy, externamente explícito. Cada comando CLI constrói um plano lógico que é otimizado antes de executar.
3. **Contratos Arrow-native** — `pa.Table` é o formato canônico entre camadas.
4. **Polimorfismo de backend** — Arrow para operações vetorizadas; DuckDB para SQL, joins pesados e windows complexas.
5. **Metadata como estado lógico** — agrupamento e ordenação vivem no plano, não apenas em metadata de schema.

---

## 3. Arquitetura-alvo

```
CLI / SQL / API  →  Frontend Adapter  →  LogicalPlan  →  Optimizer  →  Engine  →  Backend (Arrow/DuckDB)  →  I/O Adapters
```

Camadas e pacotes:

| Camada | Pacote | Responsabilidade |
|---|---|---|
| Frontend | `barrow/cli.py` | Receber input do usuário |
| Adapter | `barrow/frontend/` | Traduzir para LogicalPlan |
| Core | `barrow/core/` | Nós, plano, propriedades, resultado, erros |
| Optimizer | `barrow/optimizer/` | Transformar plano com regras |
| Engine | `barrow/execution/` | Executar plano via backends |
| Backend | `barrow/execution/backends/` | Arrow ou DuckDB |
| Operations | `barrow/operations/` | Operações tabulares puras |
| I/O | `barrow/io/` | Leitura e escrita por formato |
| Expressões | `barrow/expr/` | Parse, análise e compilação |

---

## 4. Estratégia de trabalho

### 4.1. Primeira leitura

Antes de mudar qualquer coisa, leia:

- Onde o comportamento nasce (qual camada).
- Onde ele é testado (qual `tests/` subdir).
- Onde ele é documentado (docs/, README).
- Qual é a menor superfície de alteração correta.

### 4.2. Durante a implementação

Preferir:

- Modificações cirúrgicas.
- Nomes explícitos.
- Reutilização de padrões existentes.
- `dataclasses.replace()` para transformar nós imutáveis.
- Imports lazy quando necessário para evitar ciclos.

Evitar:

- Limpezas laterais fora do escopo.
- Reestruturações não solicitadas.
- Mudanças silenciosas em defaults de I/O.
- `LogicalNode` com conhecimento de CLI ou I/O.

### 4.3. Antes de concluir

Verificar:

- O pedido foi atendido integralmente.
- O diff está coeso e respeita separação de camadas.
- Testes relevantes passam.
- Documentação foi atualizada se contrato público mudou.

---

## 5. Preferências de implementação

### Python

- Funções pequenas e semanticamente diretas.
- `frozen=True` para dataclasses de nós.
- Erros de domínio (`ExecutionError`, `PlanningError`), nunca `Exception` genérica.
- Baixa duplicação sem obscurecer o fluxo.

### Testes

- Provem o contrato alterado.
- Falhariam antes da correção.
- Legíveis sem contexto excessivo.
- Próximos da área funcional correspondente.

### Markdown

- Hierarquia forte, terminologia consistente.
- Parágrafos densos, porém legíveis.
- Exemplos operacionais quando agregarem compreensão.

---

## 6. Checklist específico

Pontos mais sensíveis ao editar código do barrow:

| Área | Risco |
|---|---|
| Integridade do LogicalPlan | Nós órfãos, schemas incompatíveis, walk() quebrado |
| Regras do optimizer | Ordem de aplicação, idempotência, semântica preservada |
| Despacho de backend | Arrow vs DuckDB devem produzir resultados equivalentes |
| I/O adapter isolation | scan/ e sink/ não devem depender de core ou optimizer |
| Defaults de `--input-format` e `--output-format` | Inferência sutil que afeta todos os comandos |
| STDIN/STDOUT | Pipes entre comandos, formato intermediário Feather |
| Metadados `grouped_by` em CSV | Round-trip via comentário no cabeçalho |
| `_set_io_defaults` em `cli.py` | Lógica delicada de inferência de formato |
| `_exec_scan` e `_exec_sink` em `engine.py` | Ponte entre plano e I/O legado |
| Expressões em Arrow vs DuckDB | Mesma expressão deve ter semântica idêntica |

---

## 7. Guia para módulos novos

### `barrow/core/`

- Nós são **imutáveis** — use `dataclasses.replace()` para transformar.
- `LogicalPlan.walk()` retorna nós bottom-up — Scan primeiro, Sink por último.
- `ExecutionResult` sempre encapsula `pa.Table` + `LogicalProperties`.
- Novos tipos de nó devem ser `@dataclass(frozen=True)` com child/children.

### `barrow/optimizer/`

- Regras são funções puras: `node: LogicalNode → LogicalNode`.
- Recursão pelos children via `getattr(node, attr)` para `child`, `left`, `right`.
- `dataclasses.replace()` para produzir nós transformados.
- Testar com planos construídos manualmente, verificar estrutura do resultado.

### `barrow/execution/`

- `execute(node)` é recursivo — executa children primeiro, depois aplica operação.
- `ArrowBackend` delega para `barrow.operations.*` (imports lazy dentro dos métodos).
- `DuckDBBackend` delega para `barrow.operations.sql`.
- Sink e Scan tratam I/O via `barrow.io.read_table/write_table`.

### `barrow/frontend/`

- `cli_to_plan(command, args)` constrói `Scan → Operation → Sink`.
- Cada builder é uma função privada `_build_X(args)`.
- Não executa — apenas constrói plano.
- Window e mutate parsam `NAME=EXPR` assignments.

---

## 8. Regra editorial

Ao produzir documentação, buscar equilíbrio entre completude, precisão, elegância e densidade técnica. Documentação densa, organizada e imediatamente operacional.

---

## 9. Instrução final

Se houver múltiplas formas válidas de resolver uma tarefa, escolha a que combina:

- Menor risco de regressão.
- Maior clareza para revisão humana.
- Melhor aderência ao estilo e à arquitetura do repositório.
- Melhor utilidade prática para quem manterá o código depois.
- Respeito à separação de camadas da arquitetura plan-based.
