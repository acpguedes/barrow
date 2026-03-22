# AGENTS.md

> Guia operacional principal para agentes de IA que trabalham no repositório **barrow**.
>
> Este documento define o contexto técnico do projeto, expectativas de qualidade, fluxo de trabalho, critérios de validação e padrões de intervenção no código. Ele foi escrito para maximizar previsibilidade, rigor técnico, segurança de mudanças e qualidade de manutenção.

## 1. Missão do projeto

**barrow** é uma ferramenta de linha de comando para manipulação tabular baseada em **Apache Arrow**, com foco em ergonomia Unix, composição por `STDIN`/`STDOUT` e interoperabilidade entre múltiplos formatos colunares e semiestruturados para análise de dados.

### Objetivos centrais

- disponibilizar transformações tabulares simples e composáveis via CLI;
- preservar pipelines de shell curtos, legíveis e previsíveis;
- usar **Arrow** como representação tabular em memória;
- suportar entrada e saída em **CSV**, **Parquet**, **Feather** e **ORC**;
- manter uma base de código pequena, explícita e relativamente fácil de evoluir;
- preparar o terreno para uma arquitetura mais unificada envolvendo CLI, SQL e, potencialmente, API Python.

### Natureza do código atual

O projeto já possui uma separação razoável entre:

- **camada de interface** (`barrow/cli.py`),
- **operações de transformação** (`barrow/operations/`),
- **leitura e escrita** (`barrow/io/`),
- **parsing de expressões** (`barrow/expr/`),
- **testes automatizados** (`tests/`),
- **documentação de produto e arquitetura** (`docs/`).

Agentes devem preservar e reforçar essa separação, evitando acoplamento desnecessário entre parsing, execução, I/O e UX da CLI.

---

## 2. Princípios mandatórios para qualquer mudança

### 2.1. Clareza antes de esperteza

Prefira soluções:

- mais explícitas do que mágicas;
- mais previsíveis do que excessivamente abstraídas;
- fáceis de testar isoladamente;
- alinhadas ao estilo já existente no repositório.

### 2.2. Mudanças mínimas, porém completas

Cada intervenção deve ser:

- **mínima** no raio de impacto;
- **completa** no fechamento do problema;
- **coesa** no escopo;
- **consistente** com documentação, testes e comportamento observável.

Não deixe a base em estado “quase pronto”. Se uma mudança altera comportamento público, considere:

1. código,
2. testes,
3. documentação,
4. mensagens de erro,
5. exemplos de uso.

### 2.3. Compatibilidade comportamental

Evite breaking changes implícitas. Sempre que possível:

- preserve flags existentes;
- preserve mensagens e fluxos de uso já documentados;
- preserve defaults de I/O;
- preserve pipelines shell existentes.

Se um breaking change for inevitável, ele deve ser claramente justificado, documentado e coberto por testes.

### 2.4. Precisão sem exagero arquitetural

Não introduza frameworks, metaprogramação ou camadas desnecessárias para problemas localizados. O projeto valoriza simplicidade operacional.

---

## 3. Mapa conceitual do repositório

## 3.1. Diretórios mais importantes

- `barrow/cli.py`  
  Define a CLI principal, parser de argumentos, subcomandos e a cola entre entrada, operações e saída.

- `barrow/io/`  
  Responsável por leitura, detecção/formatação e serialização tabular.

- `barrow/operations/`  
  Implementa as transformações centrais como `filter`, `select`, `mutate`, `groupby`, `summary`, `ungroup`, `join`, além de componentes em evolução como SQL e window.

- `barrow/expr/`  
  Parsing e representação de expressões usadas por `filter` e `mutate`.

- `tests/`  
  Suite automatizada organizada por domínio funcional.

- `docs/`  
  Documentação funcional, manual de uso, instalação e visão arquitetural.

### 3.2. Fluxo operacional da CLI

Em linhas gerais, o fluxo atual é:

1. parse de argumentos na CLI;
2. aplicação de defaults de I/O;
3. leitura da tabela de entrada;
4. transformação tabular via módulo de operações;
5. escrita da saída;
6. tratamento de `BarrowError` com retorno não-zero.

Agentes devem respeitar esse fluxo e manter a CLI como camada fina, com o máximo de lógica movido para funções reutilizáveis sempre que isso melhorar legibilidade e testabilidade.

---

## 4. Padrões de intervenção por área

## 4.1. CLI (`barrow/cli.py`)

Ao alterar a CLI:

- mantenha descrições, `help`, `epilog` e nomes de argumentos coerentes;
- preserve a consistência entre subcomandos;
- garanta que defaults de I/O continuem intuitivos;
- valide argumentos cedo e com mensagens claras;
- evite duplicação de lógica entre comandos.

### Boas práticas específicas

- argumentos compartilhados devem continuar centralizados sempre que possível;
- mensagens de erro devem ser objetivas e acionáveis;
- flags com efeito especial como `--tmp` exigem testes de regressão;
- se um comando ignora uma flag por design, isso deve estar documentado.

## 4.2. Operações (`barrow/operations/`)

Ao alterar operações tabulares:

- preserve semântica declarativa;
- evite embutir detalhes de CLI na camada de operações;
- prefira entradas e saídas tipadas/claras;
- trate casos de borda explicitamente;
- mantenha nomes alinhados ao domínio analítico.

### Exemplos de cuidado

- `groupby` e `summary` devem manter semântica coerente entre metadados lógicos e formato persistido;
- `join` deve explicitar diferenças entre tipos de junção;
- `mutate` e `filter` não devem aceitar ambiguidades silenciosas na avaliação de expressões.

## 4.3. I/O (`barrow/io/`)

Ao mexer em leitura/escrita:

- valide impactos em `STDIN`/`STDOUT`;
- preserve inferência por extensão quando aplicável;
- preserve coerência entre delimitadores de entrada e saída;
- tenha atenção a diferenças entre formatos colunares e CSV;
- trate pipe intermediário com Feather de forma explícita e testável.

## 4.4. Expressões (`barrow/expr/`)

Ao alterar parser ou avaliação:

- não quebre gramática existente sem necessidade forte;
- produza erros sintáticos claros;
- mantenha previsibilidade semântica;
- teste tanto parsing quanto erros de parsing;
- evite comportamento implícito difícil de explicar ao usuário.

## 4.5. Testes (`tests/`)

Toda mudança relevante deve adicionar ou ajustar testes próximos do domínio afetado.

Diretrizes:

- prefira testes pequenos, focados e determinísticos;
- inclua ao menos um teste de regressão para bugs corrigidos;
- quando o comportamento afeta a CLI, teste a CLI observável;
- quando o comportamento é interno, teste a unidade mais próxima da regra alterada;
- não adicione testes redundantes que só repetem cobertura sem novo valor semântico.

---

## 5. Fluxo recomendado de trabalho para agentes

## 5.1. Antes de editar

1. compreenda o objetivo real do pedido;
2. localize os módulos afetados;
3. verifique se há documentação relacionada;
4. identifique os testes existentes mais próximos;
5. planeje a menor mudança que resolva o problema com qualidade.

## 5.2. Durante a edição

- mantenha diffs pequenos e intencionais;
- preserve estilo e convenções do arquivo;
- não faça refactors amplos sem necessidade direta;
- atualize documentação se a mudança alterar contrato público;
- prefira nomes semânticos a abreviações obscuras.

## 5.3. Depois da edição

Execute o máximo viável de validações relevantes, em ordem aproximada:

```bash
make format
make lint
make test
```

Se o escopo for pequeno e o tempo/importações forem um fator, rode também subconjuntos focados de `pytest` relacionados ao módulo alterado.

---

## 6. Convenções de qualidade

## 6.1. Legibilidade de código

O código deve ser:

- fácil de ler em revisão;
- fácil de depurar;
- fácil de estender sem adivinhar intenções ocultas.

### Preferir

- funções pequenas e bem nomeadas;
- condicionais simples;
- separação clara entre validação, transformação e serialização;
- docstrings quando agregarem valor real.

### Evitar

- sobrecarga semântica de funções únicas;
- lógica implícita dispersa;
- duplicação de mensagens e regras em muitos pontos;
- dependências novas sem justificativa forte.

## 6.2. Tratamento de erro

Tratamentos de erro devem:

- falhar cedo;
- explicar o problema em linguagem objetiva;
- evitar stack traces desnecessários para erros esperados de usuário final;
- usar exceções do domínio quando isso já fizer parte do projeto.

## 6.3. Compatibilidade com shell e pipelines

Sempre considere:

- uso em pipes;
- leitura de `STDIN` sem arquivo nomeado;
- escrita em `STDOUT`;
- delimitadores customizados;
- persistência correta de formato intermediário.

---

## 7. Critérios para documentação

Atualize documentação quando houver mudança em:

- comandos disponíveis;
- argumentos/flags;
- semântica de operação;
- formatos suportados;
- exemplos públicos;
- fluxo de instalação, teste ou desenvolvimento.

### Locais naturais para atualização

- `README.md` para visão geral e exemplos rápidos;
- `docs/usage.md` e `docs/manual.md` para comportamento funcional;
- `docs/architecture.md` para decisões estruturais e direção do projeto.

---

## 8. Estratégia de testes por tipo de mudança

### Mudança em CLI

Validar:

- parsing de argumentos;
- defaults de formato;
- mensagens de erro;
- compatibilidade com `STDIN`/`STDOUT`;
- comportamento de flags especiais.

### Mudança em I/O

Validar:

- leitura e escrita por formato;
- delimitadores;
- round-trip quando aplicável;
- uso em pipes;
- inferência de formato.

### Mudança em expressão

Validar:

- parsing correto;
- erros sintáticos e semânticos;
- integração com `filter`/`mutate`.

### Mudança em operações

Validar:

- resultado tabular esperado;
- metadados relevantes;
- casos de borda;
- regressões em pipelines compostos.

---

## 9. Regras de estilo editorial para arquivos Markdown criados por agentes

Quando criar documentação neste repositório:

- use títulos informativos;
- prefira hierarquia de seções clara e estável;
- escreva com tom técnico, sóbrio e profissional;
- evite floreio promocional;
- use listas quando elas melhorarem escaneabilidade;
- inclua exemplos concretos quando a seção tratar de comportamento operacional;
- mantenha português técnico claro ou inglês técnico claro, mas evite mistura confusa dentro da mesma seção.

---

## 10. Checklist operacional para agentes

Antes de encerrar uma tarefa, confirme:

- [ ] a mudança resolve exatamente o pedido;
- [ ] o escopo permaneceu controlado;
- [ ] o código segue o padrão local;
- [ ] testes relevantes foram executados ou a limitação foi explicitada;
- [ ] documentação foi atualizada, se necessário;
- [ ] mensagens de erro e ajuda continuam claras;
- [ ] a alteração preserva a ergonomia Unix da ferramenta.

---

## 11. Anti-padrões a evitar

- alterar código sem ler os testes relacionados;
- mudar contratos públicos sem atualizar documentação;
- introduzir dependências novas por conveniência pontual;
- esconder comportamento complexo atrás de helpers genéricos demais;
- misturar responsabilidades de parsing, execução e serialização;
- corrigir um bug específico com refactor desnecessariamente amplo;
- enfraquecer precisão semântica em nome de “flexibilidade”.

---

## 12. Resumo executivo para agentes

Se precisar de uma versão curta das expectativas:

1. entenda o fluxo CLI → leitura → operação → escrita;
2. mude o mínimo necessário, com máxima clareza;
3. preserve compatibilidade, especialmente em pipelines;
4. escreva testes proporcionais ao impacto;
5. atualize docs quando o comportamento público mudar;
6. entregue mudanças tecnicamente sólidas, legíveis e revisáveis.
