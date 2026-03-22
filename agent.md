# agent.md

> Manual compacto para um agente individual atuar com alta precisão no repositório **barrow**.
>
> Este arquivo é um complemento operacional ao `agents.md`: menos amplo, mais orientado à execução. Use-o como protocolo de decisão rápida antes, durante e depois de qualquer modificação.

## 1. Modelo mental do projeto

Pense no `barrow` como uma cadeia de responsabilidade composta por quatro blocos:

1. **CLI** recebe intenção do usuário;
2. **I/O** materializa tabelas a partir de arquivos ou `STDIN`;
3. **operações** transformam tabelas Arrow de forma declarativa;
4. **writer** serializa o resultado para arquivo ou `STDOUT`.

Toda alteração deve reforçar, e não confundir, essa separação.

---

## 2. Perguntas obrigatórias antes de codar

Antes de editar qualquer arquivo, responda mentalmente:

- Qual é o contrato observável que está sendo mudado?
- O impacto é de **CLI**, **I/O**, **expressão**, **operação** ou **documentação**?
- Existe teste cobrindo algo parecido?
- A mudança pode quebrar pipeline shell já existente?
- O comportamento deve ser documentado publicamente?

Se alguma dessas respostas estiver nebulosa, investigue antes de editar.

---

## 3. Protocolo de implementação

## 3.1. Se a mudança for na CLI

- preserve nomes de comandos e semântica de flags;
- mantenha `help`, `description` e `epilog` coerentes;
- trate defaults de formato com cuidado;
- valide combinações inválidas cedo.

## 3.2. Se a mudança for em operações

- não introduza conhecimento de argparse na camada de operações;
- mantenha entradas e saídas conceitualmente puras;
- cubra edge cases com testes pequenos e claros.

## 3.3. Se a mudança for em I/O

- teste `STDIN`/`STDOUT`;
- teste delimitadores;
- teste inferência de formato;
- confirme que formatos intermediários continuam funcionais.

## 3.4. Se a mudança for em parser/expressões

- teste sucesso e falha;
- preserve mensagens legíveis;
- não amplie gramática de modo ambíguo.

---

## 4. Critérios de excelência

Uma mudança excelente neste projeto tende a ser:

- curta no diff;
- forte em semântica;
- coberta por testes úteis;
- coerente com a documentação;
- fácil de revisar linha por linha.

### Sinais de baixa qualidade

- abstração nova para esconder poucas linhas simples;
- refactor paralelo sem relação direta com o pedido;
- dependência nova sem ganho claro;
- documentação desatualizada após mudar comportamento público;
- ausência de teste para bug corrigido.

---

## 5. Sequência de validação recomendada

Sempre que possível, execute:

```bash
make format
make lint
make test
```

Quando o escopo for localizado, complemente com `pytest` direcionado para os módulos alterados.

---

## 6. Padrão de escrita de documentação

Ao produzir Markdown para este projeto:

- organize por propósito, não por improviso;
- escreva com precisão técnica;
- use exemplos concretos;
- mantenha o texto elegante, mas funcional;
- evite redundâncias infladas.

---

## 7. Regra final

Se estiver em dúvida entre uma solução “engenhosa” e uma solução “óbvia, legível e robusta”, escolha a segunda.
