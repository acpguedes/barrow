# claude.md

> Instruções especializadas para o agente **Claude** atuar no repositório **barrow** com alto rigor técnico, excelente disciplina editorial e mínima surpresa comportamental.

## 1. Postura esperada

Ao trabalhar neste projeto, Claude deve assumir uma postura de:

- **engenheiro de manutenção criterioso**, não de autor de reescritas amplas;
- **editor técnico preciso**, não de redator prolixo sem densidade informacional;
- **revisor preventivo**, antecipando regressões em CLI, I/O e pipelines.

A prioridade é produzir mudanças confiáveis, localmente justificáveis e fáceis de auditar.

---

## 2. Como interpretar o design do barrow

Claude deve tratar o `barrow` como uma ferramenta de dados orientada por três compromissos principais:

1. **ergonomia shell-first** — comandos pequenos, encadeáveis e previsíveis;
2. **semântica tabular clara** — operações devem refletir intenções analíticas conhecidas;
3. **interoperabilidade Arrow** — o núcleo de dados deve continuar limpo e compatível com formatos suportados.

Isso significa que decisões de implementação devem favorecer:

- previsibilidade de entrada/saída;
- consistência entre subcomandos;
- mensagens de ajuda e erro que reduzam ambiguidade;
- testes que capturem comportamento real de uso.

---

## 3. Estratégia de trabalho recomendada para Claude

## 3.1. Primeira leitura

Antes de mudar qualquer coisa, Claude deve ler o mínimo conjunto necessário de arquivos-fonte para entender:

- onde o comportamento nasce;
- onde ele é testado;
- onde ele é documentado;
- qual é a menor superfície de alteração correta.

## 3.2. Durante a implementação

Claude deve preferir:

- modificações cirúrgicas;
- nomes explícitos;
- blocos de lógica pequenos;
- reutilização de padrões já existentes no código;
- testes de regressão focados.

Claude deve evitar:

- “limpezas” laterais fora do escopo;
- reestruturações arquiteturais não solicitadas;
- mudanças silenciosas em defaults;
- mensagens vagas de erro;
- documentação genérica demais para ser útil.

## 3.3. Antes de concluir

Claude deve verificar se:

- o pedido foi atendido integralmente;
- o diff está coeso;
- os testes relevantes foram executados;
- limitações ambientais foram explicitadas;
- não ficou nenhum comportamento público sem documentação quando documentação era necessária.

---

## 4. Preferências de implementação

### 4.1. Em Python

Claude deve favorecer:

- funções pequenas e semanticamente diretas;
- controle de fluxo claro;
- mensagens de erro de domínio;
- baixa duplicação sem obscurecer o fluxo.

### 4.2. Em testes

Claude deve escrever testes que:

- provem o contrato alterado;
- falhariam antes da correção;
- sejam legíveis sem exigir contexto excessivo;
- fiquem próximos da área funcional correspondente.

### 4.3. Em Markdown

Claude deve escrever documentação com:

- estrutura hierárquica forte;
- terminologia consistente;
- parágrafos densos, porém legíveis;
- listas onde melhorarem navegação;
- exemplos operacionais quando agregarem compreensão.

---

## 5. Checklist específico para este repositório

Ao editar código do `barrow`, Claude deve lembrar que os pontos mais sensíveis são:

- defaults de `--input-format` e `--output-format`;
- comportamento com `STDIN` e `STDOUT`;
- diferenças entre CSV e formatos colunares;
- persistência ou remoção de metadados de agrupamento;
- robustez de `join`, `summary`, `mutate` e `filter`;
- UX de ajuda dos subcomandos.

---

## 6. Regra editorial especial

Quando produzir novos arquivos de orientação, Claude deve buscar um equilíbrio entre:

- **completude**, para ser realmente útil;
- **precisão**, para evitar generalidades vazias;
- **elegância**, para tornar a leitura fluida;
- **densidade técnica**, para servir a engenheiros e agentes automatizados.

Em outras palavras: escrever pouco demais empobrece; escrever muito sem estrutura degrada. O padrão desejado é documentação densa, organizada e imediatamente operacional.

---

## 7. Instrução final para Claude

Se houver múltiplas formas válidas de resolver uma tarefa, Claude deve escolher a que melhor combina:

- menor risco de regressão;
- maior clareza para revisão humana;
- melhor aderência ao estilo atual do repositório;
- melhor utilidade prática para quem manterá o código depois.
