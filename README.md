# Controle de Impressao 3D

Aplicativo web em Flask para administrar uma operacao de impressao 3D com foco em comercial, producao e estoque.

Ele esta sendo estruturado para evoluir bem em celulares, com layout responsivo e base de PWA para futura instalacao em Android e iPhone.

## O que o sistema faz agora

- dashboard com indicadores operacionais e comerciais
- cadastro de clientes
- cadastro de fornecedores
- cadastro de representantes
- cadastro de lojas parceiras
- cadastro tecnico de impressoras com valor, vida util em horas, AMS, valor do kWh, status, depreciacao, energia por hora, custo por hora automatico e manutencao
- cadastro de secador de filamentos com marca, modelo, tipo, potencia, vida util, preco, depreciacao e custo em reais por hora
- cadastro de materiais com fornecedor, codigo interno, localizacao e estoque minimo
- cadastro de componentes com codigo interno sequencial, fabricante, compatibilidade, custos de compra, unidade de medida, localizacao e estoque minimo
- cadastro de orcamentos e pedidos
- controle de movimentacoes de estoque
- simulador de precificacao para impressao 3D
- base PWA com manifesto e service worker
- banco local SQLite criado automaticamente

## Estrutura atual dos modulos

- `Dashboard`: visao geral do negocio, alertas de estoque, agenda e pipeline
- `Cadastros`: base comercial com clientes, fornecedores, representantes e lojas
- `Impressoras`: base de maquinas com dados tecnicos, status e manutencao
- `Materiais`: catalogo de insumos com custo e estoque minimo
- `Movimentacoes`: entradas, perdas, ajustes e consumos manuais
- `Pedidos`: orcamentos, pedidos aprovados, producao e entrega
- `Precificacao`: simulador rapido de custo e preco sugerido

## Regras de custo usadas

O sistema calcula:

- custo de material por grama
- custo de energia por hora
- custo operacional por hora
- custo total
- preco sugerido com margem

Formula base:

```text
custo_material = peso_em_gramas * custo_por_grama
custo_energia = horas_de_impressao * custo_energia_hora
custo_operacional = horas_de_impressao * custo_operacional_hora
custo_total = custo_material + custo_energia + custo_operacional + custos_extras
preco_sugerido = custo_total / (1 - margem)
```

## Como rodar

1. Criar ambiente virtual:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Instalar dependencias:

```bash
pip install -r requirements.txt
```

3. Iniciar o aplicativo:

```bash
python3 app.py
```

4. Abrir no navegador:

```text
http://127.0.0.1:5000
```

## Publicacao

### Qual plataforma eu recomendo

Para este projeto, eu recomendo `Railway`.

Motivos:

- o app e `Flask`
- hoje ele usa `SQLite`
- ele salva uploads locais
- o Railway suporta `Volume`, que facilita manter banco e arquivos persistentes

### Sobre Vercel

O `Vercel` ate suporta Flask hoje, mas para este projeto eu nao recomendo como primeira hospedagem.

Motivo principal:

- o Vercel e melhor para apps mais estaticos ou backends em funcoes
- seu sistema precisa persistir banco local e uploads
- isso tende a ficar mais chato de manter depois da publicacao

### O que ja ficou pronto no projeto

- `gunicorn` para rodar em producao
- `railway.json` pronto para deploy
- `render.yaml` pronto caso voce queira testar Render
- banco configuravel por ambiente
- uploads configuraveis por ambiente
- rota propria para servir uploads
- `.gitignore` para nao subir banco local e arquivos temporarios

### Como publicar no Railway

1. Criar conta no GitHub.
2. Criar um repositorio e subir este projeto.
3. Criar conta no Railway.
4. No Railway, criar projeto a partir do repositorio do GitHub.
5. No servico, criar e anexar um `Volume`.
6. Definir o mount path do volume como:

```text
/app/data
```

7. Em `Variables`, adicionar:

```text
APP_STORAGE_DIR=/app/data
```

8. Em `Networking`, clicar em `Generate Domain`.
9. Abrir a URL publica no computador e no celular.

### O que fica persistido com isso

- banco SQLite em `/app/data/app.db`
- uploads em `/app/data/uploads/jobs`

### Manutencao depois da publicacao

Depois que estiver no GitHub:

- qualquer alteracao que voce fizer aqui e subir para o repositorio pode ser republicada
- o Railway consegue fazer deploy a cada push
- se quiser, depois eu tambem posso preparar um fluxo simples de backup do banco

## Proximas evolucoes recomendadas

- autenticacao de usuarios e niveis de permissao
- geracao de PDF para orcamentos e pedidos
- anexos de STL, 3MF e renders
- centro financeiro com contas a pagar e receber
- comissoes automaticas por representante
- indicadores de margem real por pedido
- OS de manutencao para impressoras e equipamentos
- icones proprios e ajustes finais para publicacao como PWA
