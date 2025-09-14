# Arquivo CSV -> PostgreSQL Migrator (MVP)

## Estrutura
- .env.example
- migrator.py
- server.js
- logger.js
- package.json
- datacsv\ (seus CSVs)
- backend\scripts\ (sql gerados)
- backend\logs\ (logs do backend)

## Pré-requisitos
- Python 3.8+ com psycopg2 e python-dotenv
  - Ex: `pip install psycopg2-binary python-dotenv`
- Node.js 16+ (npm)
  - `npm install` no diretório do projeto

## Setup rápido (Windows)
1. Copie `.env.example` para `.env.example` e preencha as variáveis (POSTGRES_*, BASE_DIR, etc).
2. Instale dependências Python:
   - `pip install psycopg2-binary python-dotenv`
3. Instale dependências Node:
   - `npm install`
4. Inicie o backend:
   - `npm start` (ou `node server.js`)

## Endpoints principais
- `POST /api/import` — inicia importação. JSON opcional: `{ "file": "nome.csv" }`
- `GET /api/status` — retorna se está rodando e últimos eventos
- `GET /api/logs/stream` — Server-Sent Events (SSE) com eventos em tempo real
- `GET /api/logs` — baixa o log do backend

## Execução manual (opcional)
Você ainda pode rodar o `migrator.py` diretamente:
`python migrator.py --env-file D:\Robson\Projetos\Cascavel\.env.example`
ou processar só um arquivo:
`python migrator.py --env-file .env.example --file sample.csv`

## Frontend (futuro)
Planeje um frontend React:
- Biblioteca UI: Material UI (MUI) — componente minimalista, responsivo.
- Google Fonts & Icons.
- Conectar via REST/SSE para iniciar import e acompanhar progresso em tempo real.
- SEO: conteúdo estático, metatags, SSR/SSG (Next.js se quiser SSR).

## Notas de produção
- Para produção, adicionar autenticação (API key/JWT) no backend.
- Implementar persistência de eventos (ex.: banco ou arquivo rotativo) se necessário.
- Para arquivos grandes: considerar chunked inserts / COPY FROM CSV para performance. Aqui mantivemos INSERTs linha a linha por auditabilidade.
