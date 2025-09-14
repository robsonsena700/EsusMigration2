// server.js
const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const logger = require('./logger');
const { Client } = require('pg');

dotenv.config({ path: path.join(process.env.BASE_DIR || __dirname, '.env') });

const app = express();
app.use(cors());
app.use(express.json());

const PY_CMD = process.env.PYTHON_COMMAND || 'python';
const BASE_DIR = process.env.BASE_DIR || path.join(__dirname);
const MIGRATOR = path.join(BASE_DIR, 'migrator.py');
const CSV_DIR = path.join(BASE_DIR, 'datacsv');

let currentProcess = null;
let lastEvents = []; // small in-memory rolling buffer of events (for status/history)
const MAX_EVENTS = 500;

function pushEvent(e) {
  lastEvents.push(e);
  if (lastEvents.length > MAX_EVENTS) lastEvents.shift();
}

// Estado da migração
let migrationState = {
  status: 'idle', // idle, running, paused, completed, error
  progress: {
    current: 0,
    total: 0,
    currentFile: '',
    processedRecords: 0,
    errors: 0
  },
  selectedFiles: []
};

// Endpoint para iniciar import (opcional receber { file: 'nome.csv' })
app.post('/api/import', (req, res) => {
  const { file, co_municipio } = req.body || {};

  if (currentProcess) {
    return res.status(409).json({ status: 'busy', message: 'Um processo já está em execução.' });
  }

  const args = [MIGRATOR, '--env-file', path.join(BASE_DIR, '.env')];
  if (file) args.push('--file', file);
  if (co_municipio) args.push('--co-municipio', co_municipio);

  logger.info(`Iniciando migrator.py com args: ${args.join(' ')}`);

  // Carregar variáveis do .env para o processo Python
  const envPath = path.join(BASE_DIR, '.env');
  const envVars = { ...process.env };
  
  // Recarregar .env para garantir que as variáveis estão atualizadas
  if (fs.existsSync(envPath)) {
    const envContent = fs.readFileSync(envPath, 'utf8');
    envContent.split('\n').forEach(line => {
      const [key, ...valueParts] = line.split('=');
      if (key && valueParts.length > 0) {
        envVars[key.trim()] = valueParts.join('=').trim();
      }
    });
  }

  currentProcess = spawn(PY_CMD, args, { cwd: BASE_DIR, env: envVars });

  // stream stdout
  currentProcess.stdout.on('data', (data) => {
    const text = data.toString();
    text.split(/\r?\n/).filter(Boolean).forEach(line => {
      if (line.startsWith('EVENT:')) {
        try {
          const jsonPart = line.substring(6);
          const ev = JSON.parse(jsonPart);
          pushEvent({ ts: Date.now(), from: 'migrator', ev });
        } catch (err) {
          pushEvent({ ts: Date.now(), from: 'migrator', ev: { type: 'parse_error', raw: line }});
        }
      } else {
        pushEvent({ ts: Date.now(), from: 'migrator', ev: { type: 'stdout', message: line }});
      }
      logger.info(line);
    });
  });

  currentProcess.stderr.on('data', (data) => {
    const text = data.toString();
    text.split(/\r?\n/).filter(Boolean).forEach(line => {
      pushEvent({ ts: Date.now(), from: 'migrator', ev: { type: 'stderr', message: line }});
      logger.error(line);
    });
  });

  currentProcess.on('exit', (code, signal) => {
    pushEvent({ ts: Date.now(), from: 'migrator', ev: { type: 'exit', code, signal }});
    logger.info(`migrator.py finalizado (code=${code} signal=${signal})`);
    currentProcess = null;
  });

  res.json({ status: 'started' });
});

// Endpoints para migração
app.post('/api/migration/start', (req, res) => {
  const { files } = req.body || {};

  if (currentProcess) {
    return res.status(409).json({ status: 'busy', message: 'Um processo já está em execução.' });
  }

  if (!files || files.length === 0) {
    return res.status(400).json({ status: 'error', message: 'Nenhum arquivo selecionado.' });
  }

  // Atualizar estado da migração
  migrationState.status = 'running';
  migrationState.selectedFiles = files;
  migrationState.progress = {
    current: 0,
    total: files.length,
    currentFile: files[0] || '',
    processedRecords: 0,
    errors: 0
  };

  // Processar arquivos sequencialmente
  processNextFile(0, files, res);
});

function processNextFile(index, files, res) {
  if (index >= files.length) {
    migrationState.status = 'completed';
    migrationState.progress.currentFile = '';
    return;
  }

  const currentFile = files[index];
  migrationState.progress.current = index + 1;
  migrationState.progress.currentFile = currentFile;

  const args = [MIGRATOR, '--env-file', path.join(BASE_DIR, '.env'), '--file', currentFile];
  logger.info(`Processando arquivo ${index + 1}/${files.length}: ${currentFile}`);

  // Carregar variáveis do .env para o processo Python
  const envPath = path.join(BASE_DIR, '.env');
  const envVars = { ...process.env };
  
  if (fs.existsSync(envPath)) {
    const envContent = fs.readFileSync(envPath, 'utf8');
    envContent.split('\n').forEach(line => {
      const [key, ...valueParts] = line.split('=');
      if (key && valueParts.length > 0) {
        envVars[key.trim()] = valueParts.join('=').trim();
      }
    });
  }

  currentProcess = spawn(PY_CMD, args, { cwd: BASE_DIR, env: envVars });

  currentProcess.stdout.on('data', (data) => {
    const text = data.toString();
    text.split(/\r?\n/).filter(Boolean).forEach(line => {
      if (line.startsWith('EVENT:')) {
        try {
          const jsonPart = line.substring(6);
          const ev = JSON.parse(jsonPart);
          pushEvent({ ts: Date.now(), from: 'migrator', ev });
          
          // Atualizar progresso baseado nos eventos
          if (ev.type === 'progress') {
            migrationState.progress.processedRecords += ev.records || 0;
          }
        } catch (err) {
          pushEvent({ ts: Date.now(), from: 'migrator', ev: { type: 'parse_error', raw: line }});
        }
      } else {
        pushEvent({ ts: Date.now(), from: 'migrator', ev: { type: 'stdout', message: line }});
      }
      logger.info(line);
    });
  });

  currentProcess.stderr.on('data', (data) => {
    const text = data.toString();
    text.split(/\r?\n/).filter(Boolean).forEach(line => {
      pushEvent({ ts: Date.now(), from: 'migrator', ev: { type: 'stderr', message: line }});
      logger.error(line);
      migrationState.progress.errors++;
    });
  });

  currentProcess.on('exit', (code, signal) => {
    pushEvent({ ts: Date.now(), from: 'migrator', ev: { type: 'exit', code, signal, file: currentFile }});
    logger.info(`Arquivo ${currentFile} processado (code=${code} signal=${signal})`);
    currentProcess = null;

    if (code === 0) {
      // Processar próximo arquivo
      setTimeout(() => processNextFile(index + 1, files, res), 1000);
    } else {
      // Erro no processamento
      migrationState.status = 'error';
      migrationState.progress.currentFile = '';
    }
  });

  // Responder apenas na primeira chamada
  if (index === 0) {
    res.json({ status: 'started', message: `Iniciando processamento de ${files.length} arquivo(s)` });
  }
}

app.get('/api/migration/status', (req, res) => {
  res.json(migrationState);
});

app.post('/api/migration/pause', (req, res) => {
  if (currentProcess) {
    currentProcess.kill('SIGTERM');
    migrationState.status = 'paused';
    res.json({ status: 'paused', message: 'Migração pausada' });
  } else {
    res.status(400).json({ status: 'error', message: 'Nenhum processo em execução' });
  }
});

app.post('/api/migration/resume', (req, res) => {
  if (migrationState.status === 'paused') {
    // Retomar do arquivo atual
    const currentIndex = migrationState.progress.current - 1;
    processNextFile(currentIndex, migrationState.selectedFiles, res);
    migrationState.status = 'running';
    res.json({ status: 'resumed', message: 'Migração retomada' });
  } else {
    res.status(400).json({ status: 'error', message: 'Migração não está pausada' });
  }
});

app.post('/api/migration/reset', (req, res) => {
  if (currentProcess) {
    currentProcess.kill('SIGTERM');
    currentProcess = null;
  }
  
  migrationState = {
    status: 'idle',
    progress: {
      current: 0,
      total: 0,
      currentFile: '',
      processedRecords: 0,
      errors: 0
    },
    selectedFiles: []
  };
  
  res.json({ status: 'reset', message: 'Migração resetada' });
});

// SSE endpoint para stream de eventos em tempo real
app.get('/api/logs/stream', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive'
  });

  // envia últimos eventos para o cliente logo na conexão
  res.write(`data: ${JSON.stringify({ type: 'history', events: lastEvents })}\n\n`);

  const onEvent = (ev) => {
    try {
      res.write(`data: ${JSON.stringify(ev)}\n\n`);
    } catch (e) {
      // ignore
    }
  };

  // pequena função para enviar novos eventos do buffer (polling)
  const interval = setInterval(() => {
    if (lastEvents.length) {
      const ev = lastEvents[lastEvents.length - 1];
      onEvent(ev);
    }
  }, 1000);

  // fechar
  req.on('close', () => {
    clearInterval(interval);
  });
});

// status simples
app.get('/api/status', (req, res) => {
  res.json({
    running: !!currentProcess,
    lastEvents: lastEvents.slice(-20)
  });
});

// health check endpoint
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    uptime: process.uptime()
  });
});

// endpoint para logs em formato JSON
app.get('/api/logs', (req, res) => {
  const limit = parseInt(req.query.limit) || 100;
  const logFile = path.join(process.env.LOG_DIR || path.join(BASE_DIR, 'backend', 'logs'), 'app.log');
  
  if (fs.existsSync(logFile)) {
    try {
      const logContent = fs.readFileSync(logFile, 'utf8');
      const lines = logContent.split('\n').filter(line => line.trim());
      
      // Pegar as últimas 'limit' linhas
      const recentLines = lines.slice(-limit);
      
      // Converter para formato JSON esperado pelo frontend
      const logs = recentLines.map((line, index) => {
        // Parse do formato: "2025-09-13T15:06:35.627Z [INFO] Backend iniciado na porta 3000"
        const match = line.match(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)\s+\[(\w+)\]\s+(.+)$/);
        
        if (match) {
          return {
            timestamp: match[1],
            level: match[2].toLowerCase(),
            message: match[3],
            source: 'backend'
          };
        } else {
          // Fallback para linhas que não seguem o padrão
          return {
            timestamp: new Date().toISOString(),
            level: 'info',
            message: line,
            source: 'backend'
          };
        }
      });
      
      res.json(logs);
    } catch (error) {
      logger.error(`Erro ao ler arquivo de log: ${error.message}`);
      res.status(500).json({ error: 'Erro ao ler logs' });
    }
  } else {
    res.json([]); // Retorna array vazio se não há logs
  }
});

// download logs como arquivo (novo endpoint)
app.get('/api/logs/download', (req, res) => {
  const logFile = path.join(process.env.LOG_DIR || path.join(BASE_DIR, 'backend', 'logs'), 'app.log');
  if (fs.existsSync(logFile)) {
    res.sendFile(logFile);
  } else {
    res.status(404).send('Log não encontrado.');
  }
});

// endpoint para listar arquivos CSV disponíveis
app.get('/api/csv/files', (req, res) => {
  try {
    if (!fs.existsSync(CSV_DIR)) {
      return res.json([]);
    }
    
    const files = fs.readdirSync(CSV_DIR)
      .filter(file => file.toLowerCase().endsWith('.csv'))
      .map(file => {
        const filePath = path.join(CSV_DIR, file);
        const stats = fs.statSync(filePath);
        return {
          name: file,
          size: stats.size,
          modified: stats.mtime.toISOString()
        };
      })
      .sort((a, b) => new Date(b.modified) - new Date(a.modified));
    
    res.json(files);
  } catch (error) {
    logger.error(`Erro ao listar arquivos CSV: ${error.message}`);
    res.status(500).json({ error: 'Erro ao listar arquivos CSV' });
  }
});

// endpoint para obter configurações atuais
app.get('/api/config', (req, res) => {
  try {
    const envPath = path.join(BASE_DIR, '.env');
    const config = {};
    
    if (fs.existsSync(envPath)) {
      const envContent = fs.readFileSync(envPath, 'utf8');
      const lines = envContent.split('\n');
      
      lines.forEach(line => {
        const trimmed = line.trim();
        if (trimmed && !trimmed.startsWith('#')) {
          const [key, ...valueParts] = trimmed.split('=');
          if (key && valueParts.length > 0) {
            config[key] = valueParts.join('=');
          }
        }
      });
    }
    
    // Retornar apenas as configurações relevantes para o frontend
    const frontendConfig = {
      POSTGRES_DB: config.POSTGRES_DB || config.DB_NAME || '',
      POSTGRES_USER: config.POSTGRES_USER || config.DB_USER || '',
      POSTGRES_PASSWORD: config.POSTGRES_PASSWORD || config.DB_PASSWORD || '',
      POSTGRES_HOST: config.POSTGRES_HOST || config.DB_HOST || 'localhost',
      POSTGRES_PORT: config.POSTGRES_PORT || config.DB_PORT || '5432',
      TABLE_NAME: config.TABLE_NAME || 'public.tl_cds_cad_individual'
    };
    
    res.json(frontendConfig);
  } catch (error) {
    logger.error(`Erro ao obter configurações: ${error.message}`);
    res.status(500).json({ error: 'Erro ao obter configurações' });
  }
});

// endpoint para listar tabelas disponíveis para migração
app.get('/api/migration/tables', (req, res) => {
  try {
    const availableTables = [
      {
        name: 'public.tl_cds_cad_individual',
        displayName: 'public.tl_cds_cad_individual',
        description: 'Tabela tl_cds_cad_individual para cadastro de indivíduos',
        isDefault: true
      },
      {
        name: 'public.tb_cds_cad_individual',
        displayName: 'public.tb_cds_cad_individual',
        description: 'Tabela tb_cds_cad_individual original do sistema',
        isDefault: false
      }
    ];
    
    res.json(availableTables);
  } catch (error) {
    logger.error(`Erro ao listar tabelas: ${error.message}`);
    res.status(500).json({ error: 'Erro ao listar tabelas disponíveis' });
  }
});

// endpoint para salvar configurações
app.post('/api/config', (req, res) => {
  try {
    const {
      POSTGRES_DB,
      POSTGRES_USER,
      POSTGRES_PASSWORD,
      POSTGRES_HOST,
      POSTGRES_PORT,
      TABLE_NAME
    } = req.body;
    
    // Validar campos obrigatórios
    if (!POSTGRES_DB || !POSTGRES_USER || !POSTGRES_PASSWORD) {
      return res.status(400).json({ 
        error: 'Campos obrigatórios: POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD' 
      });
    }
    
    const envPath = path.join(BASE_DIR, '.env');
    let envContent = '';
    
    // Ler arquivo existente se houver
    if (fs.existsSync(envPath)) {
      envContent = fs.readFileSync(envPath, 'utf8');
    }
    
    // Atualizar ou adicionar configurações
    const configMap = new Map();
    
    // Parse do conteúdo existente
    const lines = envContent.split('\n');
    lines.forEach(line => {
      const trimmed = line.trim();
      if (trimmed && !trimmed.startsWith('#')) {
        const [key, ...valueParts] = trimmed.split('=');
        if (key && valueParts.length > 0) {
          configMap.set(key, valueParts.join('='));
        }
      }
    });
    
    // Atualizar com novas configurações
    configMap.set('POSTGRES_DB', POSTGRES_DB);
    configMap.set('POSTGRES_USER', POSTGRES_USER);
    configMap.set('POSTGRES_PASSWORD', POSTGRES_PASSWORD);
    configMap.set('POSTGRES_HOST', POSTGRES_HOST || 'localhost');
    configMap.set('POSTGRES_PORT', POSTGRES_PORT || '5432');
    configMap.set('TABLE_NAME', TABLE_NAME || 'public.tl_cds_cad_individual');
    
    // Gerar novo conteúdo do arquivo
    const newEnvContent = Array.from(configMap.entries())
      .map(([key, value]) => `${key}=${value}`)
      .join('\n');
    
    // Salvar arquivo
    fs.writeFileSync(envPath, newEnvContent, 'utf8');
    
    logger.info('Configurações atualizadas com sucesso');
    res.json({ message: 'Configurações salvas com sucesso' });
    
  } catch (error) {
    logger.error(`Erro ao salvar configurações: ${error.message}`);
    res.status(500).json({ error: 'Erro ao salvar configurações' });
  }
});

// endpoint para atualizar tabela de migração
app.post('/api/migration/table', (req, res) => {
  try {
    const { tableName } = req.body;
    
    if (!tableName) {
      return res.status(400).json({ error: 'Nome da tabela é obrigatório' });
    }
    
    // Validar se a tabela está na lista de tabelas permitidas
    const allowedTables = ['public.tl_cds_cad_individual', 'public.tb_cds_cad_individual'];
    if (!allowedTables.includes(tableName)) {
      return res.status(400).json({ error: 'Tabela não permitida' });
    }
    
    const envPath = path.join(BASE_DIR, '.env');
    let envContent = '';
    
    // Ler arquivo .env existente
    if (fs.existsSync(envPath)) {
      envContent = fs.readFileSync(envPath, 'utf8');
    }
    
    // Atualizar ou adicionar TABLE_NAME
    const lines = envContent.split('\n');
    let tableNameUpdated = false;
    
    const updatedLines = lines.map(line => {
      if (line.startsWith('TABLE_NAME=')) {
        tableNameUpdated = true;
        return `TABLE_NAME=${tableName}`;
      }
      return line;
    });
    
    // Se TABLE_NAME não existia, adicionar
    if (!tableNameUpdated) {
      updatedLines.push(`TABLE_NAME=${tableName}`);
    }
    
    fs.writeFileSync(envPath, updatedLines.join('\n'));
    
    logger.info(`Tabela de migração atualizada para: ${tableName}`);
    res.json({ message: 'Tabela de migração atualizada com sucesso', tableName });
  } catch (error) {
    logger.error(`Erro ao atualizar tabela de migração: ${error.message}`);
    res.status(500).json({ error: 'Erro ao atualizar tabela de migração' });
  }
});

// Endpoint para testar conexão com banco
app.post('/api/database/test', async (req, res) => {
  const { POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD } = req.body;
  
  const client = new Client({
    host: POSTGRES_HOST,
    port: POSTGRES_PORT,
    database: POSTGRES_DB,
    user: POSTGRES_USER,
    password: POSTGRES_PASSWORD,
  });

  try {
    await client.connect();
    await client.query('SELECT 1');
    await client.end();
    res.json({ status: 'success', message: 'Conexão estabelecida com sucesso' });
  } catch (error) {
    logger.error('Erro ao testar conexão:', error);
    res.status(500).json({ status: 'error', message: error.message });
  }
});

// Endpoint para verificar status do banco
app.get('/api/database/status', async (req, res) => {
  try {
    const client = new Client({
      host: process.env.POSTGRES_HOST,
      port: process.env.POSTGRES_PORT,
      database: process.env.POSTGRES_DB,
      user: process.env.POSTGRES_USER,
      password: process.env.POSTGRES_PASSWORD,
    });
    
    await client.connect();
    await client.query('SELECT 1');
    await client.end();
    res.json({ status: 'online' });
  } catch (error) {
    res.status(500).json({ status: 'offline', error: error.message });
  }
});

// Arquivo para armazenar configurações salvas
const SAVED_CONFIGS_FILE = path.join(BASE_DIR, 'saved_configs.json');

// Função para carregar configurações salvas
function loadSavedConfigs() {
  try {
    if (fs.existsSync(SAVED_CONFIGS_FILE)) {
      const data = fs.readFileSync(SAVED_CONFIGS_FILE, 'utf8');
      return JSON.parse(data);
    }
  } catch (error) {
    logger.error('Erro ao carregar configurações salvas:', error);
  }
  return [];
}

// Função para salvar configurações
function saveSavedConfigs(configs) {
  try {
    fs.writeFileSync(SAVED_CONFIGS_FILE, JSON.stringify(configs, null, 2));
    return true;
  } catch (error) {
    logger.error('Erro ao salvar configurações:', error);
    return false;
  }
}

// Endpoint para listar configurações salvas
app.get('/api/config/saved', (req, res) => {
  const configs = loadSavedConfigs();
  res.json(configs);
});

// Endpoint para salvar uma nova configuração
app.post('/api/config/save', (req, res) => {
  const { name, config } = req.body;
  
  if (!name || !config) {
    return res.status(400).json({ error: 'Nome e configuração são obrigatórios' });
  }
  
  const configs = loadSavedConfigs();
  const newConfig = {
    id: Date.now().toString(),
    name,
    config,
    createdAt: new Date().toISOString()
  };
  
  configs.push(newConfig);
  
  if (saveSavedConfigs(configs)) {
    res.json({ success: true, config: newConfig });
  } else {
    res.status(500).json({ error: 'Erro ao salvar configuração' });
  }
});

// Endpoint para carregar uma configuração específica
app.get('/api/config/load/:id', (req, res) => {
  const { id } = req.params;
  const configs = loadSavedConfigs();
  const config = configs.find(c => c.id === id);
  
  if (config) {
    res.json(config);
  } else {
    res.status(404).json({ error: 'Configuração não encontrada' });
  }
});

// Endpoint para deletar uma configuração
app.delete('/api/config/saved/:id', (req, res) => {
  const { id } = req.params;
  const configs = loadSavedConfigs();
  const filteredConfigs = configs.filter(c => c.id !== id);
  
  if (saveSavedConfigs(filteredConfigs)) {
    res.json({ success: true });
  } else {
    res.status(500).json({ error: 'Erro ao deletar configuração' });
  }
});

// Endpoint para upload de CSV
const multer = require('multer');
const upload = multer({ dest: CSV_DIR });

// Importar rotas de logs do PostgreSQL
const postgresqlLogsRoutes = require('./backend/routes/postgresql-logs');
app.use('/api/postgresql-logs', postgresqlLogsRoutes);

app.post('/api/csv/upload', upload.single('csvFile'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'Nenhum arquivo enviado' });
  }
  
  const originalName = req.file.originalname;
  const newPath = path.join(CSV_DIR, originalName);
  
  try {
    // Verificar se o arquivo de destino já existe e removê-lo
    if (fs.existsSync(newPath)) {
      fs.unlinkSync(newPath);
      logger.info(`Arquivo existente removido: ${originalName}`);
    }
    
    // Mover o arquivo temporário para o destino final
    fs.renameSync(req.file.path, newPath);
    logger.info(`Arquivo CSV enviado: ${originalName}`);
    res.json({ success: true, filename: originalName });
  } catch (error) {
    logger.error('Erro ao mover arquivo:', error);
    
    // Tentar limpar o arquivo temporário em caso de erro
    try {
      if (fs.existsSync(req.file.path)) {
        fs.unlinkSync(req.file.path);
      }
    } catch (cleanupError) {
      logger.error('Erro ao limpar arquivo temporário:', cleanupError);
    }
    
    res.status(500).json({ error: 'Erro ao processar arquivo: ' + error.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  logger.info(`Backend iniciado na porta ${PORT}`);
  logger.info(`MIGRATOR: ${MIGRATOR}`);
});
