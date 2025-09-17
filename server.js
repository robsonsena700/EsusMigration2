// server.js
const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const { spawn, exec } = require('child_process');
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

// Endpoint de debug para verificar variáveis de ambiente
app.get('/api/debug/env', (req, res) => {
  res.json({
    POSTGRES_HOST: process.env.POSTGRES_HOST,
    POSTGRES_PORT: process.env.POSTGRES_PORT,
    POSTGRES_DB: process.env.POSTGRES_DB,
    POSTGRES_USER: process.env.POSTGRES_USER,
    POSTGRES_PASSWORD: process.env.POSTGRES_PASSWORD ? '***DEFINIDA***' : 'UNDEFINED',
    BASE_DIR: process.env.BASE_DIR,
    envPath: path.join(process.env.BASE_DIR || __dirname, '.env'),
    envExists: fs.existsSync(path.join(process.env.BASE_DIR || __dirname, '.env'))
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
      // Tabelas CDS principais
      {
        name: 'public.tl_cds_cad_individual',
        displayName: 'tl_cds_cad_individual',
        description: 'Tabela tl_cds_cad_individual para cadastro de indivíduos',
        isDefault: true,
        category: 'CDS'
      },
      {
        name: 'public.tb_cds_cad_individual',
        displayName: 'tb_cds_cad_individual',
        description: 'Tabela tb_cds_cad_individual original do sistema',
        isDefault: false,
        category: 'CDS'
      },
      // Tabelas FAT principais
      {
        name: 'public.tb_fat_cad_individual',
        displayName: 'tb_fat_cad_individual',
        description: 'Tabela FAT para cadastro individual',
        isDefault: false,
        category: 'FAT'
      },
      {
        name: 'public.tb_fat_cidadao',
        displayName: 'tb_fat_cidadao',
        description: 'Tabela FAT para dados do cidadão',
        isDefault: false,
        category: 'FAT'
      },
      {
        name: 'public.tb_fat_cidadao_pec',
        displayName: 'tb_fat_cidadao_pec',
        description: 'Tabela FAT para cidadão PEC',
        isDefault: false,
        category: 'FAT'
      },
      {
        name: 'public.tb_cidadao',
        displayName: 'tb_cidadao',
        description: 'Tabela principal de cidadão',
        isDefault: false,
        category: 'Principal'
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
    const allowedTables = [
      'public.tl_cds_cad_individual', 'public.tb_cds_cad_individual',
      'public.tb_fat_cad_individual', 'public.tb_fat_cidadao', 'public.tb_fat_cidadao_pec',
      'public.tb_cidadao'
    ];
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

// Endpoint para gerar SQL para múltiplas tabelas
// Estado global para progresso da geração de SQL
let sqlGenerationState = {
  isGenerating: false,
  current: 0,
  total: 0,
  currentTable: '',
  results: [],
  errors: []
};

app.post('/api/migration/generate-multiple-sql', async (req, res) => {
  try {
    const { selectedTables, csvFiles } = req.body;
    
    // DEBUG: Log dos dados recebidos
    console.log('🔍 DEBUG - Dados recebidos no backend:', {
      selectedTables: selectedTables,
      csvFiles: csvFiles,
      body: req.body
    });
    
    if (!selectedTables || selectedTables.length === 0) {
      return res.status(400).json({ error: 'Pelo menos uma tabela deve ser selecionada' });
    }
    
    if (!csvFiles || csvFiles.length === 0) {
      return res.status(400).json({ error: 'Pelo menos um arquivo CSV deve ser fornecido' });
    }
    
    // Verificar se já está gerando
    if (sqlGenerationState.isGenerating) {
      return res.status(409).json({ error: 'Geração de SQL já está em andamento' });
    }
    
    const results = [];
    const errors = [];
    
    // Validar se as tabelas estão na lista de tabelas permitidas
    const allowedTables = [
      'public.tl_cds_cad_individual', 'public.tb_cds_cad_individual',
      'public.tb_fat_cad_individual', 'public.tb_fat_cidadao', 'public.tb_fat_cidadao_pec',
      'public.tb_cidadao'
    ];
    
    const invalidTables = selectedTables.filter(table => !allowedTables.includes(table));
    if (invalidTables.length > 0) {
      return res.status(400).json({ 
        error: 'Tabelas não permitidas encontradas', 
        invalidTables 
      });
    }
    
    // Inicializar estado da geração
    sqlGenerationState = {
      isGenerating: true,
      current: 0,
      total: selectedTables.length,
      currentTable: '',
      results: [],
      errors: []
    };
    
    const envPath = path.join(BASE_DIR, '.env');
    let originalEnvContent = '';
    
    if (fs.existsSync(envPath)) {
      originalEnvContent = fs.readFileSync(envPath, 'utf8');
    }
    
    // Processar cada tabela selecionada
    for (let i = 0; i < selectedTables.length; i++) {
      const tableName = selectedTables[i];
      
      // Atualizar progresso
      sqlGenerationState.current = i + 1;
      sqlGenerationState.currentTable = tableName;
      
      try {
        // Atualizar .env temporariamente para a tabela atual
        const lines = originalEnvContent.split('\n');
        let tableNameUpdated = false;
        
        const updatedLines = lines.map(line => {
          if (line.startsWith('TABLE_NAME=')) {
            tableNameUpdated = true;
            return `TABLE_NAME=${tableName}`;
          }
          return line;
        });
        
        if (!tableNameUpdated) {
          updatedLines.push(`TABLE_NAME=${tableName}`);
        }
        
        fs.writeFileSync(envPath, updatedLines.join('\n'));
        
        // Processar cada arquivo CSV para esta tabela
        for (const csvFile of csvFiles) {
          const csvPath = path.join(BASE_DIR, 'datacsv', csvFile);
          
          if (!fs.existsSync(csvPath)) {
            errors.push(`Arquivo CSV não encontrado: ${csvFile}`);
            continue;
          }
          
          // Executar migrator.py
          const migratorPath = path.join(BASE_DIR, 'migrator.py');
          const pythonCmd = process.env.PYTHON_COMMAND || 'python';
          const pythonCommand = `"${pythonCmd}" "${migratorPath}" --env-file "${envPath}" --file "${csvFile}" --table-name "${tableName}"`;
          
          await new Promise((resolve, reject) => {
            exec(pythonCommand, { cwd: BASE_DIR, maxBuffer: 10 * 1024 * 1024 }, (error, stdout, stderr) => {
              if (error) {
                errors.push(`Erro ao gerar SQL para ${tableName} com ${csvFile}: ${error.message}`);
                resolve();
                return;
              }
              
              // Verificar se o arquivo SQL foi gerado
              const sqlDir = path.join(BASE_DIR, 'backend', 'scripts');
              const baseName = path.basename(csvFile, '.csv');
              const tableSuffix = tableName.replace('public.', '').replace('.', '_');
              const expectedSqlFile = path.join(sqlDir, `${tableSuffix}_${baseName}.sql`);
              
              if (fs.existsSync(expectedSqlFile)) {
                const sqlContent = fs.readFileSync(expectedSqlFile, 'utf8');
                const stats = fs.statSync(expectedSqlFile);
                
                const result = {
                  tableName,
                  csvFile,
                  sqlFile: path.basename(expectedSqlFile),
                  sqlPath: expectedSqlFile,
                  fileSize: stats.size,
                  linesCount: sqlContent.split('\n').length
                };
                
                results.push(result);
                sqlGenerationState.results.push(result);
                
                logger.info(`SQL gerado com sucesso: ${expectedSqlFile}`);
              } else {
                const error = `Arquivo SQL não foi gerado para ${tableName} com ${csvFile}`;
                errors.push(error);
                sqlGenerationState.errors.push(error);
              }
              
              resolve();
            });
          });
        }
        
      } catch (error) {
        errors.push(`Erro ao processar tabela ${tableName}: ${error.message}`);
      }
    }
    
    // Restaurar .env original
    fs.writeFileSync(envPath, originalEnvContent);
    
    // Finalizar estado da geração
    sqlGenerationState.isGenerating = false;
    sqlGenerationState.currentTable = '';
    
    res.json({
      message: `Processamento concluído. ${results.length} arquivo(s) gerado(s)`,
      results,
      errors,
      totalGenerated: results.length,
      totalErrors: errors.length
    });
    
  } catch (error) {
    logger.error(`Erro ao gerar múltiplos SQLs: ${error.message}`);
    
    // Finalizar estado da geração em caso de erro
    sqlGenerationState.isGenerating = false;
    sqlGenerationState.currentTable = '';
    
    res.status(500).json({ error: 'Erro ao gerar múltiplos SQLs' });
  }
});

// Endpoint para obter status da geração de SQL em tempo real
app.get('/api/migration/sql-generation-status', (req, res) => {
  res.json(sqlGenerationState);
});

// Endpoint para gerar SQL sem executar migração
app.post('/api/migration/generate-sql', async (req, res) => {
  try {
    const { tableName, csvFile } = req.body;
    
    if (!tableName) {
      return res.status(400).json({ error: 'Nome da tabela é obrigatório' });
    }
    
    // Validar se a tabela está na lista de tabelas permitidas
    const allowedTables = [
      'public.tl_cds_cad_individual', 'public.tb_cds_cad_individual',
      'public.tb_fat_cad_individual', 'public.tb_fat_cidadao', 'public.tb_fat_cidadao_pec',
      'public.tb_cidadao'
    ];
    
    if (!allowedTables.includes(tableName)) {
      return res.status(400).json({ error: 'Tabela não permitida' });
    }
    
    // Atualizar .env temporariamente para a tabela especificada
    const envPath = path.join(BASE_DIR, '.env');
    let originalEnvContent = '';
    
    if (fs.existsSync(envPath)) {
      originalEnvContent = fs.readFileSync(envPath, 'utf8');
    }
    
    // Criar conteúdo temporário do .env
    const lines = originalEnvContent.split('\n');
    let tableNameUpdated = false;
    
    const updatedLines = lines.map(line => {
      if (line.startsWith('TABLE_NAME=')) {
        tableNameUpdated = true;
        return `TABLE_NAME=${tableName}`;
      }
      return line;
    });
    
    if (!tableNameUpdated) {
      updatedLines.push(`TABLE_NAME=${tableName}`);
    }
    
    fs.writeFileSync(envPath, updatedLines.join('\n'));
    
    // Executar migrator.py apenas para gerar SQL (sem conexão com banco)
    const migratorPath = path.join(BASE_DIR, 'migrator.py');
    const csvDir = path.join(BASE_DIR, 'datacsv');
    
    // Verificar se existe pelo menos um arquivo CSV
    let csvFiles = [];
    if (fs.existsSync(csvDir)) {
      csvFiles = fs.readdirSync(csvDir).filter(f => f.toLowerCase().endsWith('.csv'));
    }
    
    if (csvFiles.length === 0) {
      // Restaurar .env original
      fs.writeFileSync(envPath, originalEnvContent);
      return res.status(400).json({ error: 'Nenhum arquivo CSV encontrado para processar' });
    }
    
    // Usar o primeiro CSV disponível ou o especificado
    const targetCsv = csvFile || csvFiles[0];
    const csvPath = path.join(csvDir, targetCsv);
    
    if (!fs.existsSync(csvPath)) {
      // Restaurar .env original
      fs.writeFileSync(envPath, originalEnvContent);
      return res.status(400).json({ error: `Arquivo CSV não encontrado: ${targetCsv}` });
    }
    
    // Executar migrator.py
    const pythonCmd = process.env.PYTHON_COMMAND || 'python';
    const pythonCommand = `"${pythonCmd}" "${migratorPath}" --env-file "${envPath}" --file "${targetCsv}"`;
    
    logger.info(`Gerando SQL para tabela ${tableName} com arquivo ${targetCsv}`);
    
    exec(pythonCommand, { cwd: BASE_DIR, maxBuffer: 10 * 1024 * 1024 }, (error, stdout, stderr) => {
      // Restaurar .env original
      fs.writeFileSync(envPath, originalEnvContent);
      
      if (error) {
        logger.error(`Erro ao gerar SQL: ${error.message}`);
        return res.status(500).json({ error: 'Erro ao gerar SQL', details: error.message });
      }
      
      // Verificar se o arquivo SQL foi gerado
      const sqlDir = path.join(BASE_DIR, 'backend', 'scripts');
      const baseName = path.basename(targetCsv, '.csv');
      const tableSuffix = tableName.replace('public.', '').replace('.', '_');
      const expectedSqlFile = path.join(sqlDir, `${tableSuffix}_${baseName}.sql`);
      
      if (fs.existsSync(expectedSqlFile)) {
        const sqlContent = fs.readFileSync(expectedSqlFile, 'utf8');
        const stats = fs.statSync(expectedSqlFile);
        
        logger.info(`SQL gerado com sucesso: ${expectedSqlFile}`);
        res.json({
          message: 'SQL gerado com sucesso',
          tableName,
          csvFile: targetCsv,
          sqlFile: path.basename(expectedSqlFile),
          sqlPath: expectedSqlFile,
          fileSize: stats.size,
          linesCount: sqlContent.split('\n').length,
          preview: sqlContent.substring(0, 500) + (sqlContent.length > 500 ? '...' : '')
        });
      } else {
        logger.error(`Arquivo SQL não foi gerado: ${expectedSqlFile}`);
        res.status(500).json({ error: 'Arquivo SQL não foi gerado', expectedPath: expectedSqlFile });
      }
    });
    
  } catch (error) {
    logger.error(`Erro ao gerar SQL: ${error.message}`);
    res.status(500).json({ error: 'Erro ao gerar SQL' });
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

// Endpoint genérico para migração de todas as tabelas
app.post('/api/migration/table/:tableName', async (req, res) => {
  try {
    const { tableName } = req.params;
    const { csvFile, generateOnly = false } = req.body;
    
    // Validar tabelas permitidas
    const allowedTables = [
      'tb_cidadao', 
      'tb_fat_cidadao', 
      'tb_fat_cidadao_pec', 
      'tb_fat_cad_individual',
      'tb_cds_cad_individual',
      'tl_cds_cad_individual'
    ];
    
    if (!allowedTables.includes(tableName)) {
      return res.status(400).json({ error: `Tabela não permitida: ${tableName}` });
    }
    
    logger.info(`Iniciando migração ${tableName} - Arquivo: ${csvFile || 'todos'}, Apenas SQL: ${generateOnly}`);
    
    // Verificar se existe pelo menos um arquivo CSV
    const csvDir = path.join(BASE_DIR, 'datacsv');
    let csvFiles = [];
    
    if (fs.existsSync(csvDir)) {
      csvFiles = fs.readdirSync(csvDir).filter(f => f.toLowerCase().endsWith('.csv'));
    }
    
    if (csvFiles.length === 0) {
      return res.status(400).json({ error: 'Nenhum arquivo CSV encontrado para processar' });
    }
    
    // Usar o arquivo especificado ou o primeiro disponível
    const targetCsv = csvFile || csvFiles[0];
    const csvPath = path.join(csvDir, targetCsv);
    
    if (!fs.existsSync(csvPath)) {
      return res.status(400).json({ error: `Arquivo CSV não encontrado: ${targetCsv}` });
    }
    
    // Preparar argumentos para o migrator
    const migratorPath = path.join(BASE_DIR, 'migrator.py');
    const envPath = path.join(BASE_DIR, '.env');
    const pythonCmd = process.env.PYTHON_COMMAND || 'python';
    
    // Argumentos base
    const args = [
      `"${migratorPath}"`,
      '--env-file', `"${envPath}"`,
      '--file', `"${targetCsv}"`,
      '--table-name', `public.${tableName}`
    ];
    
    // Se for apenas geração de SQL, não conectar ao banco
    if (generateOnly) {
      args.push('--no-db');
    }
    
    const pythonCommand = `"${pythonCmd}" ${args.join(' ')}`;
    
    logger.info(`Executando comando: ${pythonCommand}`);
    
    // Executar o comando
    const { exec } = require('child_process');
    exec(pythonCommand, { cwd: BASE_DIR, maxBuffer: 50 * 1024 * 1024 }, (error, stdout, stderr) => {
      if (error) {
        logger.error(`Erro na migração ${tableName}: ${error.message}`);
        return res.status(500).json({ 
          error: 'Erro na migração', 
          details: error.message,
          stdout: stdout,
          stderr: stderr
        });
      }
      
      // Verificar se o arquivo SQL foi gerado
      const sqlDir = path.join(BASE_DIR, 'backend', 'scripts');
      const baseName = path.basename(targetCsv, '.csv');
      const expectedSqlFile = path.join(sqlDir, `${tableName}_${baseName}.sql`);
      
      let sqlGenerated = false;
      let sqlContent = '';
      let sqlStats = null;
      
      if (fs.existsSync(expectedSqlFile)) {
        sqlGenerated = true;
        sqlContent = fs.readFileSync(expectedSqlFile, 'utf8');
        sqlStats = fs.statSync(expectedSqlFile);
        logger.info(`SQL gerado: ${expectedSqlFile} (${sqlStats.size} bytes)`);
      }
      
      // Resposta de sucesso
      const response = {
        success: true,
        tableName: tableName,
        csvFile: targetCsv,
        generateOnly: generateOnly,
        sqlGenerated: sqlGenerated,
        sqlFile: sqlGenerated ? `${tableName}_${baseName}.sql` : null,
        sqlSize: sqlStats ? sqlStats.size : 0,
        stdout: stdout,
        stderr: stderr
      };
      
      if (generateOnly && sqlGenerated) {
        response.message = `Script SQL gerado com sucesso para ${tableName}`;
      } else if (!generateOnly) {
        response.message = `Migração da ${tableName} executada com sucesso`;
      } else {
        response.message = 'Processo concluído, mas nenhum SQL foi gerado';
      }
      
      logger.info(`Migração ${tableName} concluída: ${response.message}`);
      res.json(response);
    });
    
  } catch (error) {
    logger.error(`Erro na migração: ${error.message}`);
    res.status(500).json({ error: 'Erro interno na migração', details: error.message });
  }
});

// Endpoint específico para migração da tb_cidadao (mantido para compatibilidade)
app.post('/api/migration/tb-cidadao', async (req, res) => {
  req.params = { tableName: 'tb_cidadao' };
  return app._router.handle(req, res, () => {});
});

// Endpoint para upload de CSV
const multer = require('multer');
const upload = multer({ dest: CSV_DIR });

// Importar rotas de logs do PostgreSQL
const postgresqlLogsRoutes = require('./backend/routes/postgresql-logs');
app.use('/api/postgresql-logs', postgresqlLogsRoutes);

// Endpoints para tabelas FAT
app.get('/api/fat-tables/data', async (req, res) => {
  try {
    const { table, page = 1, limit = 50, search = '' } = req.query;
    
    logger.info(`🔍 Buscando dados da tabela: ${table} (página ${page})`);
    
    // Validar tabela
    const allowedTables = ['tb_fat_cad_individual', 'tb_fat_cidadao', 'tb_fat_cidadao_pec', 'tb_cidadao', 'tl_cds_cad_individual', 'tb_cds_cad_individual'];
    if (!allowedTables.includes(table)) {
      return res.status(400).json({ error: 'Tabela não permitida' });
    }

    // Configurar conexão com banco
    const client = new Client({
      host: process.env.POSTGRES_HOST,
      port: parseInt(process.env.POSTGRES_PORT),
      database: process.env.POSTGRES_DB,
      user: process.env.POSTGRES_USER,
      password: process.env.POSTGRES_PASSWORD,
    });

    await client.connect();

    // Construir query baseada na tabela
    let selectFields = '';
    let searchCondition = '';
    let fromClause = '';
    
    switch (table) {
      case 'tb_fat_cad_individual':
        selectFields = `
          co_seq_fat_cad_individual as id,
          nu_cns,
          nu_cpf_cidadao,
          co_dim_sexo,
          dt_nascimento,
          co_dim_raca_cor,
          co_dim_nacionalidade,
          co_dim_pais_nascimento
        `;
        if (search) {
          searchCondition = `WHERE nu_cns ILIKE '%${search}%' OR nu_cpf_cidadao ILIKE '%${search}%'`;
        }
        break;
        
      case 'tb_fat_cidadao':
        selectFields = `
          co_seq_fat_cidadao as id,
          nu_cns,
          nu_cpf_cidadao,
          co_fat_cad_individual,
          co_dim_municipio,
          co_dim_unidade_saude,
          co_dim_equipe
        `;
        if (search) {
          searchCondition = `WHERE nu_cns ILIKE '%${search}%' OR nu_cpf_cidadao ILIKE '%${search}%'`;
        }
        break;
        
      case 'tb_fat_cidadao_pec':
        selectFields = `
          co_seq_fat_cidadao_pec as id,
          co_dim_unidade_saude_vinc,
          co_dim_equipe_vinc,
          nu_cns,
          nu_cpf_cidadao,
          no_cidadao,
          co_dim_tempo_nascimento,
          nu_telefone_celular,
          co_dim_sexo
        `;
        if (search) {
          searchCondition = `WHERE nu_cns ILIKE '%${search}%' OR nu_cpf_cidadao ILIKE '%${search}%' OR no_cidadao ILIKE '%${search}%' OR nu_telefone_celular ILIKE '%${search}%'`;
        }
        break;
        
      case 'tb_cidadao':
        selectFields = `
          co_seq_cidadao as id,
          c.nu_cns,
          c.nu_cpf,
          c.no_cidadao,
          c.no_sexo,
          c.dt_nascimento,
          c.nu_telefone_celular,
          c.co_raca_cor,
          c.co_nacionalidade,
          c.co_pais_nascimento,
          c.co_unico_cidadao,
          c.co_unico_ultima_ficha
        `;
        fromClause = `FROM ${table} c`;
        if (search) {
          searchCondition = `WHERE c.nu_cns ILIKE '%${search}%' OR c.nu_cpf ILIKE '%${search}%' OR c.no_cidadao ILIKE '%${search}%' OR c.nu_telefone_celular ILIKE '%${search}%' OR c.co_unico_cidadao ILIKE '%${search}%' OR c.co_unico_ultima_ficha ILIKE '%${search}%'`;
        }
        break;
        
      case 'tl_cds_cad_individual':
        selectFields = `
          co_seq_cds_cad_individual as id,
          nu_micro_area,
          nu_cpf_cidadao,
          no_cidadao,
          dt_nascimento,
          nu_celular_cidadao,
          co_raca_cor,
          co_nacionalidade,
          co_unico_ficha_origem
        `;
        if (search) {
          searchCondition = `WHERE nu_cpf_cidadao ILIKE '%${search}%' OR no_cidadao ILIKE '%${search}%' OR nu_celular_cidadao ILIKE '%${search}%' OR co_unico_ficha_origem ILIKE '%${search}%'`;
        }
        break;
        
      case 'tb_cds_cad_individual':
        selectFields = `
          co_seq_cds_cad_individual as id,
          c.nu_micro_area,
          c.nu_cpf_cidadao,
          c.no_cidadao,
          s.no_sexo,
          c.dt_nascimento,
          c.nu_celular_cidadao,
          c.co_raca_cor,
          c.co_nacionalidade,
          c.co_unico_ficha_origem
        `;
        fromClause = `FROM ${table} c LEFT JOIN public.tb_sexo s ON c.co_sexo = s.co_sexo`;
        if (search) {
          searchCondition = `WHERE c.nu_cpf_cidadao ILIKE '%${search}%' OR c.no_cidadao ILIKE '%${search}%' OR c.nu_celular_cidadao ILIKE '%${search}%' OR c.co_unico_ficha_origem ILIKE '%${search}%'`;
        }
        break;
    }

    // Query para contar total de registros
    const countFromClause = fromClause || `FROM ${table}`;
    const countQuery = `SELECT COUNT(*) as total ${countFromClause} ${searchCondition}`;
    const countResult = await client.query(countQuery);
    const totalRecords = parseInt(countResult.rows[0].total);

    // Validar parâmetros de paginação
    const pageNum = Math.max(1, parseInt(page));
    const limitNum = Math.min(Math.max(1, parseInt(limit)), 1000); // Máximo 1000 registros por página
    const offset = (pageNum - 1) * limitNum;
    
    // Calcular total de páginas
    const totalPages = Math.ceil(totalRecords / limitNum);

    // Query para buscar dados com paginação
    let orderBy = 'id DESC';
    
    const dataFromClause = fromClause || `FROM ${table}`;
    const dataQuery = `
      SELECT ${selectFields}
      ${dataFromClause}
      ${searchCondition}
      ORDER BY ${orderBy}
      LIMIT $1 OFFSET $2
    `;
    
    logger.info(`📊 Query: ${dataQuery} (LIMIT: ${limitNum}, OFFSET: ${offset})`);
    const dataResult = await client.query(dataQuery, [limitNum, offset]);
    
    await client.end();

    logger.info(`✅ Dados obtidos: ${dataResult.rows.length} registros de ${totalRecords} total (página ${pageNum}/${totalPages})`);

    res.json({
      records: dataResult.rows,
      total: totalRecords,
      page: pageNum,
      limit: limitNum,
      totalPages: totalPages,
      hasNextPage: pageNum < totalPages,
      hasPrevPage: pageNum > 1
    });

  } catch (error) {
    logger.error(`❌ Erro ao buscar dados da tabela FAT: ${error.message}`);
    logger.error(`Stack trace: ${error.stack}`);
    res.status(500).json({ error: 'Erro ao buscar dados da tabela', details: error.message });
  }
});

// Cache para estatísticas (5 minutos)
let statsCache = null;
let statsCacheTime = 0;
const STATS_CACHE_DURATION = 5 * 60 * 1000; // 5 minutos

// Endpoint para limpar cache de estatísticas
app.delete('/api/fat-tables/stats/cache', (req, res) => {
  statsCache = null;
  statsCacheTime = 0;
  logger.info('🗑️ Cache de estatísticas limpo');
  res.json({ message: 'Cache limpo com sucesso' });
});

// Endpoint para estatísticas das tabelas FAT
app.get('/api/fat-tables/stats', async (req, res) => {
  try {
    // Verificar cache (permitir forçar atualização com ?refresh=true)
    const forceRefresh = req.query.refresh === 'true';
    const now = Date.now();
    if (!forceRefresh && statsCache && (now - statsCacheTime) < STATS_CACHE_DURATION) {
      logger.info('📊 Retornando estatísticas do cache');
      return res.json(statsCache);
    }

    const client = new Client({
      host: process.env.POSTGRES_HOST,
      port: parseInt(process.env.POSTGRES_PORT),
      database: process.env.POSTGRES_DB,
      user: process.env.POSTGRES_USER,
      password: process.env.POSTGRES_PASSWORD,
    });

    await client.connect();

    // Consulta otimizada única para todas as tabelas
    const optimizedStatsQuery = `
      WITH table_stats AS (
        SELECT 
          'tb_fat_cad_individual' as table_name,
          (SELECT COUNT(*) FROM tb_fat_cad_individual) as total,
          (SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'tb_fat_cad_individual')) as exists
        UNION ALL
        SELECT 
          'tb_fat_cidadao' as table_name,
          (SELECT COUNT(*) FROM tb_fat_cidadao) as total,
          (SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'tb_fat_cidadao')) as exists
        UNION ALL
        SELECT 
          'tb_fat_cidadao_pec' as table_name,
          (SELECT COUNT(*) FROM tb_fat_cidadao_pec) as total,
          (SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'tb_fat_cidadao_pec')) as exists
        UNION ALL
        SELECT 
          'tb_cidadao' as table_name,
          (SELECT COUNT(*) FROM tb_cidadao) as total,
          (SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'tb_cidadao')) as exists
        UNION ALL
        SELECT 
          'tl_cds_cad_individual' as table_name,
          (SELECT COUNT(*) FROM tl_cds_cad_individual) as total,
          (SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'tl_cds_cad_individual')) as exists
        UNION ALL
        SELECT 
          'tb_cds_cad_individual' as table_name,
          (SELECT COUNT(*) FROM tb_cds_cad_individual) as total,
          (SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'tb_cds_cad_individual')) as exists
      )
      SELECT table_name, total, exists FROM table_stats;
    `;

    const statsResult = await client.query(optimizedStatsQuery);
    const stats = {};
    const currentTime = new Date().toISOString();

    for (const row of statsResult.rows) {
      stats[row.table_name] = {
        total: parseInt(row.total) || 0,
        lastUpdate: currentTime,
        status: row.exists ? 'active' : 'not_found'
      };
    }

    await client.end();

    // Atualizar cache
    statsCache = stats;
    statsCacheTime = now;

    logger.info('📊 Estatísticas atualizadas e armazenadas no cache');
    res.json(stats);
  } catch (error) {
    logger.error('Erro ao buscar estatísticas das tabelas FAT:', error);
    res.status(500).json({ error: 'Erro interno do servidor' });
  }
});

// Endpoint para exportar dados das tabelas FAT
app.get('/api/fat-tables/export', async (req, res) => {
  try {
    const { table } = req.query;
    
    const allowedTables = ['tb_fat_cad_individual', 'tb_fat_cidadao', 'tb_fat_cidadao_pec', 'tb_cidadao', 'tl_cds_cad_individual', 'tb_cds_cad_individual'];
    if (!allowedTables.includes(table)) {
      return res.status(400).json({ error: 'Tabela não permitida' });
    }

    const client = new Client({
      host: process.env.POSTGRES_HOST,
      port: parseInt(process.env.POSTGRES_PORT),
      database: process.env.POSTGRES_DB,
      user: process.env.POSTGRES_USER,
      password: process.env.POSTGRES_PASSWORD,
    });

    await client.connect();

    // Query para exportar todos os dados
    const result = await client.query(`SELECT * FROM ${table} ORDER BY 1 DESC`);
    
    await client.end();

    // Converter para CSV
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Nenhum dado encontrado para exportar' });
    }

    const headers = Object.keys(result.rows[0]);
    const csvContent = [
      headers.join(','),
      ...result.rows.map(row => 
        headers.map(header => {
          const value = row[header];
          return value !== null && value !== undefined ? `"${value}"` : '';
        }).join(',')
      )
    ].join('\n');

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="${table}_export.csv"`);
    res.send(csvContent);

  } catch (error) {
    logger.error(`Erro ao exportar dados da tabela FAT: ${error.message}`);
    res.status(500).json({ error: 'Erro ao exportar dados' });
  }
});

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

// Rota para consultar dados de uma tabela específica
app.get('/api/tables/:tableName', async (req, res) => {
  try {
    const { tableName } = req.params;
    const { page = 1, limit = 10 } = req.query;
    
    const offset = (page - 1) * limit;
    
    const client = new Client({
      host: process.env.POSTGRES_HOST,
      port: process.env.POSTGRES_PORT,
      database: process.env.POSTGRES_DB,
      user: process.env.POSTGRES_USER,
      password: process.env.POSTGRES_PASSWORD,
    });

    await client.connect();
    
    // Consulta para contar total de registros
    const countQuery = `SELECT COUNT(*) as total FROM ${tableName}`;
    const countResult = await client.query(countQuery);
    const total = parseInt(countResult.rows[0].total);
    
    // Consulta para buscar dados paginados
    const dataQuery = `SELECT * FROM ${tableName} ORDER BY 1 LIMIT $1 OFFSET $2`;
    const dataResult = await client.query(dataQuery, [limit, offset]);
    
    await client.end();
    
    res.json({
      success: true,
      data: dataResult.rows,
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total: total,
        totalPages: Math.ceil(total / limit)
      }
    });
    
  } catch (error) {
    logger.error('Erro ao consultar tabela:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Erro ao consultar dados da tabela: ' + error.message 
    });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  logger.info(`Backend iniciado na porta ${PORT}`);
  logger.info(`MIGRATOR: ${MIGRATOR}`);
});
