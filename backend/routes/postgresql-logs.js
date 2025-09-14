const express = require('express')
const fs = require('fs').promises
const path = require('path')
const router = express.Router()

// Middleware para validar diretório
const validateDirectory = (req, res, next) => {
  const { directory } = req.body
  
  if (!directory) {
    return res.status(400).json({ error: 'Diretório é obrigatório' })
  }
  
  // Validação básica de segurança - evitar path traversal
  if (directory.includes('..') || directory.includes('~')) {
    return res.status(400).json({ error: 'Diretório inválido' })
  }
  
  next()
}

// Listar arquivos de log disponíveis
router.post('/files', validateDirectory, async (req, res) => {
  try {
    const { directory } = req.body
    
    // Verificar se o diretório existe
    try {
      await fs.access(directory)
    } catch (error) {
      return res.status(404).json({ 
        error: 'Diretório não encontrado',
        details: `O diretório ${directory} não existe ou não é acessível`
      })
    }
    
    // Listar arquivos .log
    const files = await fs.readdir(directory)
    const logFiles = files.filter(file => file.endsWith('.log'))
    
    // Obter informações detalhadas dos arquivos
    const fileDetails = await Promise.all(
      logFiles.map(async (filename) => {
        try {
          const filePath = path.join(directory, filename)
          const stats = await fs.stat(filePath)
          
          return {
            name: filename,
            size: formatFileSize(stats.size),
            sizeBytes: stats.size,
            modified: stats.mtime.toISOString(),
            created: stats.birthtime.toISOString()
          }
        } catch (error) {
          console.error(`Erro ao obter stats do arquivo ${filename}:`, error)
          return {
            name: filename,
            size: 'Desconhecido',
            sizeBytes: 0,
            modified: null,
            created: null,
            error: 'Erro ao acessar arquivo'
          }
        }
      })
    )
    
    // Ordenar por data de modificação (mais recente primeiro)
    fileDetails.sort((a, b) => {
      if (!a.modified || !b.modified) return 0
      return new Date(b.modified) - new Date(a.modified)
    })
    
    res.json(fileDetails)
    
  } catch (error) {
    console.error('Erro ao listar arquivos de log:', error)
    res.status(500).json({ 
      error: 'Erro interno do servidor',
      details: error.message
    })
  }
})

// Obter conteúdo de um arquivo de log
router.post('/content', validateDirectory, async (req, res) => {
  try {
    const { directory, filename, lines = 1000 } = req.body
    
    if (!filename) {
      return res.status(400).json({ error: 'Nome do arquivo é obrigatório' })
    }
    
    const filePath = path.join(directory, filename)
    
    // Verificar se o arquivo existe
    try {
      await fs.access(filePath)
    } catch (error) {
      return res.status(404).json({ 
        error: 'Arquivo não encontrado',
        details: `O arquivo ${filename} não existe`
      })
    }
    
    // Ler o arquivo
    const content = await fs.readFile(filePath, 'utf8')
    const allLines = content.split('\n').filter(line => line.trim() !== '')
    
    // Pegar as últimas N linhas
    const maxLines = Math.min(parseInt(lines) || 1000, 5000) // Máximo de 5000 linhas
    const recentLines = allLines.slice(-maxLines)
    
    // Processar linhas para extrair informações estruturadas
    const processedLines = recentLines.map((line, index) => {
      const logEntry = parsePostgreSQLLogLine(line)
      return {
        id: index,
        raw: line,
        ...logEntry
      }
    })
    
    res.json({
      filename,
      totalLines: allLines.length,
      returnedLines: processedLines.length,
      lines: processedLines,
      lastModified: (await fs.stat(filePath)).mtime.toISOString()
    })
    
  } catch (error) {
    console.error('Erro ao ler arquivo de log:', error)
    res.status(500).json({ 
      error: 'Erro interno do servidor',
      details: error.message
    })
  }
})

// Monitorar arquivo de log em tempo real (últimas linhas)
router.post('/tail', validateDirectory, async (req, res) => {
  try {
    const { directory, filename, lastSize = 0 } = req.body
    
    if (!filename) {
      return res.status(400).json({ error: 'Nome do arquivo é obrigatório' })
    }
    
    const filePath = path.join(directory, filename)
    
    // Verificar se o arquivo existe
    try {
      await fs.access(filePath)
    } catch (error) {
      return res.status(404).json({ 
        error: 'Arquivo não encontrado',
        details: `O arquivo ${filename} não existe`
      })
    }
    
    const stats = await fs.stat(filePath)
    const currentSize = stats.size
    
    // Se o arquivo não cresceu, retornar vazio
    if (currentSize <= lastSize) {
      return res.json({
        filename,
        newLines: [],
        currentSize,
        hasNewContent: false
      })
    }
    
    // Ler apenas o conteúdo novo
    const fileHandle = await fs.open(filePath, 'r')
    const buffer = Buffer.alloc(currentSize - lastSize)
    await fileHandle.read(buffer, 0, buffer.length, lastSize)
    await fileHandle.close()
    
    const newContent = buffer.toString('utf8')
    const newLines = newContent.split('\n').filter(line => line.trim() !== '')
    
    // Processar novas linhas
    const processedLines = newLines.map((line, index) => {
      const logEntry = parsePostgreSQLLogLine(line)
      return {
        id: Date.now() + index,
        raw: line,
        ...logEntry
      }
    })
    
    res.json({
      filename,
      newLines: processedLines,
      currentSize,
      hasNewContent: processedLines.length > 0
    })
    
  } catch (error) {
    console.error('Erro ao monitorar arquivo de log:', error)
    res.status(500).json({ 
      error: 'Erro interno do servidor',
      details: error.message
    })
  }
})

// Função auxiliar para formatar tamanho de arquivo
function formatFileSize(bytes) {
  if (bytes === 0) return '0 Bytes'
  
  const k = 1024
  const sizes = ['Bytes', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

// Função auxiliar para parsear linha de log do PostgreSQL
function parsePostgreSQLLogLine(line) {
  // Padrão típico do PostgreSQL: 2024-01-15 10:30:45.123 UTC [12345] LOG: mensagem
  const postgresPattern = /^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \w+) \[(\d+)\] (\w+):\s*(.*)$/
  
  const match = line.match(postgresPattern)
  
  if (match) {
    const [, timestamp, pid, level, message] = match
    
    return {
      timestamp: timestamp,
      pid: pid,
      level: level.toUpperCase(),
      message: message.trim(),
      parsed: true
    }
  }
  
  // Se não conseguir parsear, tentar extrair pelo menos o timestamp
  const timestampPattern = /^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/
  const timestampMatch = line.match(timestampPattern)
  
  // Detectar nível de log na linha
  let level = 'LOG'
  if (line.includes('ERROR')) level = 'ERROR'
  else if (line.includes('WARNING')) level = 'WARNING'
  else if (line.includes('INFO')) level = 'INFO'
  else if (line.includes('DEBUG')) level = 'DEBUG'
  
  return {
    timestamp: timestampMatch ? timestampMatch[1] : new Date().toISOString(),
    pid: null,
    level: level,
    message: line,
    parsed: false
  }
}

module.exports = router