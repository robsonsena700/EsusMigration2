import React, { useState, useEffect } from 'react'
import { FileText, Download, Trash2, RefreshCw, Search, Filter, Eye } from 'lucide-react'
import toast from 'react-hot-toast'

const PostgreSQLLogViewer = () => {
  // Estados principais
  const [availableLogFiles, setAvailableLogFiles] = useState([])
  const [selectedFile, setSelectedFile] = useState('')
  const [logContent, setLogContent] = useState([])
  const [filteredContent, setFilteredContent] = useState([])
  const [isLoading, setIsLoading] = useState(false)
  const [logDirectory, setLogDirectory] = useState('C:\\Program Files\\PostgreSQL\\16\\data\\log')
  
  // Estados de filtros
  const [filters, setFilters] = useState({
    level: 'all',
    search: '',
    dateFrom: '',
    dateTo: ''
  })
  
  // Estados de configuração
  const [autoRefresh, setAutoRefresh] = useState(false)
  const [refreshInterval, setRefreshInterval] = useState(5000)
  const [maxLines, setMaxLines] = useState(1000)
  const [autoScroll, setAutoScroll] = useState(true)

  // Níveis de log com cores
  const logLevels = {
    ERROR: { color: 'text-red-600', bg: 'bg-red-50', border: 'border-red-200' },
    WARNING: { color: 'text-yellow-600', bg: 'bg-yellow-50', border: 'border-yellow-200' },
    INFO: { color: 'text-blue-600', bg: 'bg-blue-50', border: 'border-blue-200' },
    LOG: { color: 'text-gray-600', bg: 'bg-gray-50', border: 'border-gray-200' },
    DEBUG: { color: 'text-purple-600', bg: 'bg-purple-50', border: 'border-purple-200' }
  }

  // Buscar arquivos de log disponíveis
  const fetchAvailableLogFiles = async () => {
    if (!logDirectory.trim()) {
      toast.error('Por favor, informe o diretório dos logs')
      return
    }

    setIsLoading(true)
    try {
      const response = await fetch('http://localhost:3000/api/postgresql-logs/files', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ directory: logDirectory })
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.details || errorData.error || 'Erro ao buscar arquivos')
      }

      const files = await response.json()
      setAvailableLogFiles(files)
      
      if (files.length === 0) {
        toast.info('Nenhum arquivo de log encontrado no diretório especificado')
      } else {
        toast.success(`${files.length} arquivo(s) de log encontrado(s)`)
      }
    } catch (error) {
      console.error('Erro ao buscar arquivos:', error)
      toast.error(`Erro ao buscar arquivos: ${error.message}`)
      setAvailableLogFiles([])
    } finally {
      setIsLoading(false)
    }
  }

  // Carregar conteúdo do arquivo de log
  const fetchLogContent = async (filename) => {
    if (!filename || !logDirectory.trim()) {
      toast.error('Arquivo ou diretório não especificado')
      return
    }

    setIsLoading(true)
    try {
      const response = await fetch('http://localhost:3000/api/postgresql-logs/content', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ 
          directory: logDirectory, 
          filename: filename,
          lines: maxLines
        })
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.details || errorData.error || 'Erro ao carregar conteúdo')
      }

      const data = await response.json()
      setLogContent(data.lines || [])
      setSelectedFile(filename)
      toast.success(`${data.returnedLines} linhas carregadas de ${filename}`)
    } catch (error) {
      console.error('Erro ao carregar conteúdo:', error)
      toast.error(`Erro ao carregar conteúdo: ${error.message}`)
      setLogContent([])
    } finally {
      setIsLoading(false)
    }
  }

  // Aplicar filtros
  const applyFilters = () => {
    let filtered = [...logContent]

    // Filtro por nível
    if (filters.level !== 'all') {
      filtered = filtered.filter(log => log.level === filters.level)
    }

    // Filtro por texto
    if (filters.search.trim()) {
      const searchTerm = filters.search.toLowerCase()
      filtered = filtered.filter(log => 
        log.message.toLowerCase().includes(searchTerm) ||
        log.raw.toLowerCase().includes(searchTerm)
      )
    }

    // Filtro por data
    if (filters.dateFrom) {
      filtered = filtered.filter(log => {
        if (!log.timestamp) return true
        const logDate = new Date(log.timestamp)
        const fromDate = new Date(filters.dateFrom)
        return logDate >= fromDate
      })
    }

    if (filters.dateTo) {
      filtered = filtered.filter(log => {
        if (!log.timestamp) return true
        const logDate = new Date(log.timestamp)
        const toDate = new Date(filters.dateTo + 'T23:59:59')
        return logDate <= toDate
      })
    }

    setFilteredContent(filtered)
  }

  // Baixar logs
  const downloadLogs = () => {
    if (filteredContent.length === 0) {
      toast.error('Nenhum log para baixar')
      return
    }

    const content = filteredContent.map(log => log.raw).join('\n')
    const blob = new Blob([content], { type: 'text/plain' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `postgresql-logs-${selectedFile}-${new Date().toISOString().split('T')[0]}.log`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
    toast.success('Logs baixados com sucesso')
  }

  // Limpar logs
  const clearLogs = () => {
    setLogContent([])
    setFilteredContent([])
    setSelectedFile('')
    toast.success('Logs limpos')
  }

  // Formatar timestamp
  const formatTimestamp = (timestamp) => {
    if (!timestamp) return 'N/A'
    try {
      return new Date(timestamp).toLocaleString('pt-BR')
    } catch {
      return timestamp
    }
  }

  // Obter configuração de nível
  const getLevelConfig = (level) => {
    return logLevels[level] || logLevels.LOG
  }

  // Aplicar filtros quando mudarem
  useEffect(() => {
    applyFilters()
  }, [logContent, filters])

  // Auto-refresh
  useEffect(() => {
    if (!autoRefresh || !selectedFile) return

    const interval = setInterval(() => {
      fetchLogContent(selectedFile)
    }, refreshInterval)

    return () => clearInterval(interval)
  }, [autoRefresh, selectedFile, refreshInterval, logDirectory, maxLines])

  // Auto-scroll
  useEffect(() => {
    if (autoScroll && filteredContent.length > 0) {
      setTimeout(() => {
        const logContainer = document.getElementById('log-container')
        if (logContainer) {
          logContainer.scrollTop = logContainer.scrollHeight
        }
      }, 100)
    }
  }, [filteredContent, autoScroll])

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-xl font-semibold text-gray-900 flex items-center gap-2">
          <FileText className="w-5 h-5" />
          Logs do PostgreSQL
        </h2>
        <div className="flex items-center gap-2">
          <button
            onClick={() => selectedFile && fetchLogContent(selectedFile)}
            disabled={isLoading || !selectedFile}
            className="px-3 py-1 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-1"
          >
            <RefreshCw className={`w-4 h-4 ${isLoading ? 'animate-spin' : ''}`} />
            Atualizar
          </button>
          <button
            onClick={downloadLogs}
            disabled={filteredContent.length === 0}
            className="px-3 py-1 bg-green-600 text-white rounded-md hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-1"
          >
            <Download className="w-4 h-4" />
            Baixar
          </button>
          <button
            onClick={clearLogs}
            disabled={logContent.length === 0}
            className="px-3 py-1 bg-red-600 text-white rounded-md hover:bg-red-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-1"
          >
            <Trash2 className="w-4 h-4" />
            Limpar
          </button>
        </div>
      </div>

      {/* Configuração do diretório */}
      <div className="mb-4">
        <label className="block text-sm font-medium text-gray-700 mb-2">
          Diretório dos Logs do PostgreSQL:
        </label>
        <div className="flex gap-2">
          <input
            type="text"
            value={logDirectory}
            onChange={(e) => setLogDirectory(e.target.value)}
            placeholder="Ex: C:\Program Files\e-SUS\database\postgresql-9.6.13-4-win-x64\data\pg_log"
            className="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          <button
            onClick={fetchAvailableLogFiles}
            disabled={isLoading}
            className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
          >
            {isLoading ? (
              <RefreshCw className="w-4 h-4 animate-spin" />
            ) : (
              <Search className="w-4 h-4" />
            )}
            Buscar Arquivos
          </button>
        </div>
      </div>

      {/* Lista de arquivos */}
      {availableLogFiles.length > 0 && (
        <div className="mb-4">
          <h3 className="text-sm font-medium text-gray-700 mb-2">
            Arquivos Encontrados ({availableLogFiles.length}):
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
            {availableLogFiles.map((file, index) => (
              <div
                key={index}
                className={`p-3 border rounded-md cursor-pointer transition-colors ${
                  selectedFile === file.name
                    ? 'border-blue-500 bg-blue-50'
                    : 'border-gray-200 hover:border-gray-300 hover:bg-gray-50'
                }`}
                onClick={() => fetchLogContent(file.name)}
              >
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium text-gray-900 truncate">
                    {file.name}
                  </span>
                  <Eye className="w-4 h-4 text-gray-400" />
                </div>
                <div className="text-xs text-gray-500 mt-1">
                  <div>Tamanho: {file.size}</div>
                  <div>Modificado: {formatTimestamp(file.modified)}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Filtros */}
      {logContent.length > 0 && (
        <div className="mb-4 p-4 bg-gray-50 rounded-md">
          <div className="flex items-center gap-2 mb-3">
            <Filter className="w-4 h-4 text-gray-600" />
            <span className="text-sm font-medium text-gray-700">Filtros</span>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3">
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Nível:</label>
              <select
                value={filters.level}
                onChange={(e) => setFilters({...filters, level: e.target.value})}
                className="w-full px-2 py-1 text-sm border border-gray-300 rounded focus:outline-none focus:ring-1 focus:ring-blue-500"
              >
                <option value="all">Todos</option>
                <option value="ERROR">ERROR</option>
                <option value="WARNING">WARNING</option>
                <option value="INFO">INFO</option>
                <option value="LOG">LOG</option>
                <option value="DEBUG">DEBUG</option>
              </select>
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Buscar:</label>
              <input
                type="text"
                value={filters.search}
                onChange={(e) => setFilters({...filters, search: e.target.value})}
                placeholder="Texto..."
                className="w-full px-2 py-1 text-sm border border-gray-300 rounded focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Data Início:</label>
              <input
                type="date"
                value={filters.dateFrom}
                onChange={(e) => setFilters({...filters, dateFrom: e.target.value})}
                className="w-full px-2 py-1 text-sm border border-gray-300 rounded focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Data Fim:</label>
              <input
                type="date"
                value={filters.dateTo}
                onChange={(e) => setFilters({...filters, dateTo: e.target.value})}
                className="w-full px-2 py-1 text-sm border border-gray-300 rounded focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
            </div>
          </div>
        </div>
      )}

      {/* Configurações */}
      {logContent.length > 0 && (
        <div className="mb-4 p-4 bg-gray-50 rounded-md">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="flex items-center gap-2">
              <input
                type="checkbox"
                id="autoRefresh"
                checked={autoRefresh}
                onChange={(e) => setAutoRefresh(e.target.checked)}
                className="rounded"
              />
              <label htmlFor="autoRefresh" className="text-sm text-gray-700">
                Auto-atualizar
              </label>
            </div>
            <div className="flex items-center gap-2">
              <input
                type="checkbox"
                id="autoScroll"
                checked={autoScroll}
                onChange={(e) => setAutoScroll(e.target.checked)}
                className="rounded"
              />
              <label htmlFor="autoScroll" className="text-sm text-gray-700">
                Auto-scroll
              </label>
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Máx. linhas:</label>
              <select
                value={maxLines}
                onChange={(e) => setMaxLines(parseInt(e.target.value))}
                className="w-full px-2 py-1 text-sm border border-gray-300 rounded focus:outline-none focus:ring-1 focus:ring-blue-500"
              >
                <option value={500}>500</option>
                <option value={1000}>1000</option>
                <option value={2000}>2000</option>
                <option value={5000}>5000</option>
              </select>
            </div>
          </div>
        </div>
      )}

      {/* Conteúdo dos logs */}
      {selectedFile && (
        <div className="mb-4">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-medium text-gray-700">
              Conteúdo: {selectedFile}
            </h3>
            <span className="text-xs text-gray-500">
              {filteredContent.length} de {logContent.length} linhas
            </span>
          </div>
          
          <div
            id="log-container"
            className="h-96 overflow-y-auto border border-gray-300 rounded-md bg-gray-900 p-4"
          >
            {filteredContent.length === 0 ? (
              <div className="text-center text-gray-400 py-8">
                {logContent.length === 0 ? 'Nenhum log carregado' : 'Nenhum log corresponde aos filtros'}
              </div>
            ) : (
              <div className="space-y-1">
                {filteredContent.map((log, index) => {
                  const levelConfig = getLevelConfig(log.level)
                  return (
                    <div
                      key={log.id || index}
                      className={`p-2 rounded text-xs font-mono ${levelConfig.bg} ${levelConfig.border} border`}
                    >
                      <div className="flex items-start gap-2">
                        <span className={`font-semibold ${levelConfig.color} min-w-[60px]`}>
                          {log.level}
                        </span>
                        <span className="text-gray-500 min-w-[140px]">
                          {formatTimestamp(log.timestamp)}
                        </span>
                        {log.pid && (
                          <span className="text-gray-400 min-w-[60px]">
                            PID:{log.pid}
                          </span>
                        )}
                        <span className="text-gray-800 flex-1 break-all">
                          {log.message}
                        </span>
                      </div>
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Status */}
      <div className="text-xs text-gray-500 text-center">
        {isLoading ? (
          <span className="flex items-center justify-center gap-2">
            <RefreshCw className="w-3 h-3 animate-spin" />
            Carregando...
          </span>
        ) : (
          <span>
            {availableLogFiles.length > 0 && `${availableLogFiles.length} arquivo(s) encontrado(s)`}
            {selectedFile && ` • Arquivo atual: ${selectedFile}`}
            {autoRefresh && ` • Auto-atualização ativa (${refreshInterval/1000}s)`}
          </span>
        )}
      </div>
    </div>
  )
}

export default PostgreSQLLogViewer