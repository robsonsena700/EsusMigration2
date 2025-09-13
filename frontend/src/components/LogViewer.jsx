import React, { useState, useEffect, useRef } from 'react'
import { 
  Search, 
  Filter, 
  Download, 
  RefreshCw, 
  ChevronDown,
  AlertCircle,
  Info,
  AlertTriangle,
  Bug,
  Eye,
  EyeOff,
  Trash2,
  Calendar
} from 'lucide-react'
import toast from 'react-hot-toast'

const LogViewer = ({ 
  autoRefresh = false, 
  refreshInterval = 5000,
  maxHeight = '400px',
  showControls = true,
  initialFilters = {}
}) => {
  const [logs, setLogs] = useState([])
  const [filteredLogs, setFilteredLogs] = useState([])
  const [isLoading, setIsLoading] = useState(true)
  const [searchTerm, setSearchTerm] = useState('')
  const [levelFilter, setLevelFilter] = useState('all')
  const [dateFilter, setDateFilter] = useState('all')
  const [isAutoScrollEnabled, setIsAutoScrollEnabled] = useState(true)
  const [showFilters, setShowFilters] = useState(false)
  const logContainerRef = useRef(null)
  const refreshIntervalRef = useRef(null)

  // Níveis de log com configurações
  const logLevels = {
    error: {
      label: 'Erro',
      icon: AlertCircle,
      color: 'text-error-600',
      bgColor: 'bg-error-50',
      borderColor: 'border-error-200'
    },
    warn: {
      label: 'Aviso',
      icon: AlertTriangle,
      color: 'text-warning-600',
      bgColor: 'bg-warning-50',
      borderColor: 'border-warning-200'
    },
    info: {
      label: 'Info',
      icon: Info,
      color: 'text-primary-600',
      bgColor: 'bg-primary-50',
      borderColor: 'border-primary-200'
    },
    debug: {
      label: 'Debug',
      icon: Bug,
      color: 'text-gray-600',
      bgColor: 'bg-gray-50',
      borderColor: 'border-gray-200'
    }
  }

  // Carregar logs iniciais
  useEffect(() => {
    fetchLogs()
  }, [])

  // Configurar auto-refresh
  useEffect(() => {
    if (autoRefresh) {
      refreshIntervalRef.current = setInterval(() => {
        fetchLogs(false)
      }, refreshInterval)
    }

    return () => {
      if (refreshIntervalRef.current) {
        clearInterval(refreshIntervalRef.current)
      }
    }
  }, [autoRefresh, refreshInterval])

  // Aplicar filtros
  useEffect(() => {
    applyFilters()
  }, [logs, searchTerm, levelFilter, dateFilter])

  // Auto-scroll para o final
  useEffect(() => {
    if (isAutoScrollEnabled && logContainerRef.current) {
      logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight
    }
  }, [filteredLogs, isAutoScrollEnabled])

  const fetchLogs = async (showLoading = true) => {
    if (showLoading) setIsLoading(true)
    
    try {
      const response = await fetch('/api/logs?limit=1000')
      if (response.ok) {
        const logsData = await response.json()
        setLogs(logsData)
      } else {
        throw new Error('Erro ao buscar logs')
      }
    } catch (error) {
      console.error('Erro ao buscar logs:', error)
      if (showLoading) {
        toast.error('Erro ao carregar logs')
      }
    } finally {
      if (showLoading) setIsLoading(false)
    }
  }

  const applyFilters = () => {
    let filtered = [...logs]

    // Filtro por termo de busca
    if (searchTerm) {
      filtered = filtered.filter(log => 
        log.message.toLowerCase().includes(searchTerm.toLowerCase()) ||
        (log.source && log.source.toLowerCase().includes(searchTerm.toLowerCase()))
      )
    }

    // Filtro por nível
    if (levelFilter !== 'all') {
      filtered = filtered.filter(log => log.level === levelFilter)
    }

    // Filtro por data
    if (dateFilter !== 'all') {
      const now = new Date()
      const filterDate = new Date()
      
      switch (dateFilter) {
        case 'today':
          filterDate.setHours(0, 0, 0, 0)
          break
        case 'yesterday':
          filterDate.setDate(filterDate.getDate() - 1)
          filterDate.setHours(0, 0, 0, 0)
          break
        case 'week':
          filterDate.setDate(filterDate.getDate() - 7)
          break
        case 'month':
          filterDate.setMonth(filterDate.getMonth() - 1)
          break
      }
      
      filtered = filtered.filter(log => 
        new Date(log.timestamp) >= filterDate
      )
    }

    setFilteredLogs(filtered)
  }

  const clearLogs = async () => {
    if (!confirm('Tem certeza que deseja limpar todos os logs?')) {
      return
    }

    try {
      const response = await fetch('/api/logs', { method: 'DELETE' })
      if (response.ok) {
        setLogs([])
        toast.success('Logs limpos com sucesso')
      } else {
        throw new Error('Erro ao limpar logs')
      }
    } catch (error) {
      console.error('Erro ao limpar logs:', error)
      toast.error('Erro ao limpar logs')
    }
  }

  const downloadLogs = () => {
    const logText = filteredLogs.map(log => 
      `[${formatDate(log.timestamp)}] [${log.level.toUpperCase()}] ${log.message}`
    ).join('\n')
    
    const blob = new Blob([logText], { type: 'text/plain' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `logs-${new Date().toISOString().split('T')[0]}.txt`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
    
    toast.success('Logs baixados com sucesso')
  }

  const formatDate = (dateString) => {
    return new Date(dateString).toLocaleString('pt-BR')
  }

  const formatTime = (dateString) => {
    return new Date(dateString).toLocaleTimeString('pt-BR')
  }

  const getLogLevelConfig = (level) => {
    return logLevels[level] || logLevels.info
  }

  const handleScroll = () => {
    if (logContainerRef.current) {
      const { scrollTop, scrollHeight, clientHeight } = logContainerRef.current
      const isAtBottom = scrollTop + clientHeight >= scrollHeight - 10
      setIsAutoScrollEnabled(isAtBottom)
    }
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-32">
        <div className="flex items-center space-x-2">
          <RefreshCw className="w-4 h-4 animate-spin" />
          <span>Carregando logs...</span>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Controles */}
      {showControls && (
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between space-y-3 sm:space-y-0">
          <div className="flex items-center space-x-3">
            <h3 className="text-lg font-semibold text-gray-900">
              Logs do Sistema
            </h3>
            <span className="text-sm text-gray-500">
              ({filteredLogs.length} de {logs.length})
            </span>
          </div>
          
          <div className="flex items-center space-x-2">
            <button
              onClick={() => setShowFilters(!showFilters)}
              className="btn btn-secondary btn-sm flex items-center space-x-1"
            >
              <Filter className="w-3 h-3" />
              <span>Filtros</span>
              <ChevronDown className={`w-3 h-3 transition-transform ${
                showFilters ? 'rotate-180' : ''
              }`} />
            </button>
            
            <button
              onClick={() => setIsAutoScrollEnabled(!isAutoScrollEnabled)}
              className={`btn btn-sm flex items-center space-x-1 ${
                isAutoScrollEnabled ? 'btn-primary' : 'btn-secondary'
              }`}
            >
              {isAutoScrollEnabled ? <Eye className="w-3 h-3" /> : <EyeOff className="w-3 h-3" />}
              <span>Auto-scroll</span>
            </button>
            
            <button
              onClick={downloadLogs}
              className="btn btn-secondary btn-sm flex items-center space-x-1"
              disabled={filteredLogs.length === 0}
            >
              <Download className="w-3 h-3" />
              <span>Baixar</span>
            </button>
            
            <button
              onClick={() => fetchLogs()}
              className="btn btn-secondary btn-sm flex items-center space-x-1"
            >
              <RefreshCw className="w-3 h-3" />
              <span>Atualizar</span>
            </button>
            
            <button
              onClick={clearLogs}
              className="btn btn-error btn-sm flex items-center space-x-1"
            >
              <Trash2 className="w-3 h-3" />
              <span>Limpar</span>
            </button>
          </div>
        </div>
      )}

      {/* Filtros */}
      {showFilters && (
        <div className="bg-gray-50 rounded-lg p-4 space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {/* Busca */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Buscar
              </label>
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
                <input
                  type="text"
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  placeholder="Buscar nos logs..."
                  className="input pl-10"
                />
              </div>
            </div>
            
            {/* Nível */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Nível
              </label>
              <select
                value={levelFilter}
                onChange={(e) => setLevelFilter(e.target.value)}
                className="input"
              >
                <option value="all">Todos os níveis</option>
                {Object.entries(logLevels).map(([level, config]) => (
                  <option key={level} value={level}>
                    {config.label}
                  </option>
                ))}
              </select>
            </div>
            
            {/* Data */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Período
              </label>
              <select
                value={dateFilter}
                onChange={(e) => setDateFilter(e.target.value)}
                className="input"
              >
                <option value="all">Todos os períodos</option>
                <option value="today">Hoje</option>
                <option value="yesterday">Ontem</option>
                <option value="week">Última semana</option>
                <option value="month">Último mês</option>
              </select>
            </div>
          </div>
        </div>
      )}

      {/* Container de Logs */}
      <div 
        ref={logContainerRef}
        className="bg-gray-900 rounded-lg overflow-hidden"
        style={{ maxHeight }}
        onScroll={handleScroll}
      >
        <div className="p-4 space-y-1 overflow-y-auto" style={{ maxHeight }}>
          {filteredLogs.length === 0 ? (
            <div className="text-center py-8">
              <p className="text-gray-400">Nenhum log encontrado</p>
            </div>
          ) : (
            filteredLogs.map((log, index) => {
              const levelConfig = getLogLevelConfig(log.level)
              const IconComponent = levelConfig.icon
              
              return (
                <div 
                  key={index} 
                  className="flex items-start space-x-3 text-sm font-mono hover:bg-gray-800 p-2 rounded transition-colors"
                >
                  {/* Timestamp */}
                  <span className="text-gray-400 text-xs whitespace-nowrap">
                    {formatTime(log.timestamp)}
                  </span>
                  
                  {/* Nível */}
                  <div className="flex items-center space-x-1 min-w-0">
                    <IconComponent className={`w-3 h-3 ${levelConfig.color}`} />
                    <span className={`text-xs font-medium ${levelConfig.color} uppercase`}>
                      {log.level}
                    </span>
                  </div>
                  
                  {/* Mensagem */}
                  <span className="text-gray-300 flex-1 break-words">
                    {log.message}
                  </span>
                  
                  {/* Source (se disponível) */}
                  {log.source && (
                    <span className="text-gray-500 text-xs whitespace-nowrap">
                      [{log.source}]
                    </span>
                  )}
                </div>
              )
            })
          )}
        </div>
      </div>
      
      {/* Indicador de auto-scroll */}
      {!isAutoScrollEnabled && filteredLogs.length > 0 && (
        <div className="text-center">
          <button
            onClick={() => {
              setIsAutoScrollEnabled(true)
              if (logContainerRef.current) {
                logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight
              }
            }}
            className="btn btn-secondary btn-sm"
          >
            Ir para o final
          </button>
        </div>
      )}
    </div>
  )
}

export default LogViewer