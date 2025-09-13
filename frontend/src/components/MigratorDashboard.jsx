import React, { useState, useEffect } from 'react'
import { 
  Upload, 
  Database, 
  FileText, 
  CheckCircle, 
  AlertCircle, 
  Clock, 
  Play, 
  Pause, 
  RotateCcw,
  Download,
  Eye,
  Trash2,
  RefreshCw
} from 'lucide-react'
import toast from 'react-hot-toast'
import LoadingSpinner, { OverlaySpinner } from './LoadingSpinner'

const MigratorDashboard = ({ systemStatus, onStatusUpdate }) => {
  const [csvFiles, setCsvFiles] = useState([])
  const [migrationStatus, setMigrationStatus] = useState('idle') // idle, running, paused, completed, error
  const [migrationProgress, setMigrationProgress] = useState({
    current: 0,
    total: 0,
    currentFile: '',
    processedRecords: 0,
    errors: 0
  })
  const [logs, setLogs] = useState([])
  const [isLoading, setIsLoading] = useState(true)
  const [selectedFiles, setSelectedFiles] = useState([])
  const [showLogs, setShowLogs] = useState(false)

  // Carregar dados iniciais
  useEffect(() => {
    loadInitialData()
  }, [])

  // Polling para atualizações em tempo real
  useEffect(() => {
    let interval
    if (migrationStatus === 'running') {
      interval = setInterval(() => {
        fetchMigrationStatus()
        fetchLogs()
      }, 2000)
    }
    return () => clearInterval(interval)
  }, [migrationStatus])

  const loadInitialData = async () => {
    try {
      await Promise.all([
        fetchCsvFiles(),
        fetchMigrationStatus(),
        fetchLogs()
      ])
    } catch (error) {
      console.error('Erro ao carregar dados iniciais:', error)
      toast.error('Erro ao carregar dados do sistema')
    } finally {
      setIsLoading(false)
    }
  }

  const fetchCsvFiles = async () => {
    try {
      const response = await fetch('/api/csv/files')
      if (response.ok) {
        const files = await response.json()
        setCsvFiles(files)
      }
    } catch (error) {
      console.error('Erro ao buscar arquivos CSV:', error)
    }
  }

  const fetchMigrationStatus = async () => {
    try {
      const response = await fetch('/api/migration/status')
      if (response.ok) {
        const status = await response.json()
        setMigrationStatus(status.status)
        setMigrationProgress(status.progress)
      }
    } catch (error) {
      console.error('Erro ao buscar status da migração:', error)
    }
  }

  const fetchLogs = async () => {
    try {
      const response = await fetch('/api/logs?limit=50')
      if (response.ok) {
        const logsData = await response.json()
        setLogs(logsData)
      }
    } catch (error) {
      console.error('Erro ao buscar logs:', error)
    }
  }

  const startMigration = async () => {
    try {
      setIsLoading(true);
      
      // Verificar se há arquivos selecionados
      if (selectedFiles.length === 0) {
        toast.error('Selecione pelo menos um arquivo CSV para migração');
        return;
      }
      
      const response = await fetch('/api/migration/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ files: selectedFiles })
      });
      
      if (response.ok) {
        const result = await response.json();
        setMigrationStatus('running');
        toast.success(result.message || 'Migração iniciada com sucesso!');
        fetchMigrationStatus();
      } else {
        const error = await response.json();
        toast.error(error.message || 'Erro ao iniciar migração');
      }
    } catch (error) {
      console.error('Erro ao iniciar migração:', error);
      toast.error('Erro ao conectar com o servidor');
    } finally {
      setIsLoading(false);
    }
  }

  const pauseMigration = async () => {
    try {
      const response = await fetch('/api/migration/pause', { method: 'POST' })
      if (response.ok) {
        setMigrationStatus('paused')
        toast.success('Migração pausada')
      }
    } catch (error) {
      console.error('Erro ao pausar migração:', error)
      toast.error('Erro ao pausar migração')
    }
  }

  const resumeMigration = async () => {
    try {
      const response = await fetch('/api/migration/resume', { method: 'POST' })
      if (response.ok) {
        setMigrationStatus('running')
        toast.success('Migração retomada')
      }
    } catch (error) {
      console.error('Erro ao retomar migração:', error)
      toast.error('Erro ao retomar migração')
    }
  }

  const resetMigration = async () => {
    if (!confirm('Tem certeza que deseja resetar a migração? Todos os dados processados serão perdidos.')) {
      return
    }

    try {
      const response = await fetch('/api/migration/reset', { method: 'POST' })
      if (response.ok) {
        setMigrationStatus('idle')
        setMigrationProgress({ current: 0, total: 0, currentFile: '', processedRecords: 0, errors: 0 })
        setSelectedFiles([])
        toast.success('Migração resetada')
      }
    } catch (error) {
      console.error('Erro ao resetar migração:', error)
      toast.error('Erro ao resetar migração')
    }
  }

  const toggleFileSelection = (fileName) => {
    setSelectedFiles(prev => 
      prev.includes(fileName) 
        ? prev.filter(f => f !== fileName)
        : [...prev, fileName]
    )
  }

  const selectAllFiles = () => {
    setSelectedFiles(csvFiles.map(f => f.name))
  }

  const clearSelection = () => {
    setSelectedFiles([])
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'running': return 'text-primary-600 bg-primary-100'
      case 'completed': return 'text-success-600 bg-success-100'
      case 'paused': return 'text-warning-600 bg-warning-100'
      case 'error': return 'text-error-600 bg-error-100'
      default: return 'text-gray-600 bg-gray-100'
    }
  }

  const getStatusText = (status) => {
    switch (status) {
      case 'running': return 'Em Execução'
      case 'completed': return 'Concluída'
      case 'paused': return 'Pausada'
      case 'error': return 'Erro'
      case 'idle': return 'Aguardando'
      default: return 'Desconhecido'
    }
  }

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes'
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
  }

  const formatDate = (dateString) => {
    return new Date(dateString).toLocaleString('pt-BR')
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <LoadingSpinner size="large" text="Carregando dashboard..." />
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header do Dashboard */}
      <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between space-y-4 lg:space-y-0">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">
            Dashboard de Migração
          </h1>
          <p className="text-gray-600">
            Gerencie a migração de dados e-SUS para o banco de dados
          </p>
        </div>
        
        <div className="flex items-center space-x-3">
          <button
            onClick={() => setShowLogs(!showLogs)}
            className="btn btn-secondary flex items-center space-x-2"
          >
            <Eye className="w-4 h-4" />
            <span>{showLogs ? 'Ocultar' : 'Ver'} Logs</span>
          </button>
          
          <button
            onClick={loadInitialData}
            className="btn btn-secondary flex items-center space-x-2"
          >
            <RefreshCw className="w-4 h-4" />
            <span>Atualizar</span>
          </button>
        </div>
      </div>

      {/* Cards de Status */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {/* Status da Migração */}
        <div className="card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Status da Migração</p>
              <p className={`text-lg font-semibold px-3 py-1 rounded-full text-xs ${getStatusColor(migrationStatus)}`}>
                {getStatusText(migrationStatus)}
              </p>
            </div>
            <Database className="w-8 h-8 text-primary-600" />
          </div>
        </div>

        {/* Arquivos CSV */}
        <div className="card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Arquivos CSV</p>
              <p className="text-2xl font-bold text-gray-900">{csvFiles.length}</p>
            </div>
            <FileText className="w-8 h-8 text-primary-600" />
          </div>
        </div>

        {/* Progresso */}
        <div className="card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Progresso</p>
              <p className="text-2xl font-bold text-gray-900">
                {migrationProgress.total > 0 
                  ? `${migrationProgress.current}/${migrationProgress.total}`
                  : '0/0'
                }
              </p>
            </div>
            <Clock className="w-8 h-8 text-primary-600" />
          </div>
        </div>

        {/* Registros Processados */}
        <div className="card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Registros</p>
              <p className="text-2xl font-bold text-gray-900">
                {migrationProgress.processedRecords.toLocaleString('pt-BR')}
              </p>
            </div>
            <CheckCircle className="w-8 h-8 text-success-600" />
          </div>
        </div>
      </div>

      {/* Barra de Progresso */}
      {migrationProgress.total > 0 && (
        <div className="card">
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold text-gray-900">Progresso da Migração</h3>
              <span className="text-sm text-gray-600">
                {Math.round((migrationProgress.current / migrationProgress.total) * 100)}%
              </span>
            </div>
            
            <div className="w-full bg-gray-200 rounded-full h-4 relative overflow-hidden">
              <div 
                className="bg-gradient-to-r from-primary-500 to-primary-600 h-4 rounded-full transition-all duration-500 ease-in-out relative"
                style={{ width: `${(migrationProgress.current / migrationProgress.total) * 100}%` }}
              >
                {migrationProgress.current > 0 && (
                  <div className="absolute inset-0 bg-white bg-opacity-20 animate-pulse"></div>
                )}
              </div>
            </div>
            
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div className="flex justify-between">
                <span className="text-gray-600">Arquivos:</span>
                <span className="font-medium text-gray-800">
                  {migrationProgress.current} / {migrationProgress.total}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-600">Registros:</span>
                <span className="font-medium text-success-600">
                  {migrationProgress.processedRecords.toLocaleString()}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-600">Status:</span>
                <span className={`font-medium capitalize ${
                  migrationStatus === 'running' ? 'text-primary-600' :
                  migrationStatus === 'completed' ? 'text-success-600' :
                  migrationStatus === 'paused' ? 'text-warning-600' :
                  migrationStatus === 'error' ? 'text-error-600' :
                  'text-gray-600'
                }`}>
                  {migrationStatus === 'idle' ? 'Aguardando' :
                   migrationStatus === 'running' ? 'Executando' :
                   migrationStatus === 'completed' ? 'Concluído' :
                   migrationStatus === 'paused' ? 'Pausado' :
                   migrationStatus === 'error' ? 'Erro' : migrationStatus}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-600">Erros:</span>
                <span className={`font-medium ${
                  migrationProgress.errors > 0 ? 'text-error-600' : 'text-success-600'
                }`}>
                  {migrationProgress.errors}
                </span>
              </div>
            </div>
            
            {migrationProgress.currentFile && (
              <div className="mt-3 p-3 bg-gray-50 rounded-lg">
                <div className="text-xs text-gray-500 mb-1">Processando arquivo:</div>
                <div className="text-sm font-medium text-gray-800 truncate">
                  {migrationProgress.currentFile}
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Controles de Migração */}
      <div className="card">
        <div className="space-y-4">
          <h3 className="text-lg font-semibold text-gray-900">Controles de Migração</h3>
          
          <div className="flex flex-wrap gap-3">
            {migrationStatus === 'idle' && (
              <>
                <button
                  onClick={startMigration}
                  disabled={selectedFiles.length === 0}
                  className="btn btn-primary flex items-center space-x-2"
                >
                  <Play className="w-4 h-4" />
                  <span>Iniciar Migração</span>
                </button>
                
                <button
                  onClick={selectAllFiles}
                  className="btn btn-secondary flex items-center space-x-2"
                >
                  <CheckCircle className="w-4 h-4" />
                  <span>Selecionar Todos</span>
                </button>
                
                <button
                  onClick={clearSelection}
                  className="btn btn-secondary flex items-center space-x-2"
                >
                  <Trash2 className="w-4 h-4" />
                  <span>Limpar Seleção</span>
                </button>
              </>
            )}
            
            {migrationStatus === 'running' && (
              <button
                onClick={pauseMigration}
                className="btn btn-warning flex items-center space-x-2"
              >
                <Pause className="w-4 h-4" />
                <span>Pausar</span>
              </button>
            )}
            
            {migrationStatus === 'paused' && (
              <>
                <button
                  onClick={resumeMigration}
                  className="btn btn-primary flex items-center space-x-2"
                >
                  <Play className="w-4 h-4" />
                  <span>Retomar</span>
                </button>
                
                <button
                  onClick={resetMigration}
                  className="btn btn-error flex items-center space-x-2"
                >
                  <RotateCcw className="w-4 h-4" />
                  <span>Resetar</span>
                </button>
              </>
            )}
            
            {(migrationStatus === 'completed' || migrationStatus === 'error') && (
              <button
                onClick={resetMigration}
                className="btn btn-secondary flex items-center space-x-2"
              >
                <RotateCcw className="w-4 h-4" />
                <span>Nova Migração</span>
              </button>
            )}
          </div>
          
          {selectedFiles.length > 0 && (
            <p className="text-sm text-gray-600">
              {selectedFiles.length} arquivo(s) selecionado(s) para migração
            </p>
          )}
        </div>
      </div>

      {/* Lista de Arquivos CSV */}
      <div className="card">
        <div className="space-y-4">
          <h3 className="text-lg font-semibold text-gray-900">Arquivos CSV Disponíveis</h3>
          
          {csvFiles.length === 0 ? (
            <div className="text-center py-8">
              <FileText className="w-12 h-12 text-gray-400 mx-auto mb-4" />
              <p className="text-gray-600">Nenhum arquivo CSV encontrado</p>
              <p className="text-sm text-gray-500">Verifique o diretório datacsv/</p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full table-auto">
                <thead>
                  <tr className="border-b border-gray-200">
                    <th className="text-left py-3 px-4 font-medium text-gray-900">
                      <input
                        type="checkbox"
                        checked={selectedFiles.length === csvFiles.length}
                        onChange={() => selectedFiles.length === csvFiles.length ? clearSelection() : selectAllFiles()}
                        className="rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                      />
                    </th>
                    <th className="text-left py-3 px-4 font-medium text-gray-900">Nome do Arquivo</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-900">Tamanho</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-900">Modificado</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-900">Registros</th>
                  </tr>
                </thead>
                <tbody>
                  {csvFiles.map((file, index) => (
                    <tr key={index} className="border-b border-gray-100 hover:bg-gray-50">
                      <td className="py-3 px-4">
                        <input
                          type="checkbox"
                          checked={selectedFiles.includes(file.name)}
                          onChange={() => toggleFileSelection(file.name)}
                          className="rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                        />
                      </td>
                      <td className="py-3 px-4">
                        <div className="flex items-center space-x-2">
                          <FileText className="w-4 h-4 text-gray-500" />
                          <span className="font-medium text-gray-900">{file.name}</span>
                        </div>
                      </td>
                      <td className="py-3 px-4 text-gray-600">
                        {formatFileSize(file.size)}
                      </td>
                      <td className="py-3 px-4 text-gray-600">
                        {formatDate(file.modified)}
                      </td>
                      <td className="py-3 px-4 text-gray-600">
                        {file.records ? file.records.toLocaleString('pt-BR') : 'N/A'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      {/* Logs */}
      {showLogs && (
        <div className="card">
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold text-gray-900">Logs do Sistema</h3>
              <button
                onClick={fetchLogs}
                className="btn btn-secondary btn-sm flex items-center space-x-1"
              >
                <RefreshCw className="w-3 h-3" />
                <span>Atualizar</span>
              </button>
            </div>
            
            <div className="bg-gray-900 rounded-lg p-4 max-h-96 overflow-y-auto">
              {logs.length === 0 ? (
                <p className="text-gray-400 text-sm">Nenhum log disponível</p>
              ) : (
                <div className="space-y-1">
                  {logs.map((log, index) => (
                    <div key={index} className="text-sm font-mono">
                      <span className="text-gray-400">[{formatDate(log.timestamp)}]</span>
                      <span className={`ml-2 ${
                        log.level === 'error' ? 'text-red-400' :
                        log.level === 'warn' ? 'text-yellow-400' :
                        log.level === 'info' ? 'text-blue-400' :
                        'text-gray-300'
                      }`}>
                        {log.message}
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Overlay de Loading */}
      <OverlaySpinner 
        isVisible={migrationStatus === 'running'}
        text={`Processando ${migrationProgress.currentFile || 'arquivos'}...`}
      />
    </div>
  )
}

export default MigratorDashboard