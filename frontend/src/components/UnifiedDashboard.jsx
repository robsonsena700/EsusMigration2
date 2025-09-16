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
  RefreshCw,
  Settings,
  Save,
  TestTube,
  Wifi,
  WifiOff,
  FolderOpen,
  X
} from 'lucide-react'
import toast from 'react-hot-toast'
import LoadingSpinner, { OverlaySpinner } from './LoadingSpinner'
import PostgreSQLLogViewer from './PostgreSQLLogViewer'
import FATTablesViewer from './FATTablesViewer'

const UnifiedDashboard = ({ systemStatus, onStatusUpdate }) => {
  // Estados do Dashboard
  const [csvFiles, setCsvFiles] = useState([])
  const [migrationStatus, setMigrationStatus] = useState('idle')
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
  
  // Estados da Configuração
  const [config, setConfig] = useState({
    POSTGRES_DB: '',
    POSTGRES_USER: '',
    POSTGRES_PASSWORD: '',
    POSTGRES_HOST: 'localhost',
    POSTGRES_PORT: '5432',
    TABLE_NAME: 'public.tl_cds_cad_individual'
  })
  const [selectedTables, setSelectedTables] = useState([])
  const [availableTables, setAvailableTables] = useState([])
  const [sqlGenerationProgress, setSqlGenerationProgress] = useState({
    isGenerating: false,
    current: 0,
    total: 0,
    currentTable: '',
    results: []
  })
  const [selectedFile, setSelectedFile] = useState('')
  const [uploadedFile, setUploadedFile] = useState(null)
  const [configLoading, setConfigLoading] = useState(false)
  const [message, setMessage] = useState('')
  const [messageType, setMessageType] = useState('')
  
  // Estados da UI
  const [databaseMode, setDatabaseMode] = useState('online') // 'online' ou 'offline'
  const [savedConfigs, setSavedConfigs] = useState([])
  const [showConfigModal, setShowConfigModal] = useState(false)
  const [configName, setConfigName] = useState('')
  const [activeTab, setActiveTab] = useState('dashboard') // 'dashboard', 'config', 'logs', 'fat-tables'

  // Carregar dados iniciais
  useEffect(() => {
    loadInitialData()
    fetchConfig()
    loadSavedConfigs()
    fetchAvailableTables()
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

  // Funções do Dashboard
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
      console.error('Erro ao carregar arquivos CSV:', error)
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
      const response = await fetch('/api/migration/logs')
      if (response.ok) {
        const logsData = await response.json()
        setLogs(logsData)
      }
    } catch (error) {
      console.error('Erro ao buscar logs:', error)
    }
  }

  // Funções da Configuração
  const fetchConfig = async () => {
    try {
      const response = await fetch('/api/config')
      if (response.ok) {
        const data = await response.json()
        setConfig(data)
      }
    } catch (error) {
      console.error('Erro ao carregar configurações:', error)
    }
  }

  const fetchAvailableTables = async () => {
    try {
      const response = await fetch('/api/migration/tables')
      if (response.ok) {
        const data = await response.json()
        setAvailableTables(data)
      }
    } catch (error) {
      console.error('Erro ao carregar tabelas disponíveis:', error)
    }
  }

  const updateSelectedTable = async (tableName) => {
    try {
      setConfigLoading(true)
      const response = await fetch('/api/migration/table', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ tableName })
      })
      
      if (response.ok) {
        setConfig(prev => ({ ...prev, TABLE_NAME: tableName }))
        setMessage('Tabela de migração atualizada com sucesso!')
        setMessageType('success')
        toast.success('Tabela atualizada!')
      } else {
        const errorData = await response.json()
        setMessage(errorData.error || 'Erro ao atualizar tabela')
        setMessageType('error')
        toast.error('Erro ao atualizar tabela')
      }
    } catch (error) {
      console.error('Erro ao atualizar tabela:', error)
      setMessage('Erro de conexão ao atualizar tabela')
      setMessageType('error')
      toast.error('Erro de conexão')
    } finally {
      setConfigLoading(false)
    }
  }

  const saveConfig = async () => {
    setConfigLoading(true)
    try {
      const response = await fetch('/api/config', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(config),
      })

      if (response.ok) {
        setMessage('Configuração salva com sucesso!')
        setMessageType('success')
        toast.success('Configuração salva!')
        
        // Atualizar status do sistema
        if (onStatusUpdate) {
          onStatusUpdate(prev => ({
            ...prev,
            database: 'online',
            lastCheck: new Date().toISOString()
          }))
        }
      } else {
        throw new Error('Erro ao salvar configuração')
      }
    } catch (error) {
      setMessage('Erro ao salvar configuração: ' + error.message)
      setMessageType('error')
      toast.error('Erro ao salvar configuração')
    } finally {
      setConfigLoading(false)
      setTimeout(() => {
        setMessage('')
        setMessageType('')
      }, 3000)
    }
  }

  const testConnection = async () => {
    setConfigLoading(true)
    try {
      const response = await fetch('/api/database/test', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(config),
      })

      if (response.ok) {
        setMessage('Conexão testada com sucesso!')
        setMessageType('success')
        toast.success('Conexão OK!')
        setDatabaseMode('online')
      } else {
        throw new Error('Falha na conexão')
      }
    } catch (error) {
      setMessage('Erro na conexão: ' + error.message)
      setMessageType('error')
      toast.error('Erro na conexão')
      setDatabaseMode('offline')
    } finally {
      setConfigLoading(false)
      setTimeout(() => {
        setMessage('')
        setMessageType('')
      }, 3000)
    }
  }

  // Funções de configurações salvas
  const loadSavedConfigs = async () => {
    try {
      const response = await fetch('/api/config/saved')
      if (response.ok) {
        const configs = await response.json()
        setSavedConfigs(configs)
      }
    } catch (error) {
      console.error('Erro ao carregar configurações salvas:', error)
    }
  }

  const saveNamedConfig = async () => {
    if (!configName.trim()) {
      toast.error('Digite um nome para a configuração')
      return
    }

    try {
      const response = await fetch('/api/config/save', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          name: configName,
          config: config
        }),
      })

      if (response.ok) {
        toast.success('Configuração salva!')
        setShowConfigModal(false)
        setConfigName('')
        loadSavedConfigs()
      } else {
        throw new Error('Erro ao salvar')
      }
    } catch (error) {
      toast.error('Erro ao salvar configuração')
    }
  }

  const loadNamedConfig = async (configId) => {
    try {
      const response = await fetch(`/api/config/load/${configId}`)
      if (response.ok) {
        const data = await response.json()
        setConfig(data.config)
        toast.success(`Configuração "${data.name}" carregada!`)
      }
    } catch (error) {
      toast.error('Erro ao carregar configuração')
    }
  }

  // Função para upload de arquivo
  const handleFileUpload = async (event) => {
    const file = event.target.files[0]
    if (!file) return

    const formData = new FormData()
    formData.append('csvFile', file)

    setConfigLoading(true)
    try {
      const response = await fetch('/api/csv/upload', {
        method: 'POST',
        body: formData,
      })

      if (response.ok) {
        setMessage('Arquivo enviado com sucesso!')
        setMessageType('success')
        toast.success('Arquivo enviado!')
        fetchCsvFiles()
      } else {
        throw new Error('Erro no upload')
      }
    } catch (error) {
      setMessage('Erro no upload: ' + error.message)
      setMessageType('error')
      toast.error('Erro no upload')
    } finally {
      setConfigLoading(false)
      setTimeout(() => {
        setMessage('')
        setMessageType('')
      }, 3000)
    }
  }

  // Função para processar arquivos CSV selecionados
  const processCsvFiles = async () => {
    if (selectedFiles.length === 0) {
      toast.error('Selecione pelo menos um arquivo para processar')
      return
    }

    try {
      setMigrationStatus('running')
      toast.success(`Iniciando processamento de ${selectedFiles.length} arquivo(s)`)
      
      const response = await fetch('/api/migration/start', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          files: selectedFiles
        }),
      })

      if (response.ok) {
        const result = await response.json()
        toast.success('Processamento iniciado com sucesso!')
        
        // Atualizar estado da migração
        setMigrationProgress({
          current: 0,
          total: selectedFiles.length,
          currentFile: selectedFiles[0] || '',
          processedRecords: 0,
          errors: 0
        })
      } else {
        throw new Error('Erro ao iniciar processamento')
      }
    } catch (error) {
      console.error('Erro ao processar arquivos:', error)
      toast.error('Erro ao iniciar processamento: ' + error.message)
      setMigrationStatus('error')
    }
  }

  // Função para selecionar todos os arquivos
  const selectAllFiles = () => {
    setSelectedFiles(csvFiles.map(f => f.name))
    toast.success(`${csvFiles.length} arquivo(s) selecionado(s)`)
  }

  // Função para limpar seleção
  const clearSelection = () => {
    setSelectedFiles([])
    toast.success('Seleção limpa')
  }

  // Função para gerar arquivos SQL para múltiplas tabelas
  const generateMultipleSqlFiles = async () => {
    // Validação 1: Verificar se pelo menos uma tabela está selecionada
    if (selectedTables.length === 0) {
      console.log('❌ Erro de validação: Nenhuma tabela selecionada para migração')
      toast.error('Selecione pelo menos uma tabela para migração')
      return
    }

    // Validação 2: Verificar status do backend
    if (systemStatus.backend !== 'online') {
      console.log('❌ Erro de validação: Backend não está online', { status: systemStatus.backend })
      toast.error('Backend não está online. Verifique a conexão com o servidor.')
      return
    }

    // Validação 3: Verificar status do banco de dados
    if (systemStatus.database !== 'online') {
      console.log('❌ Erro de validação: Banco de dados não está online', { status: systemStatus.database })
      toast.error('Banco de dados não está online. Verifique a conexão com o PostgreSQL.')
      return
    }

    // Validação 4: Verificar se há arquivos CSV disponíveis
    if (csvFiles.length === 0) {
      console.log('❌ Erro de validação: Nenhum arquivo CSV disponível')
      toast.error('Nenhum arquivo CSV disponível')
      return
    }

    // Validação 5: Verificar se pelo menos um arquivo CSV está selecionado
    if (selectedFiles.length === 0) {
      console.log('❌ Erro de validação: Nenhum arquivo CSV selecionado')
      toast.error('Selecione pelo menos um arquivo CSV para processar')
      return
    }

    console.log('✅ Todas as validações passaram. Iniciando geração de SQL...', {
      tabelasSelecionadas: selectedTables.length,
      arquivosCSV: csvFiles.length,
      arquivosSelecionados: selectedFiles.length,
      statusBackend: systemStatus.backend,
      statusBanco: systemStatus.database
    })

    // DEBUG: Log detalhado dos dados que serão enviados
    console.log('🔍 DEBUG - Dados que serão enviados:', {
      selectedTables: selectedTables,
      csvFiles: selectedFiles,
      payload: {
        selectedTables: selectedTables,
        csvFiles: selectedFiles
      }
    })

    try {
      setSqlGenerationProgress({
        isGenerating: true,
        current: 0,
        total: selectedTables.length,
        currentTable: selectedTables[0],
        results: []
      })

      toast.success(`Iniciando geração de SQL para ${selectedTables.length} tabela(s)`)

      // Iniciar polling do status
      const pollInterval = setInterval(async () => {
        try {
          const statusResponse = await fetch('/api/migration/sql-generation-status')
          if (statusResponse.ok) {
            const status = await statusResponse.json()
            setSqlGenerationProgress(prev => ({
              ...prev,
              current: status.current,
              currentTable: status.currentTable,
              results: status.results
            }))
            
            // Parar polling se a geração terminou
            if (!status.isGenerating) {
              clearInterval(pollInterval)
            }
          }
        } catch (error) {
          console.error('Erro ao obter status:', error)
        }
      }, 500) // Poll a cada 500ms

      const response = await fetch('/api/migration/generate-multiple-sql', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          selectedTables: selectedTables,
          csvFiles: selectedFiles
        }),
      })

      const result = await response.json()
      clearInterval(pollInterval) // Parar polling

      if (response.ok) {
        setSqlGenerationProgress(prev => ({
          ...prev,
          isGenerating: false,
          current: prev.total,
          results: result.results
        }))

        toast.success(`Geração concluída! ${result.totalGenerated} arquivo(s) SQL gerado(s) para ${selectedTables.length} tabela(s) e ${selectedFiles.length} CSV(s)`)
        
        if (result.errors && result.errors.length > 0) {
          console.warn('Erros durante a geração:', result.errors)
          toast.error(`${result.totalErrors} erro(s) encontrado(s). Verifique o console.`)
        }
      } else {
        throw new Error(result.error || 'Erro ao gerar arquivos SQL')
      }
    } catch (error) {
      console.error('Erro ao gerar arquivos SQL:', error)
      toast.error('Erro ao gerar arquivos SQL: ' + error.message)
      setSqlGenerationProgress(prev => ({
        ...prev,
        isGenerating: false
      }))
    }
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-96">
        <LoadingSpinner size="large" text="Carregando dashboard..." />
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header com Status */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center gap-4">
          <h1 className="text-2xl font-bold text-gray-900">Sistema de Migração</h1>
          <div className="flex items-center gap-2">
            {databaseMode === 'online' ? (
              <div className="flex items-center gap-2 text-green-600">
                <Wifi className="w-4 h-4" />
                <span className="text-sm font-medium">Online</span>
              </div>
            ) : (
              <div className="flex items-center gap-2 text-red-600">
                <WifiOff className="w-4 h-4" />
                <span className="text-sm font-medium">Offline</span>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Navegação por Tabs */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="border-b border-gray-200">
          <nav className="flex space-x-8 px-6" aria-label="Tabs">
            <button
              onClick={() => setActiveTab('dashboard')}
              className={`py-4 px-1 border-b-2 font-medium text-sm transition-colors ${
                activeTab === 'dashboard'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              <div className="flex items-center gap-2">
                <FileText className="w-4 h-4" />
                Dashboard
              </div>
            </button>
            <button
              onClick={() => setActiveTab('fat-tables')}
              className={`py-4 px-1 border-b-2 font-medium text-sm transition-colors ${
                activeTab === 'fat-tables'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              <div className="flex items-center gap-2">
                <Database className="w-4 h-4" />
                Analisar
              </div>
            </button>
            <button
              onClick={() => setActiveTab('config')}
              className={`py-4 px-1 border-b-2 font-medium text-sm transition-colors ${
                activeTab === 'config'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              <div className="flex items-center gap-2">
                <Settings className="w-4 h-4" />
                Configurações
              </div>
            </button>
            <button
              onClick={() => setActiveTab('logs')}
              className={`py-4 px-1 border-b-2 font-medium text-sm transition-colors ${
                activeTab === 'logs'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              <div className="flex items-center gap-2">
                <Eye className="w-4 h-4" />
                Logs
              </div>
            </button>
          </nav>
        </div>
      </div>

      {/* Conteúdo das Tabs */}
      <div className="space-y-6">
        {/* Tab: Dashboard */}
        {activeTab === 'dashboard' && (
          <div className="space-y-6">
            {/* Status Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600">Backend</p>
                  <p className={`text-lg font-semibold ${
                    systemStatus.backend === 'online' ? 'text-green-600' : 'text-red-600'
                  }`}>
                    {systemStatus.backend === 'online' ? 'Online' : 'Offline'}
                  </p>
                </div>
                <div className={`p-3 rounded-full ${
                  systemStatus.backend === 'online' ? 'bg-green-100' : 'bg-red-100'
                }`}>
                  {systemStatus.backend === 'online' ? (
                    <CheckCircle className="w-6 h-6 text-green-600" />
                  ) : (
                    <AlertCircle className="w-6 h-6 text-red-600" />
                  )}
                </div>
              </div>
            </div>

            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600">Database</p>
                  <p className={`text-lg font-semibold ${
                    databaseMode === 'online' ? 'text-green-600' : 'text-red-600'
                  }`}>
                    {databaseMode === 'online' ? 'Online' : 'Offline'}
                  </p>
                </div>
                <div className={`p-3 rounded-full ${
                  databaseMode === 'online' ? 'bg-green-100' : 'bg-red-100'
                }`}>
                  <Database className={`w-6 h-6 ${
                    databaseMode === 'online' ? 'text-green-600' : 'text-red-600'
                  }`} />
                </div>
              </div>
            </div>

            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600">Arquivos CSV</p>
                  <p className="text-lg font-semibold text-blue-600">{csvFiles.length}</p>
                </div>
                <div className="p-3 rounded-full bg-blue-100">
                  <FileText className="w-6 h-6 text-blue-600" />
                </div>
              </div>
            </div>

            <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
              <div className="flex items-center justify-between mb-4">
                <div>
                  <p className="text-sm font-medium text-gray-600">Status</p>
                  <p className={`text-lg font-semibold ${
                    migrationStatus === 'running' ? 'text-yellow-600' :
                    migrationStatus === 'completed' ? 'text-green-600' :
                    migrationStatus === 'error' ? 'text-red-600' : 'text-gray-600'
                  }`}>
                    {migrationStatus === 'idle' ? 'Aguardando' :
                     migrationStatus === 'running' ? 'Executando' :
                     migrationStatus === 'completed' ? 'Concluído' :
                     migrationStatus === 'error' ? 'Erro' : 'Pausado'}
                  </p>
                </div>
                <div className={`p-3 rounded-full ${
                  migrationStatus === 'running' ? 'bg-yellow-100' :
                  migrationStatus === 'completed' ? 'bg-green-100' :
                  migrationStatus === 'error' ? 'bg-red-100' : 'bg-gray-100'
                }`}>
                  {migrationStatus === 'running' ? (
                    <Clock className="w-6 h-6 text-yellow-600" />
                  ) : migrationStatus === 'completed' ? (
                    <CheckCircle className="w-6 h-6 text-green-600" />
                  ) : migrationStatus === 'error' ? (
                    <AlertCircle className="w-6 h-6 text-red-600" />
                  ) : (
                    <Pause className="w-6 h-6 text-gray-600" />
                  )}
                </div>
              </div>
              
              {/* Barra de Progresso */}
              {(migrationStatus === 'running' || migrationStatus === 'completed') && (
                <div className="space-y-2">
                  <div className="flex justify-between text-xs text-gray-600">
                    <span>
                      {migrationProgress.current} de {migrationProgress.total} arquivos
                    </span>
                    <span>
                      {migrationProgress.total > 0 
                        ? Math.round((migrationProgress.current / migrationProgress.total) * 100)
                        : 0}%
                    </span>
                  </div>
                  <div className="w-full bg-gray-200 rounded-full h-2">
                    <div 
                      className={`h-2 rounded-full transition-all duration-300 ${
                        migrationStatus === 'completed' ? 'bg-green-500' : 'bg-yellow-500'
                      }`}
                      style={{ 
                        width: `${migrationProgress.total > 0 
                          ? (migrationProgress.current / migrationProgress.total) * 100 
                          : 0}%` 
                      }}
                    ></div>
                  </div>
                  {migrationProgress.currentFile && (
                    <p className="text-xs text-gray-500 truncate">
                      Processando: {migrationProgress.currentFile}
                    </p>
                  )}
                </div>
              )}
            </div>
          </div>


          </div>
        )}

        {/* Tab: Tabelas FAT */}
        {activeTab === 'fat-tables' && (
          <FATTablesViewer />
        )}

        {/* Tab: Configurações */}
        {activeTab === 'config' && (
          <div className="space-y-4">
            {/* 1. Configuração do Banco de Dados */}
            <div className="bg-white rounded-lg shadow-sm border border-gray-200">
              <div className="p-4 border-b border-gray-200">
                <div className="flex items-center justify-between">
                  <h2 className="text-lg font-semibold text-gray-900">Configuração do Banco de Dados</h2>
                  <button
                    onClick={() => setShowConfigModal(true)}
                    className="flex items-center gap-2 px-3 py-1.5 text-sm font-medium text-blue-600 hover:text-blue-700 transition-colors"
                  >
                    <Save className="w-4 h-4" />
                    Salvar Config
                  </button>
                </div>
              </div>
              <div className="p-4">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Nome do Banco
                    </label>
                    <input
                      type="text"
                      value={config.POSTGRES_DB}
                      onChange={(e) => setConfig({...config, POSTGRES_DB: e.target.value})}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm"
                      placeholder="nome_do_banco"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Usuário
                    </label>
                    <input
                      type="text"
                      value={config.POSTGRES_USER}
                      onChange={(e) => setConfig({...config, POSTGRES_USER: e.target.value})}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm"
                      placeholder="usuario"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Senha
                    </label>
                    <input
                      type="password"
                      value={config.POSTGRES_PASSWORD}
                      onChange={(e) => setConfig({...config, POSTGRES_PASSWORD: e.target.value})}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm"
                      placeholder="senha"
                    />
                  </div>
                </div>

                {/* Seletor de Tabelas */}
                <div className="mt-4 pt-4 border-t border-gray-200">
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Tabelas para Migração
                  </label>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                    {availableTables.map((table) => (
                      <div
                        key={table.name}
                        className={`p-3 border rounded-lg cursor-pointer transition-all ${
                          selectedTables.includes(table.name)
                            ? 'border-blue-500 bg-blue-50'
                            : 'border-gray-200 hover:border-gray-300'
                        }`}
                        onClick={() => {
                          setSelectedTables(prev => 
                            prev.includes(table.name)
                              ? prev.filter(t => t !== table.name)
                              : [...prev, table.name]
                          )
                        }}
                      >
                        <div className="flex items-center justify-between">
                          <div>
                            <p className="text-sm font-medium text-gray-900">
                              {table.displayName}
                            </p>
                            <p className="text-xs text-gray-500">
                              {table.category}
                            </p>
                          </div>
                          <input
                            type="checkbox"
                            checked={selectedTables.includes(table.name)}
                            onChange={() => {}}
                            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                  {selectedTables.length > 0 && (
                    <div className="mt-3 p-3 bg-blue-50 rounded-md">
                      <p className="text-sm text-blue-700">
                        {selectedTables.length} tabela(s) selecionada(s) para migração
                      </p>
                      <div className="mt-3 flex gap-2">
                        <button
                          onClick={generateMultipleSqlFiles}
                          disabled={
                            selectedTables.length === 0 || 
                            csvFiles.length === 0 || 
                            migrationStatus === 'running' || 
                            sqlGenerationProgress.isGenerating ||
                            systemStatus.backend !== 'online' ||
                            systemStatus.database !== 'online'
                          }
                          className="px-4 py-2 bg-green-600 text-white text-sm rounded-md hover:bg-green-700 disabled:bg-gray-400 disabled:cursor-not-allowed flex items-center gap-2"
                        >
                          <FileText className="w-4 h-4" />
                          {sqlGenerationProgress.isGenerating ? 'Gerando...' : 'Gerar Arquivos SQL'}
                        </button>
                        <button
                          onClick={() => setSelectedTables([])}
                          className="px-4 py-2 bg-gray-500 text-white text-sm rounded-md hover:bg-gray-600 flex items-center gap-2"
                        >
                          <X className="w-4 h-4" />
                          Limpar Seleção
                        </button>
                      </div>

                      {/* Feedback de Condições para Geração */}
                      {(selectedTables.length === 0 || 
                        csvFiles.length === 0 || 
                        systemStatus.backend !== 'online' || 
                        systemStatus.database !== 'online') && (
                        <div className="mt-3 p-3 bg-yellow-50 border border-yellow-200 rounded-md">
                          <h4 className="text-sm font-medium text-yellow-800 mb-2">
                            Condições necessárias para gerar arquivos SQL:
                          </h4>
                          <ul className="text-xs text-yellow-700 space-y-1">
                            <li className={`flex items-center gap-2 ${selectedTables.length > 0 ? 'text-green-600' : 'text-red-600'}`}>
                              {selectedTables.length > 0 ? '✅' : '❌'} 
                              Pelo menos 1 tabela selecionada ({selectedTables.length} selecionada{selectedTables.length !== 1 ? 's' : ''})
                            </li>
                            <li className={`flex items-center gap-2 ${csvFiles.length > 0 ? 'text-green-600' : 'text-red-600'}`}>
                              {csvFiles.length > 0 ? '✅' : '❌'} 
                              Arquivos CSV disponíveis ({csvFiles.length} arquivo{csvFiles.length !== 1 ? 's' : ''})
                            </li>
                            <li className={`flex items-center gap-2 ${systemStatus.backend === 'online' ? 'text-green-600' : 'text-red-600'}`}>
                              {systemStatus.backend === 'online' ? '✅' : '❌'} 
                              Backend online (Status: {systemStatus.backend})
                            </li>
                            <li className={`flex items-center gap-2 ${systemStatus.database === 'online' ? 'text-green-600' : 'text-red-600'}`}>
                              {systemStatus.database === 'online' ? '✅' : '❌'} 
                              Banco de dados online (Status: {systemStatus.database})
                            </li>
                          </ul>
                        </div>
                      )}

                      {/* Barra de Progresso da Geração de SQL */}
                      {sqlGenerationProgress.isGenerating && (
                        <div className="mt-4 p-3 bg-green-50 rounded-md border border-green-200">
                          <div className="flex justify-between text-sm text-green-700 mb-2">
                            <span>Gerando arquivos SQL...</span>
                            <span>
                              {sqlGenerationProgress.current} de {sqlGenerationProgress.total} tabelas
                            </span>
                          </div>
                          <div className="w-full bg-green-200 rounded-full h-2">
                            <div 
                              className="h-2 bg-green-500 rounded-full transition-all duration-300"
                              style={{ 
                                width: `${sqlGenerationProgress.total > 0 
                                  ? (sqlGenerationProgress.current / sqlGenerationProgress.total) * 100 
                                  : 0}%` 
                              }}
                            ></div>
                          </div>
                          {sqlGenerationProgress.currentTable && (
                            <p className="text-xs text-green-600 mt-2">
                              Processando: {sqlGenerationProgress.currentTable}
                            </p>
                          )}
                        </div>
                      )}

                      {/* Resultados da Geração */}
                      {sqlGenerationProgress.results.length > 0 && (
                        <div className="mt-4 p-3 bg-gray-50 rounded-md border border-gray-200">
                          <h4 className="text-sm font-medium text-gray-900 mb-2">
                            Arquivos SQL Gerados ({sqlGenerationProgress.results.length})
                          </h4>
                          <div className="text-xs text-gray-600 mb-2">
                            {selectedTables.length} tabela(s) × {selectedFiles.length} arquivo(s) CSV = {sqlGenerationProgress.results.length} arquivo(s) SQL
                          </div>
                          <div className="space-y-1 max-h-32 overflow-y-auto">
                            {sqlGenerationProgress.results.map((result, index) => (
                              <div key={index} className="text-xs text-gray-600 flex justify-between">
                                <span>{result.sqlFile}</span>
                                <span className="text-gray-400">
                                  {(result.fileSize / 1024).toFixed(1)} KB
                                </span>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </div>
                
                {message && (
                  <div className={`mt-3 p-3 rounded-md text-sm ${
                    messageType === 'success' ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-700'
                  }`}>
                    {message}
                  </div>
                )}
                
                <div className="flex gap-3 mt-4">
                  <button
                    onClick={testConnection}
                    disabled={configLoading}
                    className="flex items-center gap-2 px-3 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors text-sm"
                  >
                    {configLoading ? (
                      <LoadingSpinner size="small" />
                    ) : (
                      <TestTube className="w-4 h-4" />
                    )}
                    Testar Conexão
                  </button>
                  <button
                    onClick={saveConfig}
                    disabled={configLoading}
                    className="flex items-center gap-2 px-3 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors text-sm"
                  >
                    {configLoading ? (
                      <LoadingSpinner size="small" />
                    ) : (
                      <Save className="w-4 h-4" />
                    )}
                    Salvar
                  </button>
                </div>
              </div>
            </div>

            {/* 2. Configurações Salvas */}
            {savedConfigs.length > 0 && (
              <div className="bg-white rounded-lg shadow-sm border border-gray-200">
                <div className="p-4 border-b border-gray-200">
                  <h2 className="text-lg font-semibold text-gray-900">Configurações Salvas</h2>
                </div>
                <div className="p-4">
                  <div className="space-y-2">
                    {savedConfigs.map((savedConfig) => (
                      <div
                        key={savedConfig.id}
                        className="flex items-center justify-between p-3 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
                      >
                        <div>
                          <p className="font-medium text-gray-900 text-sm">{savedConfig.name}</p>
                          <p className="text-xs text-gray-500">
                            {savedConfig.config.POSTGRES_HOST}:{savedConfig.config.POSTGRES_PORT}/{savedConfig.config.POSTGRES_DB}
                          </p>
                        </div>
                        <button
                          onClick={() => loadNamedConfig(savedConfig.id)}
                          className="px-3 py-1.5 text-sm font-medium text-blue-600 hover:text-blue-700 transition-colors"
                        >
                          Carregar
                        </button>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            )}

            {/* 3. Upload de Arquivos CSV */}
            <div className="bg-white rounded-lg shadow-sm border border-gray-200">
              <div className="p-4 border-b border-gray-200">
                <h2 className="text-lg font-semibold text-gray-900">Upload de Arquivos CSV</h2>
              </div>
              <div className="p-4">
                <div className="border-2 border-dashed border-gray-300 rounded-lg p-4 text-center hover:border-gray-400 transition-colors">
                  <Upload className="w-8 h-8 text-gray-400 mx-auto mb-2" />
                  <p className="text-gray-600 mb-2 text-sm">Arraste um arquivo CSV aqui ou clique para selecionar</p>
                  <input
                    type="file"
                    accept=".csv"
                    onChange={handleFileUpload}
                    className="hidden"
                    id="file-upload"
                  />
                  <label
                    htmlFor="file-upload"
                    className="inline-flex items-center gap-2 px-3 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 cursor-pointer transition-colors text-sm"
                  >
                    <Upload className="w-4 h-4" />
                    Selecionar Arquivo
                  </label>
                </div>
              </div>
            </div>

            {/* 4. Arquivos CSV */}
            <div className="bg-white rounded-lg shadow-sm border border-gray-200">
              <div className="p-4 border-b border-gray-200">
                <div className="flex items-center justify-between">
                  <h2 className="text-lg font-semibold text-gray-900">Arquivos CSV</h2>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={processCsvFiles}
                      disabled={selectedFiles.length === 0 || migrationStatus === 'running'}
                      className="flex items-center gap-2 px-3 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors text-sm"
                    >
                      <Play className="w-4 h-4" />
                      Gerar
                    </button>
                    {csvFiles.length > 0 && (
                      <button
                        onClick={selectedFiles.length === csvFiles.length ? clearSelection : selectAllFiles}
                        className="flex items-center gap-2 px-2 py-2 text-sm font-medium text-blue-600 hover:text-blue-700 transition-colors"
                      >
                        <CheckCircle className="w-4 h-4" />
                        {selectedFiles.length === csvFiles.length ? 'Limpar' : 'Todos'}
                      </button>
                    )}
                    <button
                      onClick={fetchCsvFiles}
                      className="flex items-center gap-2 px-2 py-2 text-sm font-medium text-gray-600 hover:text-gray-900 transition-colors"
                    >
                      <RefreshCw className="w-4 h-4" />
                    </button>
                  </div>
                </div>
              </div>
              <div className="p-4">
                {csvFiles.length === 0 ? (
                  <div className="text-center py-6">
                    <FileText className="w-10 h-10 text-gray-400 mx-auto mb-3" />
                    <p className="text-gray-500 text-sm">Nenhum arquivo CSV encontrado</p>
                    <p className="text-xs text-gray-400 mt-1">
                      Faça upload de arquivos acima
                    </p>
                  </div>
                ) : (
                  <div className="space-y-2">
                    {csvFiles.map((file, index) => (
                      <div
                        key={index}
                        className="flex items-center justify-between p-3 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
                      >
                        <div className="flex items-center gap-3">
                          <input
                            type="checkbox"
                            checked={selectedFiles.includes(file.name)}
                            onChange={(e) => {
                              if (e.target.checked) {
                                setSelectedFiles([...selectedFiles, file.name])
                              } else {
                                setSelectedFiles(selectedFiles.filter(f => f !== file.name))
                              }
                            }}
                            className="w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 rounded focus:ring-blue-500 focus:ring-2"
                          />
                          <FileText className="w-4 h-4 text-blue-600" />
                          <div>
                            <p className="font-medium text-gray-900 text-sm">{file.name}</p>
                            <p className="text-xs text-gray-500">
                              {file.size} • {file.modified}
                            </p>
                          </div>
                        </div>
                        <div className="flex items-center gap-1">
                          <button className="p-1.5 text-gray-400 hover:text-blue-600 transition-colors">
                            <Eye className="w-4 h-4" />
                          </button>
                          <button className="p-1.5 text-gray-400 hover:text-green-600 transition-colors">
                            <Download className="w-4 h-4" />
                          </button>
                          <button className="p-1.5 text-gray-400 hover:text-red-600 transition-colors">
                            <Trash2 className="w-4 h-4" />
                          </button>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Tab: Logs */}
        {activeTab === 'logs' && (
          <PostgreSQLLogViewer />
        )}

          {/* Modal para Salvar Configuração */}
          {showConfigModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 w-full max-w-md">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Salvar Configuração</h3>
            <div className="mb-4">
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Nome da Configuração
              </label>
              <input
                type="text"
                value={configName}
                onChange={(e) => setConfigName(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                placeholder="Ex: Produção, Desenvolvimento..."
              />
            </div>
            <div className="flex gap-3 justify-end">
              <button
                onClick={() => {
                  setShowConfigModal(false)
                  setConfigName('')
                }}
                className="px-4 py-2 text-gray-600 hover:text-gray-800 transition-colors"
              >
                Cancelar
              </button>
              <button
                onClick={saveNamedConfig}
                className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
              >
                Salvar
              </button>
            </div>
          </div>
        </div>
      )}

        </div>

      {configLoading && <OverlaySpinner />}
    </div>
  )
}

export default UnifiedDashboard