import React, { useState, useEffect } from 'react'
import { 
  Database, 
  Users, 
  FileText, 
  Eye, 
  RefreshCw, 
  Search,
  Filter,
  Download,
  AlertCircle,
  CheckCircle,
  Clock,
  BarChart3,
  Play,
  FileCode,
  Upload
} from 'lucide-react'
import toast from 'react-hot-toast'
import LoadingSpinner from './LoadingSpinner'

const FATTablesViewer = () => {
  const [activeTable, setActiveTable] = useState('tb_fat_cad_individual')
  const [tableData, setTableData] = useState([])
  const [isLoading, setIsLoading] = useState(false)
  const [searchTerm, setSearchTerm] = useState('')
  const [currentPage, setCurrentPage] = useState(1)
  const [totalRecords, setTotalRecords] = useState(0)
  const [tableStats, setTableStats] = useState({})
  const [migrationStatus, setMigrationStatus] = useState('idle') // idle, running, completed, error
  const [migrationResult, setMigrationResult] = useState(null)
  const recordsPerPage = 20

  // Definição das tabelas FAT disponíveis
  const fatTables = [
    {
      id: 'tb_fat_cad_individual',
      name: 'tb_fat_cad_individual',
      description: 'Tabela tb_fat_cad_individual',
      icon: FileText,
      color: 'bg-blue-500'
    },
    {
      id: 'tb_fat_cidadao',
      name: 'tb_fat_cidadao',
      description: 'Tabela tb_fat_cidadao',
      icon: Users,
      color: 'bg-green-500'
    },
    {
      id: 'tb_fat_cidadao_pec',
      name: 'tb_fat_cidadao_pec',
      description: 'Tabela tb_fat_cidadao_pec',
      icon: Database,
      color: 'bg-purple-500'
    },
    {
      id: 'tb_cidadao',
      name: 'tb_cidadao',
      description: 'Tabela tb_cidadao',
      icon: Users,
      color: 'bg-orange-500'
    },
    {
      id: 'tl_cds_cad_individual',
      name: 'tl_cds_cad_individual',
      description: 'Tabela tl_cds_cad_individual',
      icon: FileText,
      color: 'bg-indigo-500'
    },
    {
      id: 'tb_cds_cad_individual',
      name: 'tb_cds_cad_individual',
      description: 'Tabela tb_cds_cad_individual',
      icon: FileText,
      color: 'bg-cyan-500'
    }
  ]

  // Carregar dados da tabela selecionada
  const loadTableData = async (tableName, page = 1, search = '') => {
    setIsLoading(true)
    try {
      const params = new URLSearchParams({
        table: tableName,
        page: page.toString(),
        limit: recordsPerPage.toString(),
        ...(search && { search })
      })

      const response = await fetch(`/api/fat-tables/data?${params}`)
      if (!response.ok) {
        throw new Error('Erro ao carregar dados da tabela')
      }

      const data = await response.json()
      setTableData(data.records || [])
      setTotalRecords(data.total || 0)
      
      toast.success(`${data.records?.length || 0} registros carregados`)
    } catch (error) {
      console.error('Erro ao carregar dados:', error)
      toast.error('Erro ao carregar dados da tabela')
      setTableData([])
      setTotalRecords(0)
    } finally {
      setIsLoading(false)
    }
  }

  // Carregar estatísticas das tabelas
  const loadTableStats = async () => {
    try {
      const response = await fetch('/api/fat-tables/stats')
      if (response.ok) {
        const stats = await response.json()
        setTableStats(stats)
      }
    } catch (error) {
      console.error('Erro ao carregar estatísticas:', error)
    }
  }

  // Efeito para carregar dados quando a tabela ativa muda
  useEffect(() => {
    loadTableData(activeTable, 1, searchTerm)
    loadTableStats()
  }, [activeTable])

  // Efeito para pesquisa com debounce
  useEffect(() => {
    const timeoutId = setTimeout(() => {
      if (searchTerm !== '') {
        loadTableData(activeTable, 1, searchTerm)
        setCurrentPage(1)
      } else {
        loadTableData(activeTable, currentPage)
      }
    }, 500)

    return () => clearTimeout(timeoutId)
  }, [searchTerm])

  // Função para mudança de página
  const handlePageChange = (newPage) => {
    setCurrentPage(newPage)
    loadTableData(activeTable, newPage, searchTerm)
  }

  // Função para exportar dados
  const exportTableData = async () => {
    try {
      const response = await fetch(`/api/fat-tables/export?table=${activeTable}`)
      if (!response.ok) {
        throw new Error('Erro ao exportar dados')
      }

      const blob = await response.blob()
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.style.display = 'none'
      a.href = url
      a.download = `${activeTable}_export.csv`
      document.body.appendChild(a)
      a.click()
      window.URL.revokeObjectURL(url)
      
      toast.success('Dados exportados com sucesso!')
    } catch (error) {
      console.error('Erro ao exportar:', error)
      toast.error('Erro ao exportar dados')
    }
  }

  // Função para executar migração de qualquer tabela
  const executeMigration = async (generateOnly = false) => {
    // Verificar se a tabela suporta migração
    const migrableTables = [
      'tb_cidadao', 
      'tb_fat_cidadao', 
      'tb_fat_cidadao_pec', 
      'tb_fat_cad_individual',
      'tb_cds_cad_individual',
      'tl_cds_cad_individual'
    ];
    
    if (!migrableTables.includes(activeTable)) {
      toast.error(`Migração não disponível para ${activeTable}`)
      return
    }

    setMigrationStatus('running')
    setMigrationResult(null)

    try {
      const response = await fetch(`/api/migration/table/${activeTable}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          generateOnly: generateOnly
        })
      })

      const result = await response.json()

      if (!response.ok) {
        throw new Error(result.error || 'Erro na migração')
      }

      setMigrationStatus('completed')
      setMigrationResult(result)
      
      if (generateOnly) {
        toast.success('Script SQL gerado com sucesso!')
      } else {
        toast.success('Migração executada com sucesso!')
        // Recarregar dados da tabela
        loadTableData(activeTable, currentPage, searchTerm)
        loadTableStats()
      }
    } catch (error) {
      console.error('Erro na migração:', error)
      setMigrationStatus('error')
      setMigrationResult({ error: error.message })
      toast.error(error.message || 'Erro na migração')
    }
  }

  // Função para baixar script SQL gerado
  const downloadGeneratedSQL = () => {
    if (!migrationResult?.sqlContent) {
      toast.error('Nenhum script SQL disponível')
      return
    }

    const blob = new Blob([migrationResult.sqlContent], { type: 'text/sql' })
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.style.display = 'none'
    a.href = url
    a.download = `tb_cidadao_migration_${new Date().toISOString().split('T')[0]}.sql`
    document.body.appendChild(a)
    a.click()
    window.URL.revokeObjectURL(url)
    
    toast.success('Script SQL baixado com sucesso!')
  }

  // Obter campos de pesquisa específicos para cada tabela
  const getSearchFields = () => {
    const baseFields = ['nome', 'CNS']
    
    switch (activeTable) {
      case 'tb_fat_cad_individual':
        return [...baseFields, 'UUID ficha', 'data cadastro']
      case 'tb_fat_cidadao':
        return ['CNS', 'UUID ficha', 'UUID ficha origem']
      case 'tb_fat_cidadao_pec':
        return ['Unidade saúde vinc', 'Equipe vinc', 'CNS', 'CPF', 'Nome cidadão', 'Tempo nascimento', 'Telefone celular', 'UUID ficha']
      case 'tb_cidadao':
        return ['CNS', 'CPF', 'Nome cidadão', 'Sexo', 'Data nascimento', 'Telefone celular', 'Raça/cor', 'Nacionalidade', 'País nascimento', 'Único cidadão', 'Única ficha']
      case 'tb_cds_cad_individual':
        return [
          'Micro Área', 
          'CPF Cidadão', 
          'Nome Cidadão', 
          'Sexo', 
          'Data Nascimento', 
          'Celular Cidadão', 
          'Raça/Cor', 
          'Nacionalidade', 
          'Único Ficha Origem'
        ]
      case 'tl_cds_cad_individual':
        return ['Micro área', 'CPF cidadão', 'Nome cidadão', 'Data nascimento', 'Celular cidadão', 'Raça/cor', 'Nacionalidade', 'Único ficha origem']
      default:
        return baseFields
    }
  }

  // Renderizar cabeçalhos da tabela baseado na tabela ativa
  const renderTableHeaders = () => {
    const commonHeaders = ['ID', 'CNS', 'Nome', 'Data Nascimento', 'Sexo', 'Unidade Saúde']
    
    switch (activeTable) {
      case 'tb_fat_cad_individual':
        return [
          'CNS', 
          'CPF', 
          'Sexo', 
          'Data Nascimento', 
          'Raça/Cor', 
          'Nacionalidade', 
          'País Nascimento', 
          'UUID Ficha', 
          'UUID Ficha Origem'
        ]
      case 'tb_fat_cidadao':
        return [
          'CNS', 
          'UUID Ficha', 
          'UUID Ficha Origem'
        ]
      case 'tb_fat_cidadao_pec':
        return [
          'Unidade Saúde Vinc', 
          'Equipe Vinc', 
          'CNS', 
          'CPF', 
          'Nome Cidadão', 
          'Tempo Nascimento', 
          'Telefone Celular', 
          'UUID Ficha'
        ]
      case 'tb_cidadao':
        return [
          'CNS', 
          'CPF', 
          'Nome Cidadão', 
          'Sexo', 
          'Data Nascimento', 
          'Telefone Celular', 
          'Raça/Cor', 
          'Nacionalidade', 
          'País Nascimento', 
          'Único Cidadão', 
          'Única Ficha'
        ]
      default:
        return commonHeaders
    }
  }

  // Renderizar linha da tabela
  const renderTableRow = (record, index) => {
    return (
      <tr key={index} className="hover:bg-gray-50 border-b border-gray-200">
        {activeTable === 'tb_fat_cad_individual' ? (
          <>
            <td className="px-4 py-3 text-sm text-gray-900">{record.nu_cns || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.nu_cpf_cidadao || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.co_dim_sexo || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.dt_nascimento || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.co_dim_raca_cor || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.co_dim_nacionalidade || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.co_dim_pais_nascimento || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-600 font-mono text-xs w-80 min-w-80 break-all">{record.nu_uuid_ficha || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-600 font-mono text-xs w-80 min-w-80 break-all">{record.nu_uuid_ficha_origem || '-'}</td>
          </>
        ) : activeTable === 'tb_fat_cidadao' ? (
          <>
            <td className="px-4 py-3 text-sm text-gray-900">{record.nu_cns || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-600 font-mono text-xs w-80 min-w-80 break-all">{record.nu_uuid_ficha || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-600 font-mono text-xs w-80 min-w-80 break-all">{record.nu_uuid_ficha_origem || '-'}</td>
          </>
        ) : activeTable === 'tb_fat_cidadao_pec' ? (
          <>
            <td className="px-4 py-3 text-sm text-gray-900">{record.co_dim_unidade_saude_vinc || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.co_dim_equipe_vinc || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.nu_cns || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.nu_cpf_cidadao || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900 font-medium">{record.no_cidadao || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.co_dim_tempo_nascimento || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.nu_telefone_celular || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-600 font-mono text-xs w-80 min-w-80 break-all">{record.nu_uuid_ficha || '-'}</td>
          </>
        ) : activeTable === 'tb_cidadao' ? (
          <>
            <td className="px-4 py-3 text-sm text-gray-900">{record.nu_cns || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.nu_cpf || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900 font-medium w-64 min-w-64">{record.no_cidadao || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.no_sexo || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.dt_nascimento || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.nu_telefone_celular || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.co_raca_cor || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.co_nacionalidade || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.co_pais_nascimento || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900 font-mono text-xs w-80 min-w-80 break-all">{record.co_unico_cidadao || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900 font-mono text-xs w-80 min-w-80 break-all">{record.co_unico_ultima_ficha || '-'}</td>
          </>
        ) : activeTable === 'tb_cds_cad_individual' ? (
          <>
            <td className="px-4 py-3 text-sm text-gray-900">{record.nu_micro_area || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.nu_cpf_cidadao || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900 font-medium">{record.no_cidadao || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.no_sexo || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.dt_nascimento || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.nu_celular_cidadao || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.co_raca_cor || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.co_nacionalidade || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.co_unico_ficha_origem || '-'}</td>
          </>
        ) : activeTable === 'tl_cds_cad_individual' ? (
          <>
            <td className="px-4 py-3 text-sm text-gray-900">{record.nu_micro_area || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.nu_cpf_cidadao || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900 font-medium">{record.no_cidadao || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.dt_nascimento || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.nu_celular_cidadao || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.co_raca_cor || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.co_nacionalidade || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.co_unico_ficha_origem || '-'}</td>
          </>
        ) : (
          <>
            <td className="px-4 py-3 text-sm text-gray-900">{record.id || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.cns || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900 font-medium">{record.nome || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.data_nascimento || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.sexo || '-'}</td>
            <td className="px-4 py-3 text-sm text-gray-900">{record.unidade_saude || '-'}</td>
          </>
        )}
      </tr>
    )
  }

  const totalPages = Math.ceil(totalRecords / recordsPerPage)

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <Database className="h-6 w-6 text-blue-600" />
            Tabelas (e-SUS)
          </h2>
          <p className="text-gray-600 mt-1">
            Visualização e gerenciamento das tabelas fato do e-SUS
          </p>
        </div>
        
        <div className="flex items-center gap-3">
          <button
            onClick={() => loadTableData(activeTable, currentPage, searchTerm)}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            <RefreshCw className="h-4 w-4" />
            Atualizar
          </button>
          
          <button
            onClick={exportTableData}
            className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors"
          >
            <Download className="h-4 w-4" />
            Exportar
          </button>

          {/* Botões de migração para tabelas suportadas */}
          {['tb_cidadao', 'tb_fat_cidadao', 'tb_fat_cidadao_pec', 'tb_fat_cad_individual', 'tb_cds_cad_individual', 'tl_cds_cad_individual'].includes(activeTable) && (
            <>
              <button
                onClick={() => executeMigration(true)}
                disabled={migrationStatus === 'running'}
                className="flex items-center gap-2 px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <FileCode className="h-4 w-4" />
                {migrationStatus === 'running' ? 'Gerando...' : 'Gerar SQL'}
              </button>
              
              <button
                onClick={() => executeMigration(false)}
                disabled={migrationStatus === 'running'}
                className="flex items-center gap-2 px-4 py-2 bg-orange-600 text-white rounded-lg hover:bg-orange-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <Play className="h-4 w-4" />
                {migrationStatus === 'running' ? 'Migrando...' : 'Executar Migração'}
              </button>
            </>
          )}
        </div>
      </div>

      {/* Estatísticas das Tabelas */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {fatTables.map((table) => {
          const Icon = table.icon
          const stats = tableStats[table.id] || {}
          
          return (
            <div
              key={table.id}
              onClick={() => setActiveTable(table.id)}
              className={`p-4 rounded-lg border-2 cursor-pointer transition-all ${
                activeTable === table.id
                  ? 'border-blue-500 bg-blue-50'
                  : 'border-gray-200 hover:border-gray-300 bg-white'
              }`}
            >
              <div className="flex items-center gap-3">
                <div className={`p-2 rounded-lg ${table.color} text-white`}>
                  <Icon className="h-5 w-5" />
                </div>
                <div className="flex-1">
                  <h3 className="font-semibold text-gray-900 text-sm">{table.name}</h3>
                  <p className="text-xs text-gray-600 mt-1">{table.description}</p>
                </div>
              </div>
              
              <div className="mt-3 flex items-center justify-between">
                <span className="text-2xl font-bold text-gray-900">
                  {stats.total || 0}
                </span>
                <span className="text-xs text-gray-500">registros</span>
              </div>
            </div>
          )
        })}
      </div>

      {/* Resultado da Migração para tabelas suportadas */}
      {['tb_cidadao', 'tb_fat_cidadao', 'tb_fat_cidadao_pec', 'tb_fat_cad_individual', 'tb_cds_cad_individual', 'tl_cds_cad_individual'].includes(activeTable) && migrationResult && (
        <div className={`p-4 rounded-lg border-2 ${
          migrationStatus === 'completed' ? 'border-green-500 bg-green-50' :
          migrationStatus === 'error' ? 'border-red-500 bg-red-50' :
          'border-gray-300 bg-gray-50'
        }`}>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              {migrationStatus === 'completed' ? (
                <CheckCircle className="h-5 w-5 text-green-600" />
              ) : migrationStatus === 'error' ? (
                <AlertCircle className="h-5 w-5 text-red-600" />
              ) : (
                <Clock className="h-5 w-5 text-gray-600" />
              )}
              <div>
                <h3 className="font-semibold text-gray-900">
                  {migrationStatus === 'completed' ? 'Migração Concluída' :
                   migrationStatus === 'error' ? 'Erro na Migração' :
                   'Processando Migração'} - {activeTable}
                </h3>
                {migrationResult.error ? (
                  <p className="text-sm text-red-600">{migrationResult.error}</p>
                ) : migrationResult.message ? (
                  <p className="text-sm text-gray-600">{migrationResult.message}</p>
                ) : null}
              </div>
            </div>
            
            {migrationResult.sqlContent && (
              <button
                onClick={downloadGeneratedSQL}
                className="flex items-center gap-2 px-3 py-1 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors text-sm"
              >
                <Download className="h-4 w-4" />
                Baixar SQL
              </button>
            )}
          </div>
          
          {migrationResult.recordsProcessed && (
            <div className="mt-3 grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className="text-gray-600">Registros processados:</span>
                <span className="ml-2 font-semibold">{migrationResult.recordsProcessed}</span>
              </div>
              <div>
                <span className="text-gray-600">Tempo de execução:</span>
                <span className="ml-2 font-semibold">{migrationResult.executionTime || 'N/A'}</span>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Barra de Pesquisa */}
      <div className="flex items-center gap-4">
        <div className="flex-1 relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
          <input
            type="text"
            placeholder={`Pesquisar por ${getSearchFields().join(', ')}...`}
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
        </div>
        
        <div className="text-sm text-gray-600">
          {totalRecords} registros encontrados
        </div>
      </div>

      {/* Tabela de Dados */}
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                {renderTableHeaders().map((header, index) => {
                  // Definir larguras específicas para colunas UUID
                  let headerClass = "px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                  
                  if (activeTable === 'tb_fat_cad_individual' || activeTable === 'tb_fat_cidadao' || activeTable === 'tb_fat_cidadao_pec') {
                    if (header === 'UUID Ficha' || header === 'UUID Ficha Origem') {
                      headerClass += " w-80 min-w-80"
                    }
                  }
                  
                  return (
                    <th
                      key={index}
                      className={headerClass}
                    >
                      {header}
                    </th>
                  )
                })}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {isLoading ? (
                <tr>
                  <td colSpan={renderTableHeaders().length} className="px-4 py-8 text-center">
                    <LoadingSpinner size="medium" text="Carregando dados..." />
                  </td>
                </tr>
              ) : tableData.length === 0 ? (
                <tr>
                  <td colSpan={renderTableHeaders().length} className="px-4 py-8 text-center text-gray-500">
                    <AlertCircle className="h-8 w-8 mx-auto mb-2 text-gray-400" />
                    Nenhum registro encontrado
                  </td>
                </tr>
              ) : (
                tableData.map((record, index) => renderTableRow(record, index))
              )}
            </tbody>
          </table>
        </div>

        {/* Paginação */}
        {totalPages > 1 && (
          <div className="bg-gray-50 px-3 py-2 border-t border-gray-200 flex items-center justify-between text-xs">
            <div className="text-gray-600">
              {((currentPage - 1) * recordsPerPage) + 1}-{Math.min(currentPage * recordsPerPage, totalRecords)} de {totalRecords}
            </div>
            
            <div className="flex items-center gap-1">
              <button
                onClick={() => handlePageChange(currentPage - 1)}
                disabled={currentPage === 1}
                className="px-2 py-1 text-xs border border-gray-300 rounded hover:bg-white disabled:opacity-50 disabled:cursor-not-allowed"
              >
                ‹
              </button>
              
              <span className="px-2 py-1 text-xs bg-blue-600 text-white rounded min-w-[24px] text-center">
                {currentPage}
              </span>
              
              <button
                onClick={() => handlePageChange(currentPage + 1)}
                disabled={currentPage === totalPages}
                className="px-2 py-1 text-xs border border-gray-300 rounded hover:bg-white disabled:opacity-50 disabled:cursor-not-allowed"
              >
                ›
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export default FATTablesViewer