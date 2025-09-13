import React from 'react'
import { Link, useLocation } from 'react-router-dom'
import { Activity, Database, Server, Clock, Settings, Home } from 'lucide-react'

const Header = ({ systemStatus }) => {
  const getStatusColor = (status) => {
    switch (status) {
      case 'online': return 'text-success-600 bg-success-100'
      case 'offline': return 'text-error-600 bg-error-100'
      case 'checking': return 'text-warning-600 bg-warning-100'
      default: return 'text-gray-600 bg-gray-100'
    }
  }

  const getStatusText = (status) => {
    switch (status) {
      case 'online': return 'Online'
      case 'offline': return 'Offline'
      case 'checking': return 'Verificando...'
      default: return 'Desconhecido'
    }
  }

  const formatLastCheck = (timestamp) => {
    if (!timestamp) return 'Nunca'
    const date = new Date(timestamp)
    return date.toLocaleTimeString('pt-BR', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    })
  }

  return (
    <header className="bg-white border-b border-gray-200 shadow-sm">
      <div className="container mx-auto px-4 py-4 max-w-7xl">
        <div className="flex items-center justify-between">
          {/* Logo e Título */}
          <div className="flex items-center space-x-6">
            <div className="flex items-center space-x-3">
              <div className="flex items-center justify-center w-10 h-10 bg-primary-600 rounded-lg">
                <Activity className="w-6 h-6 text-white" />
              </div>
              <div>
                <h1 className="text-xl font-bold text-gray-900">
                  e-SUS Migrator
                </h1>
                <p className="text-sm text-gray-600">
                  Sistema de Migração de Dados
                </p>
              </div>
            </div>
            
            {/* Navegação */}
            <nav className="flex items-center space-x-1">
              <NavigationLink to="/" icon={Home} label="Dashboard" />
              <NavigationLink to="/settings" icon={Settings} label="Configurações" />
            </nav>
          </div>

          {/* Status do Sistema */}
          <div className="flex items-center space-x-4">
            {/* Status do Backend */}
            <div className="flex items-center space-x-2">
              <Server className="w-4 h-4 text-gray-500" />
              <span className="text-sm text-gray-600">Backend:</span>
              <span className={`px-2 py-1 rounded-full text-xs font-medium ${getStatusColor(systemStatus.backend)}`}>
                {getStatusText(systemStatus.backend)}
              </span>
            </div>

            {/* Status do Database */}
            <div className="flex items-center space-x-2">
              <Database className="w-4 h-4 text-gray-500" />
              <span className="text-sm text-gray-600">Database:</span>
              <span className={`px-2 py-1 rounded-full text-xs font-medium ${getStatusColor(systemStatus.database)}`}>
                {getStatusText(systemStatus.database)}
              </span>
            </div>

            {/* Última Verificação */}
            <div className="flex items-center space-x-2 text-xs text-gray-500">
              <Clock className="w-3 h-3" />
              <span>Última verificação: {formatLastCheck(systemStatus.lastCheck)}</span>
            </div>
          </div>
        </div>
      </div>
    </header>
  )
}

// Componente de navegação
const NavigationLink = ({ to, icon: Icon, label }) => {
  const location = useLocation()
  const isActive = location.pathname === to
  
  return (
    <Link
      to={to}
      className={`flex items-center space-x-2 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
        isActive
          ? 'bg-primary-100 text-primary-700'
          : 'text-gray-600 hover:text-gray-900 hover:bg-gray-100'
      }`}
    >
      <Icon className="w-4 h-4" />
      <span>{label}</span>
    </Link>
  )
}

export default Header