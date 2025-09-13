import React from 'react'
import { AlertTriangle, RefreshCw, Home, Bug } from 'lucide-react'

class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props)
    this.state = { 
      hasError: false, 
      error: null, 
      errorInfo: null,
      errorId: null
    }
  }

  static getDerivedStateFromError(error) {
    // Atualiza o state para mostrar a UI de erro
    return { 
      hasError: true,
      errorId: Date.now().toString(36) + Math.random().toString(36).substr(2)
    }
  }

  componentDidCatch(error, errorInfo) {
    // Log do erro para debugging
    console.error('ErrorBoundary capturou um erro:', error, errorInfo)
    
    this.setState({
      error: error,
      errorInfo: errorInfo
    })

    // Aqui você pode enviar o erro para um serviço de logging
    this.logErrorToService(error, errorInfo)
  }

  logErrorToService = (error, errorInfo) => {
    try {
      // Enviar erro para o backend para logging
      fetch('/api/errors', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          error: {
            message: error.message,
            stack: error.stack,
            name: error.name
          },
          errorInfo: errorInfo,
          errorId: this.state.errorId,
          timestamp: new Date().toISOString(),
          userAgent: navigator.userAgent,
          url: window.location.href
        })
      }).catch(err => {
        console.error('Falha ao enviar erro para o servidor:', err)
      })
    } catch (err) {
      console.error('Erro ao processar log de erro:', err)
    }
  }

  handleReload = () => {
    window.location.reload()
  }

  handleGoHome = () => {
    window.location.href = '/'
  }

  handleReportBug = () => {
    const errorDetails = {
      errorId: this.state.errorId,
      message: this.state.error?.message,
      timestamp: new Date().toISOString()
    }
    
    // Copiar detalhes do erro para a área de transferência
    navigator.clipboard.writeText(JSON.stringify(errorDetails, null, 2))
      .then(() => {
        alert('Detalhes do erro copiados para a área de transferência!')
      })
      .catch(() => {
        alert(`ID do Erro: ${this.state.errorId}\nPor favor, anote este ID para reportar o problema.`)
      })
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen bg-gray-50 flex items-center justify-center p-4">
          <div className="max-w-md w-full">
            {/* Card de Erro */}
            <div className="bg-white rounded-lg shadow-lg p-6 text-center">
              {/* Ícone de Erro */}
              <div className="mx-auto flex items-center justify-center w-16 h-16 bg-error-100 rounded-full mb-4">
                <AlertTriangle className="w-8 h-8 text-error-600" />
              </div>
              
              {/* Título */}
              <h1 className="text-xl font-bold text-gray-900 mb-2">
                Oops! Algo deu errado
              </h1>
              
              {/* Descrição */}
              <p className="text-gray-600 mb-6">
                Ocorreu um erro inesperado na aplicação. Nossa equipe foi notificada automaticamente.
              </p>
              
              {/* ID do Erro */}
              <div className="bg-gray-50 rounded-lg p-3 mb-6">
                <p className="text-xs text-gray-500 mb-1">ID do Erro:</p>
                <p className="text-sm font-mono text-gray-700">
                  {this.state.errorId}
                </p>
              </div>
              
              {/* Botões de Ação */}
              <div className="space-y-3">
                <button
                  onClick={this.handleReload}
                  className="w-full btn btn-primary flex items-center justify-center space-x-2"
                >
                  <RefreshCw className="w-4 h-4" />
                  <span>Recarregar Página</span>
                </button>
                
                <button
                  onClick={this.handleGoHome}
                  className="w-full btn btn-secondary flex items-center justify-center space-x-2"
                >
                  <Home className="w-4 h-4" />
                  <span>Voltar ao Início</span>
                </button>
                
                <button
                  onClick={this.handleReportBug}
                  className="w-full btn btn-secondary flex items-center justify-center space-x-2 text-xs"
                >
                  <Bug className="w-3 h-3" />
                  <span>Copiar Detalhes do Erro</span>
                </button>
              </div>
            </div>
            
            {/* Informações Técnicas (apenas em desenvolvimento) */}
            {process.env.NODE_ENV === 'development' && this.state.error && (
              <div className="mt-4 bg-red-50 border border-red-200 rounded-lg p-4">
                <h3 className="text-sm font-medium text-red-800 mb-2">
                  Detalhes Técnicos (Desenvolvimento)
                </h3>
                <div className="text-xs text-red-700 space-y-2">
                  <div>
                    <strong>Erro:</strong> {this.state.error.message}
                  </div>
                  <div>
                    <strong>Stack:</strong>
                    <pre className="mt-1 text-xs bg-red-100 p-2 rounded overflow-auto max-h-32">
                      {this.state.error.stack}
                    </pre>
                  </div>
                  {this.state.errorInfo && (
                    <div>
                      <strong>Component Stack:</strong>
                      <pre className="mt-1 text-xs bg-red-100 p-2 rounded overflow-auto max-h-32">
                        {this.state.errorInfo.componentStack}
                      </pre>
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      )
    }

    return this.props.children
  }
}

export default ErrorBoundary