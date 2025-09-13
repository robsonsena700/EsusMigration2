import React, { useState, useEffect } from 'react'
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom'
import { Toaster } from 'react-hot-toast'
import MigratorDashboard from './components/MigratorDashboard'
import ConfigurationPanel from './components/ConfigurationPanel'
import Header from './components/Header'
import Footer from './components/Footer'
import LoadingSpinner from './components/LoadingSpinner'
import ErrorBoundary from './components/ErrorBoundary'

function App() {
  const [isLoading, setIsLoading] = useState(true)
  const [systemStatus, setSystemStatus] = useState({
    backend: 'checking',
    database: 'checking',
    lastCheck: null
  })

  // Verificar status do sistema na inicialização
  useEffect(() => {
    const checkSystemStatus = async () => {
      try {
        // Verificar backend
        const backendResponse = await fetch('/api/health')
        const backendStatus = backendResponse.ok ? 'online' : 'offline'
        
        // Verificar database através do backend
        const dbResponse = await fetch('/api/database/status')
        const dbStatus = dbResponse.ok ? 'online' : 'offline'
        
        setSystemStatus({
          backend: backendStatus,
          database: dbStatus,
          lastCheck: new Date().toISOString()
        })
      } catch (error) {
        console.error('Erro ao verificar status do sistema:', error)
        setSystemStatus({
          backend: 'offline',
          database: 'offline',
          lastCheck: new Date().toISOString()
        })
      } finally {
        setIsLoading(false)
      }
    }

    // Delay inicial para melhor UX
    const timer = setTimeout(() => {
      checkSystemStatus()
    }, 1000)

    return () => clearTimeout(timer)
  }, [])

  // Verificação periódica do status (a cada 30 segundos)
  useEffect(() => {
    if (!isLoading) {
      const interval = setInterval(async () => {
        try {
          const response = await fetch('/api/health')
          const backendStatus = response.ok ? 'online' : 'offline'
          
          setSystemStatus(prev => ({
            ...prev,
            backend: backendStatus,
            lastCheck: new Date().toISOString()
          }))
        } catch (error) {
          setSystemStatus(prev => ({
            ...prev,
            backend: 'offline',
            lastCheck: new Date().toISOString()
          }))
        }
      }, 30000)

      return () => clearInterval(interval)
    }
  }, [isLoading])

  if (isLoading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-primary-50 to-primary-100 flex items-center justify-center">
        <LoadingSpinner size="large" text="Inicializando sistema..." />
      </div>
    )
  }

  return (
    <ErrorBoundary>
      <Router>
        <div className="min-h-screen bg-gray-50 flex flex-col">
          {/* Header */}
          <Header systemStatus={systemStatus} />
          
          {/* Main Content */}
          <main className="flex-1 container mx-auto px-4 py-6 max-w-7xl">
            <Routes>
              {/* Rota principal - Dashboard */}
              <Route 
                path="/" 
                element={
                  <MigratorDashboard 
                    systemStatus={systemStatus}
                    onStatusUpdate={setSystemStatus}
                  />
                } 
              />
              
              {/* Rota para dashboard (alias) */}
              <Route 
                path="/dashboard" 
                element={<Navigate to="/" replace />} 
              />
              
              {/* Rota para migração (futura expansão) */}
              <Route 
                path="/migration" 
                element={<Navigate to="/" replace />} 
              />
              
              {/* Rota para logs (futura expansão) */}
              <Route 
                path="/logs" 
                element={<Navigate to="/" replace />} 
              />
              
              {/* Rota para configurações */}
              <Route 
                path="/settings" 
                element={<ConfigurationPanel />} 
              />
              
              {/* Rota para configuração (alias) */}
              <Route 
                path="/config" 
                element={<Navigate to="/settings" replace />} 
              />
              
              {/* Fallback para rotas não encontradas */}
              <Route 
                path="*" 
                element={<Navigate to="/" replace />} 
              />
            </Routes>
          </main>
          
          {/* Footer */}
          <Footer />
          
          {/* Toast Notifications */}
          <Toaster
            position="top-right"
            toastOptions={{
              duration: 4000,
              style: {
                background: '#fff',
                color: '#374151',
                border: '1px solid #e5e7eb',
                borderRadius: '0.75rem',
                fontSize: '14px',
                fontWeight: '500',
                boxShadow: '0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05)'
              },
              success: {
                iconTheme: {
                  primary: '#10b981',
                  secondary: '#fff'
                }
              },
              error: {
                iconTheme: {
                  primary: '#ef4444',
                  secondary: '#fff'
                }
              },
              loading: {
                iconTheme: {
                  primary: '#3b82f6',
                  secondary: '#fff'
                }
              }
            }}
          />
        </div>
      </Router>
    </ErrorBoundary>
  )
}

export default App