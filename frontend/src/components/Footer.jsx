import React from 'react'
import { Heart, Code, Shield } from 'lucide-react'

const Footer = () => {
  const currentYear = new Date().getFullYear()

  return (
    <footer className="bg-white border-t border-gray-200 mt-auto">
      <div className="container mx-auto px-4 py-6 max-w-7xl">
        <div className="flex flex-col items-center justify-center space-y-4">
          {/* Informações do Sistema */}
          <div className="flex flex-col items-center space-y-2">
            <div className="flex items-center space-x-1 text-sm text-gray-600">
              <Code className="w-4 h-4" />
              <span>Sistema de Migração e-SUS</span>
            </div>
            <div className="flex items-center space-x-1 text-xs text-gray-500">
              <Shield className="w-3 h-3" />
              <span>Dados protegidos por LGPD</span>
            </div>
          </div>

          {/* Copyright */}
          <div className="flex flex-col items-center">
            <div className="flex items-center space-x-1 text-xs text-gray-500">
              <span>© {currentYear} Desenvolvido com</span>
              <Heart className="w-3 h-3 text-red-500" />
              <span>para a saúde pública</span>
            </div>
            <p className="text-xs text-gray-400 mt-1">
              Versão 1.0.0 - Build {new Date().toISOString().slice(0, 10)}
            </p>
          </div>
        </div>

        {/* Linha de Separação e Links */}
        <div className="mt-4 pt-4 border-t border-gray-100">
          <div className="flex flex-col md:flex-row items-center justify-between space-y-2 md:space-y-0">
            <div className="flex items-center space-x-4 text-xs text-gray-500">
              <span>Sistema interno - Uso restrito</span>
              <span>•</span>
              <span>Dados sensíveis protegidos</span>
              <span>•</span>
              <span>Acesso monitorado</span>
            </div>
            
            <div className="flex items-center space-x-4 text-xs">
              <button 
                className="text-gray-500 hover:text-primary-600 transition-colors"
                onClick={() => window.open('/api/health', '_blank')}
              >
                Status da API
              </button>
              <button 
                className="text-gray-500 hover:text-primary-600 transition-colors"
                onClick={() => window.open('/api/logs/download', '_blank')}
              >
                Logs do Sistema
              </button>
            </div>
          </div>
        </div>
      </div>
    </footer>
  )
}

export default Footer