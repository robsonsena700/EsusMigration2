import React from 'react'
import { Loader2 } from 'lucide-react'
import clsx from 'clsx'

const LoadingSpinner = ({ 
  size = 'medium', 
  text = 'Carregando...', 
  className = '',
  showText = true,
  color = 'primary'
}) => {
  const sizeClasses = {
    small: 'w-4 h-4',
    medium: 'w-6 h-6',
    large: 'w-8 h-8',
    xlarge: 'w-12 h-12'
  }

  const colorClasses = {
    primary: 'text-primary-600',
    white: 'text-white',
    gray: 'text-gray-600',
    success: 'text-success-600',
    warning: 'text-warning-600',
    error: 'text-error-600'
  }

  const textSizeClasses = {
    small: 'text-xs',
    medium: 'text-sm',
    large: 'text-base',
    xlarge: 'text-lg'
  }

  return (
    <div className={clsx(
      'flex flex-col items-center justify-center space-y-2',
      className
    )}>
      <Loader2 
        className={clsx(
          'animate-spin',
          sizeClasses[size],
          colorClasses[color]
        )} 
      />
      {showText && text && (
        <p className={clsx(
          'font-medium',
          textSizeClasses[size],
          colorClasses[color]
        )}>
          {text}
        </p>
      )}
    </div>
  )
}

// Componente para loading inline
export const InlineSpinner = ({ size = 'small', className = '' }) => {
  return (
    <Loader2 
      className={clsx(
        'animate-spin',
        sizeClasses[size] || 'w-4 h-4',
        'text-current',
        className
      )} 
    />
  )
}

// Componente para loading de página inteira
export const FullPageSpinner = ({ text = 'Carregando sistema...' }) => {
  return (
    <div className="fixed inset-0 bg-white bg-opacity-90 flex items-center justify-center z-50">
      <div className="text-center">
        <LoadingSpinner 
          size="xlarge" 
          text={text}
          color="primary"
        />
      </div>
    </div>
  )
}

// Componente para loading de overlay
export const OverlaySpinner = ({ 
  isVisible = false, 
  text = 'Processando...', 
  onCancel = null 
}) => {
  if (!isVisible) return null

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg p-6 shadow-xl max-w-sm w-full mx-4">
        <div className="text-center">
          <LoadingSpinner 
            size="large" 
            text={text}
            color="primary"
          />
          {onCancel && (
            <button
              onClick={onCancel}
              className="mt-4 btn btn-secondary text-sm"
            >
              Cancelar
            </button>
          )}
        </div>
      </div>
    </div>
  )
}

export default LoadingSpinner