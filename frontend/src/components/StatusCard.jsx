import React from 'react'
import { 
  CheckCircle, 
  AlertCircle, 
  XCircle, 
  Clock, 
  Info,
  TrendingUp,
  TrendingDown,
  Minus
} from 'lucide-react'

const StatusCard = ({ 
  title, 
  value, 
  subtitle, 
  status = 'neutral', 
  icon: CustomIcon, 
  trend,
  trendValue,
  onClick,
  className = '',
  size = 'default'
}) => {
  const getStatusConfig = (status) => {
    switch (status) {
      case 'success':
        return {
          bgColor: 'bg-success-50',
          borderColor: 'border-success-200',
          textColor: 'text-success-700',
          iconColor: 'text-success-600',
          defaultIcon: CheckCircle
        }
      case 'warning':
        return {
          bgColor: 'bg-warning-50',
          borderColor: 'border-warning-200',
          textColor: 'text-warning-700',
          iconColor: 'text-warning-600',
          defaultIcon: AlertCircle
        }
      case 'error':
        return {
          bgColor: 'bg-error-50',
          borderColor: 'border-error-200',
          textColor: 'text-error-700',
          iconColor: 'text-error-600',
          defaultIcon: XCircle
        }
      case 'info':
        return {
          bgColor: 'bg-primary-50',
          borderColor: 'border-primary-200',
          textColor: 'text-primary-700',
          iconColor: 'text-primary-600',
          defaultIcon: Info
        }
      case 'pending':
        return {
          bgColor: 'bg-gray-50',
          borderColor: 'border-gray-200',
          textColor: 'text-gray-700',
          iconColor: 'text-gray-600',
          defaultIcon: Clock
        }
      default: // neutral
        return {
          bgColor: 'bg-white',
          borderColor: 'border-gray-200',
          textColor: 'text-gray-900',
          iconColor: 'text-gray-600',
          defaultIcon: Info
        }
    }
  }

  const getSizeConfig = (size) => {
    switch (size) {
      case 'small':
        return {
          padding: 'p-4',
          iconSize: 'w-5 h-5',
          titleSize: 'text-sm',
          valueSize: 'text-lg',
          subtitleSize: 'text-xs'
        }
      case 'large':
        return {
          padding: 'p-8',
          iconSize: 'w-10 h-10',
          titleSize: 'text-lg',
          valueSize: 'text-3xl',
          subtitleSize: 'text-sm'
        }
      default: // default
        return {
          padding: 'p-6',
          iconSize: 'w-8 h-8',
          titleSize: 'text-base',
          valueSize: 'text-2xl',
          subtitleSize: 'text-sm'
        }
    }
  }

  const getTrendIcon = (trend) => {
    switch (trend) {
      case 'up':
        return <TrendingUp className="w-4 h-4 text-success-600" />
      case 'down':
        return <TrendingDown className="w-4 h-4 text-error-600" />
      case 'neutral':
        return <Minus className="w-4 h-4 text-gray-600" />
      default:
        return null
    }
  }

  const getTrendColor = (trend) => {
    switch (trend) {
      case 'up':
        return 'text-success-600'
      case 'down':
        return 'text-error-600'
      default:
        return 'text-gray-600'
    }
  }

  const statusConfig = getStatusConfig(status)
  const sizeConfig = getSizeConfig(size)
  const IconComponent = CustomIcon || statusConfig.defaultIcon

  const cardClasses = `
    ${sizeConfig.padding}
    ${statusConfig.bgColor}
    border ${statusConfig.borderColor}
    rounded-lg
    transition-all duration-200
    ${onClick ? 'cursor-pointer hover:shadow-md hover:scale-105' : ''}
    ${className}
  `

  const handleClick = () => {
    if (onClick) {
      onClick()
    }
  }

  return (
    <div 
      className={cardClasses}
      onClick={handleClick}
      role={onClick ? 'button' : undefined}
      tabIndex={onClick ? 0 : undefined}
      onKeyDown={onClick ? (e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault()
          onClick()
        }
      } : undefined}
    >
      <div className="flex items-center justify-between">
        <div className="flex-1 min-w-0">
          {/* Título */}
          {title && (
            <p className={`${sizeConfig.titleSize} font-medium text-gray-600 mb-1`}>
              {title}
            </p>
          )}
          
          {/* Valor Principal */}
          {value !== undefined && value !== null && (
            <div className="flex items-baseline space-x-2">
              <p className={`${sizeConfig.valueSize} font-bold ${statusConfig.textColor}`}>
                {typeof value === 'number' ? value.toLocaleString('pt-BR') : value}
              </p>
              
              {/* Trend */}
              {trend && (
                <div className="flex items-center space-x-1">
                  {getTrendIcon(trend)}
                  {trendValue && (
                    <span className={`text-xs font-medium ${getTrendColor(trend)}`}>
                      {trendValue}
                    </span>
                  )}
                </div>
              )}
            </div>
          )}
          
          {/* Subtítulo */}
          {subtitle && (
            <p className={`${sizeConfig.subtitleSize} text-gray-500 mt-1`}>
              {subtitle}
            </p>
          )}
        </div>
        
        {/* Ícone */}
        {IconComponent && (
          <div className="flex-shrink-0 ml-4">
            <IconComponent className={`${sizeConfig.iconSize} ${statusConfig.iconColor}`} />
          </div>
        )}
      </div>
    </div>
  )
}

// Componente de Status Badge para uso inline
export const StatusBadge = ({ status, text, size = 'default' }) => {
  const getStatusConfig = (status) => {
    switch (status) {
      case 'success':
        return 'bg-success-100 text-success-800 border-success-200'
      case 'warning':
        return 'bg-warning-100 text-warning-800 border-warning-200'
      case 'error':
        return 'bg-error-100 text-error-800 border-error-200'
      case 'info':
        return 'bg-primary-100 text-primary-800 border-primary-200'
      case 'pending':
        return 'bg-gray-100 text-gray-800 border-gray-200'
      default:
        return 'bg-gray-100 text-gray-800 border-gray-200'
    }
  }

  const getSizeConfig = (size) => {
    switch (size) {
      case 'small':
        return 'px-2 py-1 text-xs'
      case 'large':
        return 'px-4 py-2 text-base'
      default:
        return 'px-3 py-1 text-sm'
    }
  }

  return (
    <span className={`
      inline-flex items-center
      ${getSizeConfig(size)}
      ${getStatusConfig(status)}
      border rounded-full font-medium
    `}>
      {text}
    </span>
  )
}

// Componente de Grid de Status Cards
export const StatusGrid = ({ children, columns = 'auto' }) => {
  const getGridColumns = (columns) => {
    if (typeof columns === 'number') {
      return `grid-cols-${columns}`
    }
    
    switch (columns) {
      case 'auto':
        return 'grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4'
      case 'responsive':
        return 'grid-cols-1 sm:grid-cols-2 lg:grid-cols-3'
      case 'equal':
        return 'grid-cols-1 md:grid-cols-2'
      default:
        return 'grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4'
    }
  }

  return (
    <div className={`grid ${getGridColumns(columns)} gap-6`}>
      {children}
    </div>
  )
}

export default StatusCard