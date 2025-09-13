import React, { useState, useRef } from 'react'
import { Upload, X, FileText, AlertCircle, CheckCircle } from 'lucide-react'
import toast from 'react-hot-toast'

const FileUpload = ({ onFilesUploaded, acceptedTypes = '.csv', multiple = true, maxSize = 50 * 1024 * 1024 }) => {
  const [isDragOver, setIsDragOver] = useState(false)
  const [uploadedFiles, setUploadedFiles] = useState([])
  const [isUploading, setIsUploading] = useState(false)
  const fileInputRef = useRef(null)

  const validateFile = (file) => {
    const errors = []
    
    // Verificar tipo de arquivo
    if (acceptedTypes && !acceptedTypes.split(',').some(type => 
      file.name.toLowerCase().endsWith(type.trim().toLowerCase())
    )) {
      errors.push(`Tipo de arquivo não suportado. Aceitos: ${acceptedTypes}`)
    }
    
    // Verificar tamanho
    if (file.size > maxSize) {
      errors.push(`Arquivo muito grande. Máximo: ${formatFileSize(maxSize)}`)
    }
    
    return errors
  }

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes'
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
  }

  const handleFiles = async (files) => {
    const fileArray = Array.from(files)
    const validFiles = []
    const invalidFiles = []

    fileArray.forEach(file => {
      const errors = validateFile(file)
      if (errors.length === 0) {
        validFiles.push({
          file,
          name: file.name,
          size: file.size,
          status: 'pending',
          progress: 0,
          errors: []
        })
      } else {
        invalidFiles.push({ file, errors })
      }
    })

    if (invalidFiles.length > 0) {
      invalidFiles.forEach(({ file, errors }) => {
        toast.error(`${file.name}: ${errors.join(', ')}`)
      })
    }

    if (validFiles.length > 0) {
      setUploadedFiles(prev => [...prev, ...validFiles])
      await uploadFiles(validFiles)
    }
  }

  const uploadFiles = async (files) => {
    setIsUploading(true)
    
    try {
      for (const fileData of files) {
        await uploadSingleFile(fileData)
      }
      
      if (onFilesUploaded) {
        onFilesUploaded(files.map(f => f.file))
      }
      
      toast.success(`${files.length} arquivo(s) enviado(s) com sucesso!`)
    } catch (error) {
      console.error('Erro no upload:', error)
      toast.error('Erro durante o upload dos arquivos')
    } finally {
      setIsUploading(false)
    }
  }

  const uploadSingleFile = async (fileData) => {
    const formData = new FormData()
    formData.append('file', fileData.file)

    try {
      // Atualizar status para uploading
      setUploadedFiles(prev => prev.map(f => 
        f.name === fileData.name 
          ? { ...f, status: 'uploading', progress: 0 }
          : f
      ))

      const response = await fetch('/api/csv/upload', {
        method: 'POST',
        body: formData,
        onUploadProgress: (progressEvent) => {
          const progress = Math.round((progressEvent.loaded * 100) / progressEvent.total)
          setUploadedFiles(prev => prev.map(f => 
            f.name === fileData.name 
              ? { ...f, progress }
              : f
          ))
        }
      })

      if (response.ok) {
        const result = await response.json()
        setUploadedFiles(prev => prev.map(f => 
          f.name === fileData.name 
            ? { ...f, status: 'completed', progress: 100, result }
            : f
        ))
      } else {
        const error = await response.json()
        throw new Error(error.message || 'Erro no upload')
      }
    } catch (error) {
      setUploadedFiles(prev => prev.map(f => 
        f.name === fileData.name 
          ? { ...f, status: 'error', errors: [error.message] }
          : f
      ))
      throw error
    }
  }

  const removeFile = (fileName) => {
    setUploadedFiles(prev => prev.filter(f => f.name !== fileName))
  }

  const clearAllFiles = () => {
    setUploadedFiles([])
  }

  const handleDragOver = (e) => {
    e.preventDefault()
    setIsDragOver(true)
  }

  const handleDragLeave = (e) => {
    e.preventDefault()
    setIsDragOver(false)
  }

  const handleDrop = (e) => {
    e.preventDefault()
    setIsDragOver(false)
    const files = e.dataTransfer.files
    if (files.length > 0) {
      handleFiles(files)
    }
  }

  const handleFileSelect = (e) => {
    const files = e.target.files
    if (files.length > 0) {
      handleFiles(files)
    }
    // Limpar input para permitir selecionar o mesmo arquivo novamente
    e.target.value = ''
  }

  const openFileDialog = () => {
    fileInputRef.current?.click()
  }

  const getStatusIcon = (status) => {
    switch (status) {
      case 'completed':
        return <CheckCircle className="w-4 h-4 text-success-600" />
      case 'error':
        return <AlertCircle className="w-4 h-4 text-error-600" />
      case 'uploading':
        return <div className="w-4 h-4 border-2 border-primary-600 border-t-transparent rounded-full animate-spin" />
      default:
        return <FileText className="w-4 h-4 text-gray-500" />
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'completed': return 'border-success-300 bg-success-50'
      case 'error': return 'border-error-300 bg-error-50'
      case 'uploading': return 'border-primary-300 bg-primary-50'
      default: return 'border-gray-300 bg-white'
    }
  }

  return (
    <div className="space-y-4">
      {/* Área de Upload */}
      <div
        className={`
          relative border-2 border-dashed rounded-lg p-8 text-center transition-all duration-200
          ${isDragOver 
            ? 'border-primary-400 bg-primary-50' 
            : 'border-gray-300 hover:border-gray-400'
          }
          ${isUploading ? 'pointer-events-none opacity-50' : 'cursor-pointer'}
        `}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onDrop={handleDrop}
        onClick={openFileDialog}
      >
        <input
          ref={fileInputRef}
          type="file"
          accept={acceptedTypes}
          multiple={multiple}
          onChange={handleFileSelect}
          className="hidden"
        />
        
        <div className="space-y-4">
          <div className="flex justify-center">
            <Upload className={`w-12 h-12 ${
              isDragOver ? 'text-primary-600' : 'text-gray-400'
            }`} />
          </div>
          
          <div>
            <p className="text-lg font-medium text-gray-900">
              {isDragOver 
                ? 'Solte os arquivos aqui' 
                : 'Arraste arquivos ou clique para selecionar'
              }
            </p>
            <p className="text-sm text-gray-600 mt-1">
              Tipos aceitos: {acceptedTypes} • Tamanho máximo: {formatFileSize(maxSize)}
            </p>
          </div>
          
          <button
            type="button"
            className="btn btn-primary"
            disabled={isUploading}
          >
            Selecionar Arquivos
          </button>
        </div>
      </div>

      {/* Lista de Arquivos */}
      {uploadedFiles.length > 0 && (
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <h3 className="text-lg font-medium text-gray-900">
              Arquivos ({uploadedFiles.length})
            </h3>
            <button
              onClick={clearAllFiles}
              className="btn btn-secondary btn-sm"
              disabled={isUploading}
            >
              Limpar Todos
            </button>
          </div>
          
          <div className="space-y-2">
            {uploadedFiles.map((fileData, index) => (
              <div
                key={index}
                className={`border rounded-lg p-4 ${getStatusColor(fileData.status)}`}
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-3 flex-1 min-w-0">
                    {getStatusIcon(fileData.status)}
                    
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium text-gray-900 truncate">
                        {fileData.name}
                      </p>
                      <p className="text-xs text-gray-600">
                        {formatFileSize(fileData.size)}
                        {fileData.result?.records && (
                          <span className="ml-2">
                            • {fileData.result.records.toLocaleString('pt-BR')} registros
                          </span>
                        )}
                      </p>
                    </div>
                  </div>
                  
                  <div className="flex items-center space-x-2">
                    {fileData.status === 'uploading' && (
                      <div className="text-xs text-gray-600">
                        {fileData.progress}%
                      </div>
                    )}
                    
                    <button
                      onClick={(e) => {
                        e.stopPropagation()
                        removeFile(fileData.name)
                      }}
                      className="text-gray-400 hover:text-gray-600"
                      disabled={isUploading}
                    >
                      <X className="w-4 h-4" />
                    </button>
                  </div>
                </div>
                
                {/* Barra de Progresso */}
                {fileData.status === 'uploading' && (
                  <div className="mt-2">
                    <div className="w-full bg-gray-200 rounded-full h-1.5">
                      <div 
                        className="bg-primary-600 h-1.5 rounded-full transition-all duration-300"
                        style={{ width: `${fileData.progress}%` }}
                      ></div>
                    </div>
                  </div>
                )}
                
                {/* Erros */}
                {fileData.errors.length > 0 && (
                  <div className="mt-2">
                    {fileData.errors.map((error, errorIndex) => (
                      <p key={errorIndex} className="text-xs text-error-600">
                        {error}
                      </p>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

export default FileUpload