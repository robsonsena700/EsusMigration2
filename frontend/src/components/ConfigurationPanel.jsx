import React, { useState, useEffect } from 'react';
import './ConfigurationPanel.css';

const ConfigurationPanel = () => {
  const [config, setConfig] = useState({
    POSTGRES_DB: '',
    POSTGRES_USER: '',
    POSTGRES_PASSWORD: '',
    POSTGRES_HOST: 'localhost',
    POSTGRES_PORT: '5432',
    TABLE_NAME: 'public.tl_cds_cad_individual'
  });
  
  const [migrationOptions, setMigrationOptions] = useState({
    co_municipio: '',
    require_co_municipio: false
  });
  
  const [csvFiles, setCsvFiles] = useState([]);
  const [selectedFile, setSelectedFile] = useState('');
  const [uploadedFile, setUploadedFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState('');
  const [messageType, setMessageType] = useState(''); // 'success' ou 'error'

  // Carregar configurações existentes
  useEffect(() => {
    fetchConfig();
    fetchCsvFiles();
  }, []);

  const fetchConfig = async () => {
    try {
      const response = await fetch('/api/config');
      if (response.ok) {
        const data = await response.json();
        setConfig(data);
      }
    } catch (error) {
      console.error('Erro ao carregar configurações:', error);
    }
  };

  const fetchCsvFiles = async () => {
    try {
      const response = await fetch('/api/csv/files');
      if (response.ok) {
        const files = await response.json();
        setCsvFiles(files);
      }
    } catch (error) {
      console.error('Erro ao carregar arquivos CSV:', error);
    }
  };

  const handleConfigChange = (field, value) => {
    setConfig(prev => ({
      ...prev,
      [field]: value
    }));
  };

  const handleMigrationOptionChange = (field, value) => {
    setMigrationOptions(prev => ({
      ...prev,
      [field]: value
    }));
  };

  const handleSaveConfig = async () => {
    setLoading(true);
    setMessage('');
    
    try {
      const response = await fetch('/api/config', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(config)
      });
      
      const data = await response.json();
      
      if (response.ok) {
        setMessage('Configurações salvas com sucesso!');
        setMessageType('success');
      } else {
        setMessage(data.error || 'Erro ao salvar configurações');
        setMessageType('error');
      }
    } catch (error) {
      setMessage('Erro de conexão ao salvar configurações');
      setMessageType('error');
    } finally {
      setLoading(false);
    }
  };

  const handleFileUpload = async (event) => {
    const file = event.target.files[0];
    if (!file) return;
    
    if (!file.name.toLowerCase().endsWith('.csv')) {
      setMessage('Por favor, selecione um arquivo CSV válido');
      setMessageType('error');
      return;
    }
    
    setUploadedFile(file);
    setMessage('');
  };

  const handleRunMigration = async () => {
    if (!selectedFile && !uploadedFile) {
      setMessage('Por favor, selecione um arquivo CSV');
      setMessageType('error');
      return;
    }
    
    setLoading(true);
    setMessage('');
    
    try {
      const formData = new FormData();
      
      if (uploadedFile) {
        formData.append('file', uploadedFile);
      } else {
        formData.append('filename', selectedFile);
      }
      
      formData.append('table_name', config.TABLE_NAME);
      
      // Adicionar opções de migração
      if (migrationOptions.require_co_municipio && migrationOptions.co_municipio) {
        formData.append('co_municipio', migrationOptions.co_municipio);
      }
      
      const response = await fetch('/api/import', {
        method: 'POST',
        body: formData
      });
      
      const data = await response.json();
      
      if (response.ok) {
        setMessage('Migração iniciada com sucesso! Verifique os logs para acompanhar o progresso.');
        setMessageType('success');
        // Atualizar lista de arquivos CSV
        fetchCsvFiles();
      } else {
        setMessage(data.error || 'Erro ao executar migração');
        setMessageType('error');
      }
    } catch (error) {
      setMessage('Erro de conexão ao executar migração');
      setMessageType('error');
    } finally {
      setLoading(false);
    }
  };

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  return (
    <div className="configuration-panel">
      <div className="config-header">
        <h2>Configuração do Sistema</h2>
        <p>Configure as informações de conexão PostgreSQL e execute migrações de dados CSV</p>
      </div>

      {message && (
        <div className={`message ${messageType}`}>
          {message}
        </div>
      )}

      <div className="config-sections">
        {/* Seção de Configuração PostgreSQL */}
        <div className="config-section">
          <h3>Configuração PostgreSQL</h3>
          <div className="form-grid">
            <div className="form-group">
              <label htmlFor="postgres_db">Nome do Banco de Dados *</label>
              <input
                id="postgres_db"
                type="text"
                value={config.POSTGRES_DB}
                onChange={(e) => handleConfigChange('POSTGRES_DB', e.target.value)}
                placeholder="nome_do_banco"
                required
              />
            </div>
            
            <div className="form-group">
              <label htmlFor="postgres_user">Usuário *</label>
              <input
                id="postgres_user"
                type="text"
                value={config.POSTGRES_USER}
                onChange={(e) => handleConfigChange('POSTGRES_USER', e.target.value)}
                placeholder="usuario"
                required
              />
            </div>
            
            <div className="form-group">
              <label htmlFor="postgres_password">Senha *</label>
              <input
                id="postgres_password"
                type="password"
                value={config.POSTGRES_PASSWORD}
                onChange={(e) => handleConfigChange('POSTGRES_PASSWORD', e.target.value)}
                placeholder="senha"
                required
              />
            </div>
            
            <div className="form-group">
              <label htmlFor="postgres_host">Host</label>
              <input
                id="postgres_host"
                type="text"
                value={config.POSTGRES_HOST}
                onChange={(e) => handleConfigChange('POSTGRES_HOST', e.target.value)}
                placeholder="localhost"
              />
            </div>
            
            <div className="form-group">
              <label htmlFor="postgres_port">Porta</label>
              <input
                id="postgres_port"
                type="number"
                value={config.POSTGRES_PORT}
                onChange={(e) => handleConfigChange('POSTGRES_PORT', e.target.value)}
                placeholder="5432"
              />
            </div>
            
            <div className="form-group">
              <label htmlFor="table_name">Nome da Tabela</label>
              <input
                id="table_name"
                type="text"
                value={config.TABLE_NAME}
                onChange={(e) => handleConfigChange('TABLE_NAME', e.target.value)}
                placeholder="public.tl_cds_cad_individual"
              />
            </div>
          </div>
          
          <button 
            className="btn btn-primary"
            onClick={handleSaveConfig}
            disabled={loading}
          >
            {loading ? 'Salvando...' : 'Salvar Configurações'}
          </button>
        </div>

        {/* Seção de Upload e Migração */}
        <div className="config-section">
          <h3>Migração de Dados CSV</h3>
          
          <div className="migration-options">
            <div className="option-group">
              <h4>Upload de Arquivo CSV</h4>
              <div className="file-upload">
                <input
                  type="file"
                  accept=".csv"
                  onChange={handleFileUpload}
                  id="csv-upload"
                />
                <label htmlFor="csv-upload" className="file-upload-label">
                  {uploadedFile ? uploadedFile.name : 'Escolher arquivo CSV'}
                </label>
              </div>
            </div>
            
            <div className="option-group">
              <h4>Opções de Migração</h4>
              <div className="form-group">
                <label>
                  <input
                    type="checkbox"
                    checked={migrationOptions.require_co_municipio}
                    onChange={(e) => handleMigrationOptionChange('require_co_municipio', e.target.checked)}
                  />
                  Preencher código do município obrigatoriamente
                </label>
                <p className="field-description">
                  Código identificador do município onde nasceu o cidadão (chave estrangeira TB_LOCALIDADE)
                </p>
              </div>
              
              {migrationOptions.require_co_municipio && (
                <div className="form-group">
                  <label htmlFor="co_municipio">Código do Município *</label>
                  <input
                    id="co_municipio"
                    type="text"
                    value={migrationOptions.co_municipio}
                    onChange={(e) => handleMigrationOptionChange('co_municipio', e.target.value)}
                    placeholder="Ex: 1407 (Cascavel)"
                    required={migrationOptions.require_co_municipio}
                  />
                  <small className="field-hint">
                    Deixe em branco para usar NULL quando a informação não estiver disponível no CSV
                  </small>
                </div>
              )}
            </div>
          </div>
          
          <button 
            className="btn btn-success"
            onClick={handleRunMigration}
            disabled={loading || !uploadedFile || (migrationOptions.require_co_municipio && !migrationOptions.co_municipio)}
          >
            {loading ? 'Executando...' : 'Executar Migração'}
          </button>
        </div>
      </div>
    </div>
  );
};

export default ConfigurationPanel;