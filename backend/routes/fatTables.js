const express = require('express');
const { Pool } = require('pg');
const router = express.Router();

// Configuração do pool de conexões PostgreSQL
const pool = new Pool({
  user: process.env.POSTGRES_USER,
  host: process.env.POSTGRES_HOST,
  database: process.env.POSTGRES_DB,
  password: process.env.POSTGRES_PASSWORD,
  port: process.env.POSTGRES_PORT,
});

// Endpoint para obter estatísticas das tabelas FAT
router.get('/stats', async (req, res) => {
  try {
    console.log('🔍 Buscando estatísticas das tabelas FAT...');
    
    // Query para buscar informações sobre tabelas que começam com 'fat_'
    const query = `
      SELECT 
        schemaname,
        tablename,
        n_tup_ins as total_inserts,
        n_tup_upd as total_updates,
        n_tup_del as total_deletes,
        n_live_tup as live_tuples,
        n_dead_tup as dead_tuples,
        last_vacuum,
        last_autovacuum,
        last_analyze,
        last_autoanalyze
      FROM pg_stat_user_tables 
      WHERE tablename LIKE 'fat_%'
      ORDER BY tablename;
    `;
    
    const result = await pool.query(query);
    
    // Se não houver tabelas FAT, retornar estrutura vazia
    if (result.rows.length === 0) {
      console.log('ℹ️ Nenhuma tabela FAT encontrada');
      return res.json({
        tables: [],
        summary: {
          totalTables: 0,
          totalRecords: 0,
          lastUpdate: null
        }
      });
    }
    
    // Calcular estatísticas resumidas
    const totalTables = result.rows.length;
    const totalRecords = result.rows.reduce((sum, row) => sum + parseInt(row.live_tuples || 0), 0);
    const lastUpdate = result.rows.reduce((latest, row) => {
      const dates = [row.last_vacuum, row.last_autovacuum, row.last_analyze, row.last_autoanalyze]
        .filter(date => date !== null)
        .map(date => new Date(date));
      
      if (dates.length === 0) return latest;
      
      const maxDate = new Date(Math.max(...dates));
      return latest === null || maxDate > latest ? maxDate : latest;
    }, null);
    
    console.log(`✅ Encontradas ${totalTables} tabelas FAT com ${totalRecords} registros`);
    
    res.json({
      tables: result.rows,
      summary: {
        totalTables,
        totalRecords,
        lastUpdate
      }
    });
    
  } catch (error) {
    console.error('❌ Erro ao buscar estatísticas das tabelas FAT:', error.message);
    res.status(500).json({ 
      error: 'Erro ao buscar estatísticas',
      details: error.message 
    });
  }
});

// Endpoint para obter detalhes de uma tabela FAT específica
router.get('/table/:tableName', async (req, res) => {
  try {
    const { tableName } = req.params;
    console.log(`🔍 Buscando detalhes da tabela: ${tableName}`);
    
    // Verificar se a tabela existe e é uma tabela FAT
    if (!tableName.startsWith('fat_')) {
      return res.status(400).json({ 
        error: 'Nome de tabela inválido. Deve começar com "fat_"' 
      });
    }
    
    // Query para obter informações detalhadas da tabela
    const tableInfoQuery = `
      SELECT 
        schemaname,
        tablename,
        n_tup_ins as total_inserts,
        n_tup_upd as total_updates,
        n_tup_del as total_deletes,
        n_live_tup as live_tuples,
        n_dead_tup as dead_tuples,
        last_vacuum,
        last_autovacuum,
        last_analyze,
        last_autoanalyze
      FROM pg_stat_user_tables 
      WHERE tablename = $1;
    `;
    
    const tableResult = await pool.query(tableInfoQuery, [tableName]);
    
    if (tableResult.rows.length === 0) {
      return res.status(404).json({ 
        error: `Tabela ${tableName} não encontrada` 
      });
    }
    
    // Query para obter estrutura da tabela (colunas)
    const columnsQuery = `
      SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default
      FROM information_schema.columns 
      WHERE table_name = $1 
      ORDER BY ordinal_position;
    `;
    
    const columnsResult = await pool.query(columnsQuery, [tableName]);
    
    console.log(`✅ Detalhes da tabela ${tableName} obtidos com sucesso`);
    
    res.json({
      table: tableResult.rows[0],
      columns: columnsResult.rows
    });
    
  } catch (error) {
    console.error(`❌ Erro ao buscar detalhes da tabela ${req.params.tableName}:`, error.message);
    res.status(500).json({ 
      error: 'Erro ao buscar detalhes da tabela',
      details: error.message 
    });
  }
});

// Endpoint para obter dados de uma tabela FAT (com paginação)
router.get('/table/:tableName/data', async (req, res) => {
  try {
    const { tableName } = req.params;
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 50;
    const offset = (page - 1) * limit;
    
    console.log(`🔍 Buscando dados da tabela: ${tableName} (página ${page})`);
    
    // Verificar se a tabela existe e é uma tabela FAT
    if (!tableName.startsWith('fat_')) {
      return res.status(400).json({ 
        error: 'Nome de tabela inválido. Deve começar com "fat_"' 
      });
    }
    
    // Query para contar total de registros
    const countQuery = `SELECT COUNT(*) as total FROM ${tableName}`;
    const countResult = await pool.query(countQuery);
    const total = parseInt(countResult.rows[0].total);
    
    // Query para obter os dados com paginação
    const dataQuery = `SELECT * FROM ${tableName} LIMIT $1 OFFSET $2`;
    const dataResult = await pool.query(dataQuery, [limit, offset]);
    
    const totalPages = Math.ceil(total / limit);
    
    console.log(`✅ Dados da tabela ${tableName} obtidos: ${dataResult.rows.length} registros`);
    
    res.json({
      data: dataResult.rows,
      pagination: {
        page,
        limit,
        total,
        totalPages,
        hasNext: page < totalPages,
        hasPrev: page > 1
      }
    });
    
  } catch (error) {
    console.error(`❌ Erro ao buscar dados da tabela ${req.params.tableName}:`, error.message);
    res.status(500).json({ 
      error: 'Erro ao buscar dados da tabela',
      details: error.message 
    });
  }
});

module.exports = router;