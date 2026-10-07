-- =============================================================================
-- QueryAstra — Setup inicial do banco de dados
-- =============================================================================
-- Execute ANTES de subir o container pela primeira vez, como superusuário
-- PostgreSQL (com TimescaleDB instalado no servidor).
--
-- Exemplo de uso:
--   psql -U postgres -f scripts/create_db.sql
--
-- Se o servidor for gerenciado (RDS, Azure, Neon, Supabase etc.), crie o banco
-- e o usuário pelo painel do provedor e pule direto para o passo "Extensão".
-- =============================================================================

-- 1. Usuário da aplicação
--    Altere a senha antes de executar em produção!
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'queryastra') THEN
        CREATE USER queryastra WITH ENCRYPTED PASSWORD 'changeme';
    END IF;
END
$$;

-- 2. Banco de dados
SELECT 'CREATE DATABASE queryastra OWNER queryastra'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'queryastra') \gexec

-- 3. Permissões no banco
GRANT ALL PRIVILEGES ON DATABASE queryastra TO queryastra;

-- =============================================================================
-- Conecte-se ao banco queryastra e execute o bloco abaixo:
--   psql -U postgres -d queryastra -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"
--
-- Ou adicione ao comando acima com \c:
-- =============================================================================
-- \c queryastra
-- CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
-- GRANT USAGE ON SCHEMA public TO queryastra;
-- GRANT CREATE ON SCHEMA public TO queryastra;
