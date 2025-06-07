-- sql/create_database_structure.sql
-- Estructura de base de datos para el proyecto de videojuegos

-- Crear base de datos (ejecutar como superusuario)
-- CREATE DATABASE vg_core;

-- Conectar a la base de datos vg_core
\c vg_core;

-- =====================================================
-- SCHEMA PARA LOGS
-- =====================================================
CREATE SCHEMA IF NOT EXISTS logs;

-- Tabla de logs para tracking de ETL
CREATE TABLE IF NOT EXISTS logs.etl_logs (
    id SERIAL PRIMARY KEY,
    load_start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    load_end_time TIMESTAMP WITH TIME ZONE NOT NULL,
    records_count INTEGER NOT NULL DEFAULT 0,
    source VARCHAR(50) NOT NULL,
    endpoint VARCHAR(200),
    output_file VARCHAR(200),
    duration_seconds DECIMAL(10,2) GENERATED ALWAYS AS 
        (EXTRACT(EPOCH FROM (load_end_time - load_start_time))) STORED,
    status VARCHAR(20) DEFAULT 'SUCCESS',
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Índices para la tabla de logs
CREATE INDEX IF NOT EXISTS idx_etl_logs_source ON logs.etl_logs(source);
CREATE INDEX IF NOT EXISTS idx_etl_logs_start_time ON logs.etl_logs(load_start_time);
CREATE INDEX IF NOT EXISTS idx_etl_logs_status ON logs.etl_logs(status);

-- =====================================================
-- SCHEMA CORE - DATOS LIMPIOS
-- =====================================================
CREATE SCHEMA IF NOT EXISTS core;

-- Tabla para datos de Twitch
CREATE TABLE IF NOT EXISTS core.twitch_games (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(50) NOT NULL,
    name VARCHAR(500) NOT NULL,
    box_art_url TEXT,
    igdb_id VARCHAR(50),
    
    -- Campos técnicos DW
    dw_fecha_registro TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    dw_id_carga INTEGER REFERENCES logs.etl_logs(id),
    dw_deleted BOOLEAN DEFAULT FALSE,
    dw_source VARCHAR(50) DEFAULT 'TWITCH',
    dw_endpoint VARCHAR(200),
    
    UNIQUE(game_id, dw_id_carga)
);

CREATE TABLE IF NOT EXISTS core.twitch_streams (
    id SERIAL PRIMARY KEY,
    stream_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    user_login VARCHAR(100),
    user_name VARCHAR(100),
    game_id VARCHAR(50),
    game_name VARCHAR(500),
    stream_type VARCHAR(20),
    title TEXT,
    viewer_count INTEGER DEFAULT 0,
    started_at TIMESTAMP WITH TIME ZONE,
    language VARCHAR(10),
    thumbnail_url TEXT,
    tag_ids TEXT[], -- Array de tags
    is_mature BOOLEAN DEFAULT FALSE,
    
    -- Campos técnicos DW
    dw_fecha_registro TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    dw_id_carga INTEGER REFERENCES logs.etl_logs(id),
    dw_deleted BOOLEAN DEFAULT FALSE,
    dw_source VARCHAR(50) DEFAULT 'TWITCH',
    dw_endpoint VARCHAR(200)
);

-- Tabla para datos de Pokémon Showdown
CREATE TABLE IF NOT EXISTS core.showdown_battles (
    id SERIAL PRIMARY KEY,
    battle_id VARCHAR(100) NOT NULL,
    format VARCHAR(50),
    player1_name VARCHAR(100),
    player1_rating INTEGER,
    player2_name VARCHAR(100),
    player2_rating INTEGER,
    winner VARCHAR(100),
    battle_date TIMESTAMP WITH TIME ZONE,
    turns_count INTEGER,
    duration_seconds INTEGER,
    
    -- Campos técnicos DW
    dw_fecha_registro TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    dw_id_carga INTEGER REFERENCES logs.etl_logs(id),
    dw_deleted BOOLEAN DEFAULT FALSE,
    dw_source VARCHAR(50) DEFAULT 'SHOWDOWN',
    dw_endpoint VARCHAR(200),
    
    UNIQUE(battle_id, dw_id_carga)
);

CREATE TABLE IF NOT EXISTS core.showdown_pokemon_usage (
    id SERIAL PRIMARY KEY,
    pokemon_name VARCHAR(100) NOT NULL,
    format VARCHAR(50) NOT NULL,
    usage_percentage DECIMAL(5,2),
    raw_usage_count INTEGER,
    raw_usage_percentage DECIMAL(5,2),
    real_usage_count INTEGER,
    real_usage_percentage DECIMAL(5,2),
    month_year VARCHAR(7), -- Format: YYYY-MM
    
    -- Campos técnicos DW
    dw_fecha_registro TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    dw_id_carga INTEGER REFERENCES logs.etl_logs(id),
    dw_deleted BOOLEAN DEFAULT FALSE,
    dw_source VARCHAR(50) DEFAULT 'SHOWDOWN',
    dw_endpoint VARCHAR(200),
    
    UNIQUE(pokemon_name, format, month_year, dw_id_carga)
);

-- Tabla para datos de HowLongToBeat
CREATE TABLE IF NOT EXISTS core.hltb_games (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(50) NOT NULL,
    game_title VARCHAR(500) NOT NULL,
    game_alias VARCHAR(500),
    game_type VARCHAR(50),
    game_image TEXT,
    game_image_url TEXT,
    comp_main DECIMAL(6,2), -- Horas para historia principal
    comp_plus DECIMAL(6,2), -- Horas para historia + extras  
    comp_100 DECIMAL(6,2),  -- Horas para completar 100%
    comp_all DECIMAL(6,2),  -- Promedio de todos los estilos
    invested_co DECIMAL(6,2), -- Co-op
    invested_mp DECIMAL(6,2), -- Multiplayer
    count_comp INTEGER DEFAULT 0,
    count_speedrun INTEGER DEFAULT 0,
    count_backlog INTEGER DEFAULT 0,
    count_review INTEGER DEFAULT 0,
    review_score DECIMAL(3,1),
    
    -- Campos técnicos DW
    dw_fecha_registro TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    dw_id_carga INTEGER REFERENCES logs.etl_logs(id),
    dw_deleted BOOLEAN DEFAULT FALSE,
    dw_source VARCHAR(50) DEFAULT 'HLTB',
    dw_endpoint VARCHAR(200),
    
    UNIQUE(game_id, dw_id_carga)
);

-- =====================================================
-- ÍNDICES PARA OPTIMIZAR CONSULTAS
-- =====================================================

-- Índices Twitch
CREATE INDEX IF NOT EXISTS idx_twitch_games_name ON core.twitch_games(name);
CREATE INDEX IF NOT EXISTS idx_twitch_games_dw_source ON core.twitch_games(dw_source);
CREATE INDEX IF NOT EXISTS idx_twitch_streams_game_id ON core.twitch_streams(game_id);
CREATE INDEX IF NOT EXISTS idx_twitch_streams_viewer_count ON core.twitch_streams(viewer_count DESC);
CREATE INDEX IF NOT EXISTS idx_twitch_streams_started_at ON core.twitch_streams(started_at);

-- Índices Showdown
CREATE INDEX IF NOT EXISTS idx_showdown_battles_format ON core.showdown_battles(format);
CREATE INDEX IF NOT EXISTS idx_showdown_battles_date ON core.showdown_battles(battle_date);
CREATE INDEX IF NOT EXISTS idx_showdown_pokemon_format ON core.showdown_pokemon_usage(format);
CREATE INDEX IF NOT EXISTS idx_showdown_pokemon_usage ON core.showdown_pokemon_usage(usage_percentage DESC);

-- Índices HowLongToBeat
CREATE INDEX IF NOT EXISTS idx_hltb_games_title ON core.hltb_games(game_title);
CREATE INDEX IF NOT EXISTS idx_hltb_games_comp_main ON core.hltb_games(comp_main);
CREATE INDEX IF NOT EXISTS idx_hltb_games_review_score ON core.hltb_games(review_score DESC);

-- =====================================================
-- VISTAS PARA ANÁLISIS Y KPIs
-- =====================================================

-- Vista de juegos más populares en Twitch
CREATE OR REPLACE VIEW core.v_twitch_popular_games AS
SELECT 
    tg.name as game_name,
    COUNT(ts.stream_id) as active_streams,
    AVG(ts.viewer_count) as avg_viewers,
    SUM(ts.viewer_count) as total_viewers,
    MAX(ts.viewer_count) as max_viewers
FROM core.twitch_games tg
LEFT JOIN core.twitch_streams ts ON tg.game_id = ts.game_id
WHERE tg.dw_deleted = FALSE AND (ts.dw_deleted = FALSE OR ts.dw_deleted IS NULL)
GROUP BY tg.game_id, tg.name
ORDER BY total_viewers DESC;

-- Vista de Pokémon más usados
CREATE OR REPLACE VIEW core.v_showdown_top_pokemon AS
SELECT 
    pokemon_name,
    format,
    AVG(usage_percentage) as avg_usage_percentage,
    COUNT(*) as months_tracked,
    MAX(month_year) as latest_month
FROM core.showdown_pokemon_usage
WHERE dw_deleted = FALSE
GROUP BY pokemon_name, format
ORDER BY avg_usage_percentage DESC;

-- Vista de juegos por duración
CREATE OR REPLACE VIEW core.v_hltb_games_by_duration AS
SELECT 
    game_title,
    comp_main,
    comp_plus,
    comp_100,
    review_score,
    CASE 
        WHEN comp_main < 10 THEN 'Corto (< 10h)'
        WHEN comp_main BETWEEN 10 AND 25 THEN 'Medio (10-25h)'
        WHEN comp_main BETWEEN 25 AND 50 THEN 'Largo (25-50h)'
        WHEN comp_main > 50 THEN 'Muy Largo (> 50h)'
        ELSE 'Sin datos'
    END as duration_category
FROM core.hltb_games
WHERE dw_deleted = FALSE AND comp_main IS NOT NULL
ORDER BY comp_main DESC;

-- =====================================================
-- FUNCIONES AUXILIARES
-- =====================================================

-- Función para limpiar y obtener estadísticas de carga
CREATE OR REPLACE FUNCTION logs.get_load_summary(p_source VARCHAR DEFAULT NULL)
  RETURNS TABLE (
    source             VARCHAR,
    total_loads        BIGINT,
    total_records      BIGINT,
    avg_duration_seconds  DECIMAL,
    last_load_time     TIMESTAMP WITH TIME ZONE,
    success_rate       DECIMAL
  )
AS $func$
BEGIN
  RETURN QUERY
  SELECT 
    l.source,
    COUNT(*) as total_loads,
    SUM(l.records_count) as total_records,
    AVG(l.duration_seconds) as avg_duration_seconds,
    MAX(l.load_end_time) as last_load_time,
    ROUND(
      (COUNT(*) FILTER (WHERE l.status = 'SUCCESS')::DECIMAL / COUNT(*)) * 100, 
      2
    ) as success_rate
  FROM logs.etl_logs l
  WHERE (p_source IS NULL OR l.source = p_source)
  GROUP BY l.source
  ORDER BY total_records DESC;
END;
$func$ LANGUAGE plpgsql;


-- Función para obtener KPIs de Twitch
CREATE OR REPLACE FUNCTION core.get_twitch_kpis()
  RETURNS TABLE (
    total_games           INTEGER,
    total_active_streams  INTEGER,
    avg_viewers_per_stream DECIMAL,
    top_game              VARCHAR,
    top_game_viewers      BIGINT
  )
AS $func$
BEGIN
  RETURN QUERY
  SELECT 
    (SELECT COUNT(DISTINCT game_id) FROM core.twitch_games WHERE dw_deleted = FALSE)::INTEGER,
    (SELECT COUNT(*)                   FROM core.twitch_streams WHERE dw_deleted = FALSE)::INTEGER,
    (SELECT AVG(viewer_count)          FROM core.twitch_streams WHERE dw_deleted = FALSE),
    (SELECT game_name                  FROM core.v_twitch_popular_games LIMIT 1),
    (SELECT total_viewers              FROM core.v_twitch_popular_games LIMIT 1);
END;
$func$ LANGUAGE plpgsql;


-- =====================================================
-- DATOS DE EJEMPLO / TESTING
-- =====================================================

-- Insertar log de ejemplo
INSERT INTO logs.etl_logs (load_start_time, load_end_time, records_count, source, endpoint, output_file)
VALUES 
(NOW() - INTERVAL '1 hour', NOW() - INTERVAL '55 minutes', 1000, 'TWITCH', 'games/top', 'twitch_games_sample'),
(NOW() - INTERVAL '50 minutes', NOW() - INTERVAL '45 minutes', 2500, 'TWITCH', 'streams', 'twitch_streams_sample'),
(NOW() - INTERVAL '40 minutes', NOW() - INTERVAL '35 minutes', 500, 'SHOWDOWN', 'battles', 'showdown_battles_sample'),
(NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '25 minutes', 750, 'HLTB', 'games', 'hltb_games_sample');

-- =====================================================
-- PERMISOS Y USUARIOS
-- =====================================================

-- Crear usuario para la aplicación (ejecutar como superusuario)
-- CREATE USER vg_etl_user WITH PASSWORD 'your_secure_password';
-- GRANT CONNECT ON DATABASE vg_core TO vg_etl_user;
-- GRANT USAGE ON SCHEMA logs, core TO vg_etl_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA logs, core TO vg_etl_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA logs, core TO vg_etl_user;

-- Usuario de solo lectura para dashboards
-- CREATE USER vg_dashboard_user WITH PASSWORD 'dashboard_password';
-- GRANT CONNECT ON DATABASE vg_core TO vg_dashboard_user;
-- GRANT USAGE ON SCHEMA core TO vg_dashboard_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA core TO vg_dashboard_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA logs TO vg_dashboard_user;

-- =====================================================
-- COMENTARIOS PARA DOCUMENTACIÓN
-- =====================================================

COMMENT ON SCHEMA logs IS 'Schema para almacenar logs y metadatos del proceso ETL';
COMMENT ON SCHEMA core IS 'Schema principal con datos limpios y transformados';

COMMENT ON TABLE logs.etl_logs IS 'Registro de todas las operaciones ETL realizadas';
COMMENT ON TABLE core.twitch_games IS 'Juegos extraídos de Twitch API';
COMMENT ON TABLE core.twitch_streams IS 'Streams activos extraídos de Twitch API';
COMMENT ON TABLE core.showdown_battles IS 'Batallas de Pokémon Showdown';
COMMENT ON TABLE core.showdown_pokemon_usage IS 'Estadísticas de uso de Pokémon por formato';
COMMENT ON TABLE core.hltb_games IS 'Información de duración de juegos de HowLongToBeat';

-- Verificar que todo se creó correctamente
SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables 
WHERE schemaname IN ('logs', 'core')
ORDER BY schemaname, tablename;