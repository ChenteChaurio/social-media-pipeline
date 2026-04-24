-- Crear tablas para el pipeline de redes sociales

-- Tabla de posts crudos
CREATE TABLE IF NOT EXISTS raw_posts (
  id SERIAL PRIMARY KEY,
  user_id VARCHAR(100),
  username VARCHAR(100),
  text TEXT,
  hashtags TEXT[],
  timestamp TIMESTAMP DEFAULT NOW(),
  created_at TIMESTAMP DEFAULT NOW()
);

-- Tabla de análisis de sentimiento
CREATE TABLE IF NOT EXISTS sentiment_analysis (
  id SERIAL PRIMARY KEY,
  post_id INTEGER,
  text TEXT,
  sentiment VARCHAR(20), -- positivo, negativo, neutro
  confidence FLOAT,
  timestamp TIMESTAMP DEFAULT NOW(),
  created_at TIMESTAMP DEFAULT NOW()
);

-- Tabla de trending hashtags
CREATE TABLE IF NOT EXISTS trending_hashtags (
  id SERIAL PRIMARY KEY,
  hashtag VARCHAR(255),
  total_posts INTEGER,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Tabla de alertas (contenido sensible)
CREATE TABLE IF NOT EXISTS alert_events (
  id SERIAL PRIMARY KEY,
  user_id VARCHAR(100),
  username VARCHAR(100),
  text TEXT,
  alert_reason VARCHAR(255),
  timestamp TIMESTAMP DEFAULT NOW(),
  created_at TIMESTAMP DEFAULT NOW()
);

-- Índices para mejorar queries
CREATE INDEX idx_raw_posts_timestamp ON raw_posts(timestamp);
CREATE INDEX idx_sentiment_timestamp ON sentiment_analysis(timestamp);
CREATE INDEX idx_trending_window ON trending_hashtags(window_start);
CREATE INDEX idx_alerts_timestamp ON alert_events(timestamp);

-- Insert de datos de ejemplo para testing
INSERT INTO sentiment_analysis (text, sentiment, confidence) VALUES
  ('Me encanta este proyecto!', 'positivo', 0.95),
  ('Esto es terrible', 'negativo', 0.88),
  ('Es un día normal', 'neutro', 0.72);

INSERT INTO trending_hashtags (hashtag, total_posts, window_start, window_end) VALUES
  ('#trending', 150, NOW() - INTERVAL '5 minutes', NOW()),
  ('#news', 120, NOW() - INTERVAL '5 minutes', NOW()),
  ('#tech', 95, NOW() - INTERVAL '5 minutes', NOW());
