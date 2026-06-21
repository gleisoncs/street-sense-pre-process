CREATE EXTENSION IF NOT EXISTS postgis;

-- Region boundary polygons researched via osmnx, used to filter occurrences
-- by geography. Distinguished by country/state/city/(neighborhood) so that
-- same-named cities don't collide, and a city can hold many bairros.
CREATE TABLE IF NOT EXISTS regioes (
    id          SERIAL PRIMARY KEY,
    nome        VARCHAR(150),                 -- display name / query used
    tipo        VARCHAR(20),                  -- 'cidade' | 'bairro'
    bairro      VARCHAR(150),                 -- NULL for city-level rows
    cidade      VARCHAR(150),
    estado      VARCHAR(100),
    pais        VARCHAR(100),
    osm_id      BIGINT,
    geom        GEOMETRY(MultiPolygon, 4326),
    created_at  TIMESTAMP DEFAULT NOW()
);

-- One row per (country, state, city, type, neighborhood). COALESCE handles the
-- NULL bairro of city-level rows so re-imports upsert instead of duplicating.
CREATE UNIQUE INDEX IF NOT EXISTS uq_regioes_key
    ON regioes (pais, estado, cidade, tipo, COALESCE(bairro, ''));

CREATE INDEX IF NOT EXISTS idx_regioes_geom ON regioes USING GIST (geom);
