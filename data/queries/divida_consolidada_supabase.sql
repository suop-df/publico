-- Tabela Supabase para contingência do dashboard Dívida Consolidada
-- Mesmo padrão de rcl / resultado_primario_nominal / minimo_educacao:
-- 1 linha por ano, coluna "dados" em JSONB com o mesmo objeto do gz.
-- Rodar manualmente no SQL Editor do Supabase.

CREATE TABLE IF NOT EXISTS divida_consolidada (
    ano            INT PRIMARY KEY,
    dados          JSONB NOT NULL,
    atualizado_em  TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE divida_consolidada ENABLE ROW LEVEL SECURITY;

CREATE POLICY "divida_consolidada_public_read"
    ON divida_consolidada FOR SELECT
    USING (true);

CREATE POLICY "divida_consolidada_public_insert"
    ON divida_consolidada FOR INSERT
    WITH CHECK (true);

CREATE POLICY "divida_consolidada_public_update"
    ON divida_consolidada FOR UPDATE
    USING (true)
    WITH CHECK (true);
