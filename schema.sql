-- ========================================================
-- Funil de sucesso de instalação por CPE
-- Etapas 1-4: alimentadas automaticamente pelo Consumer (Kafka)
-- Etapas 5-6: simuladas/manuais por enquanto (sem sistema de
--             chamados real integrado ainda)
-- ========================================================

CREATE TABLE device_lifecycle (
    device_id               TEXT PRIMARY KEY,

    -- Etapa 1: Instalação concluída
    instalacao_concluida_em TIMESTAMPTZ,

    -- Etapa 2: CPE provisionado (provision rodou sem erro no GenieACS)
    provisionado_em         TIMESTAMPTZ,

    -- Etapa 3: Teste de conectividade OK (IPPing/status WAN)
    conectividade_ok_em     TIMESTAMPTZ,

    -- Etapa 4: acompanhamento contínuo de degradação
    ultimo_score_qualidade  INTEGER,          -- 0-100, o mesmo score que já calculamos
    ultima_verificacao_em   TIMESTAMPTZ,
    degradacao_detectada    BOOLEAN DEFAULT FALSE,

    -- Etapa 5: visita técnica (SIMULADO por enquanto)
    ultima_visita_tecnica_em TIMESTAMPTZ,

    -- Etapa 6: chamado de suporte (SIMULADO por enquanto)
    ultimo_chamado_suporte_em TIMESTAMPTZ,

    criado_em                TIMESTAMPTZ DEFAULT now(),
    atualizado_em             TIMESTAMPTZ DEFAULT now()
);

-- Histórico bruto de todos os eventos recebidos do Kafka
-- (etapas 1-4 são derivadas daqui + atualizam device_lifecycle)
CREATE TABLE device_events (
    id          BIGSERIAL PRIMARY KEY,
    device_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,      -- ex: 'inform', 'provision_ok', 'ping_ok'
    payload     JSONB NOT NULL,
    recebido_em TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_device_events_device_id ON device_events (device_id);
CREATE INDEX idx_device_events_recebido_em ON device_events (recebido_em);

-- Tabela de "chamados/visitas" simulados -- por enquanto você
-- insere manualmente aqui pra testar a lógica do funil, até ter
-- um sistema real de ticketing pra integrar
CREATE TABLE tickets_simulados (
    id          BIGSERIAL PRIMARY KEY,
    device_id   TEXT NOT NULL,
    tipo        TEXT NOT NULL CHECK (tipo IN ('visita_tecnica', 'chamado_suporte')),
    descricao   TEXT,
    criado_em   TIMESTAMPTZ DEFAULT now()
);

-- ========================================================
-- View: calcula o status do funil em tempo real por CPE
-- ========================================================
CREATE VIEW funil_sucesso_instalacao AS
SELECT
    dl.device_id,
    dl.instalacao_concluida_em IS NOT NULL                                  AS etapa1_instalacao,
    dl.provisionado_em IS NOT NULL                                          AS etapa2_provisionado,
    dl.conectividade_ok_em IS NOT NULL                                      AS etapa3_conectividade,
    NOT dl.degradacao_detectada                                             AS etapa4_sem_degradacao,
    dl.ultima_visita_tecnica_em IS NULL
        OR dl.ultima_visita_tecnica_em < dl.instalacao_concluida_em         AS etapa5_sem_nova_visita,
    dl.ultimo_chamado_suporte_em IS NULL
        OR dl.ultimo_chamado_suporte_em < dl.instalacao_concluida_em        AS etapa6_sem_chamado,
    (dl.instalacao_concluida_em IS NOT NULL
        AND dl.provisionado_em IS NOT NULL
        AND dl.conectividade_ok_em IS NOT NULL
        AND NOT dl.degradacao_detectada
        AND (dl.ultima_visita_tecnica_em IS NULL OR dl.ultima_visita_tecnica_em < dl.instalacao_concluida_em)
        AND (dl.ultimo_chamado_suporte_em IS NULL OR dl.ultimo_chamado_suporte_em < dl.instalacao_concluida_em)
        AND dl.instalacao_concluida_em < now() - INTERVAL '30 days'
    ) AS instalacao_bem_sucedida
FROM device_lifecycle dl;