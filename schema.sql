-- ========================================================
-- Installation success funnel, per CPE
-- Stages 1-4: fed automatically by the Consumer (Kafka)
-- Stages 5-6: simulated/manual for now (no real ticketing
--             system integrated yet)
-- ========================================================

CREATE TABLE device_lifecycle (
    device_id                 TEXT PRIMARY KEY,

    -- Stage 1: Installation completed
    installation_completed_at TIMESTAMPTZ,

    -- Stage 2: CPE provisioned (first successful contact with GenieACS)
    provisioned_at            TIMESTAMPTZ,

    -- Stage 3: Connectivity confirmed (WAN connected / ping OK)
    connectivity_ok_at        TIMESTAMPTZ,

    -- Stage 4: ongoing degradation monitoring
    last_quality_score        INTEGER,          -- 0-100
    last_checked_at           TIMESTAMPTZ,
    degradation_detected      BOOLEAN DEFAULT FALSE,

    -- Stage 5: technical visit (SIMULATED for now)
    last_technical_visit_at   TIMESTAMPTZ,

    -- Stage 6: support ticket (SIMULATED for now)
    last_support_ticket_at    TIMESTAMPTZ,

    created_at                TIMESTAMPTZ DEFAULT now(),
    updated_at                TIMESTAMPTZ DEFAULT now()
);

-- Raw history of every event received from Kafka.
-- Stages 1-4 above are derived from here (the consumer updates
-- device_lifecycle whenever it processes one of these events).
-- Also doubles as the alert log: transitions like disconnection,
-- unexpected reboot, or signal out of range are logged here with
-- their own event_type, no separate alerts table needed.
CREATE TABLE device_events (
    id          BIGSERIAL PRIMARY KEY,
    device_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,      -- 'inform', 'installation_completed', 'provision_ok',
                                     -- 'ping_ok', 'disconnection_detected', 'unexpected_reboot',
                                     -- 'signal_degraded'
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_device_events_device_id ON device_events (device_id);
CREATE INDEX idx_device_events_received_at ON device_events (received_at);
CREATE INDEX idx_device_events_event_type ON device_events (event_type);

-- "Tickets/visits" table -- for now you insert manually here to
-- test the funnel logic, until there's a real ticketing system
-- to integrate.
CREATE TABLE simulated_tickets (
    id          BIGSERIAL PRIMARY KEY,
    device_id   TEXT NOT NULL,
    type        TEXT NOT NULL CHECK (type IN ('technical_visit', 'support_ticket')),
    description TEXT,
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- ========================================================
-- View: real-time funnel status per CPE
-- ========================================================
CREATE VIEW installation_success_funnel AS
SELECT
    dl.device_id,
    dl.installation_completed_at IS NOT NULL                             AS stage1_installation,
    dl.provisioned_at IS NOT NULL                                        AS stage2_provisioned,
    dl.connectivity_ok_at IS NOT NULL                                    AS stage3_connectivity,
    NOT dl.degradation_detected                                         AS stage4_no_degradation,
    dl.last_technical_visit_at IS NULL
        OR dl.last_technical_visit_at < dl.installation_completed_at     AS stage5_no_new_visit,
    dl.last_support_ticket_at IS NULL
        OR dl.last_support_ticket_at < dl.installation_completed_at      AS stage6_no_ticket,
    (dl.installation_completed_at IS NOT NULL
        AND dl.provisioned_at IS NOT NULL
        AND dl.connectivity_ok_at IS NOT NULL
        AND NOT dl.degradation_detected
        AND (dl.last_technical_visit_at IS NULL OR dl.last_technical_visit_at < dl.installation_completed_at)
        AND (dl.last_support_ticket_at IS NULL OR dl.last_support_ticket_at < dl.installation_completed_at)
        AND dl.installation_completed_at < now() - INTERVAL '30 days'
    ) AS installation_successful
FROM device_lifecycle dl;