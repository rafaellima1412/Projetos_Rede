/**
 * Consumer: device-events (Kafka) -> device_events + device_lifecycle (Postgres)
 *
 * Cada mensagem do Kafka já chega com um event_type ('inform',
 * 'installation_completed', 'provision_ok', 'ping_ok'). O consumer:
 *   1. grava a mensagem crua em device_events (histórico completo)
 *   2. atualiza device_lifecycle de acordo com o event_type
 *   3. no caso de 'inform', compara com o último estado (Redis) pra
 *      detectar degradação/desconexão/reboot e loga isso também como
 *      um device_events (event_type próprio), publicando em
 *      alertas-ftt se algo mudou de patamar
 *
 * Não existe tabela separada de "estado atual" ou "alertas" -- o
 * device_lifecycle já cobre o resumo por CPE, e o device_events já
 * cobre o histórico bruto (alertas incluídos, como um tipo de evento
 * a mais).
 */

const { Kafka } = require('kafkajs');
const { Pool } = require('pg');
const Redis = require('ioredis');

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'device-events';
const KAFKA_ALERTS_TOPIC = process.env.KAFKA_ALERTS_TOPIC || 'alertas-ftt';
const CONSUMER_GROUP_ID = process.env.CONSUMER_GROUP_ID || 'ftt-consumer-group';

const PG_HOST = process.env.PG_HOST || 'postgres';
const PG_PORT = process.env.PG_PORT || '5432';
const PG_USER = process.env.PG_USER || 'postgres';
const PG_PASSWORD = process.env.PG_PASSWORD || 'postgres';
const PG_DATABASE = process.env.PG_DATABASE || 'genieacs_funil';

const REDIS_HOST = process.env.REDIS_HOST || 'redis';
const REDIS_PORT = process.env.REDIS_PORT || '6379';

// Faixa aceitável de sinal óptico (dBm). Sem limiar universal --
// ajustar por vendor/ONU se a base for heterogênea.
const RX_POWER_MIN_DBM = parseFloat(process.env.RX_POWER_MIN_DBM || '-27');
const RX_POWER_MAX_DBM = parseFloat(process.env.RX_POWER_MAX_DBM || '-8');

const redis = new Redis({ host: REDIS_HOST, port: Number(REDIS_PORT) });

const pgPool = new Pool({
  host: PG_HOST,
  port: Number(PG_PORT),
  user: PG_USER,
  password: PG_PASSWORD,
  database: PG_DATABASE,
});

const kafka = new Kafka({ clientId: 'consumer', brokers: [KAFKA_BROKER] });
const kafkaConsumer = kafka.consumer({ groupId: CONSUMER_GROUP_ID });
const kafkaProducer = kafka.producer();

function redisStateKey(deviceId) {
  return `device:last_telemetry:${deviceId}`;
}

function isConnected(connectionStatus) {
  if (connectionStatus === null || connectionStatus === undefined) return null;
  const normalized = String(connectionStatus).toLowerCase();
  return normalized === 'connected' || normalized === 'up' || normalized === '1';
}

function isRxPowerOutOfRange(rxPowerDbm) {
  if (rxPowerDbm === null || rxPowerDbm === undefined) return false;
  const value = Number(rxPowerDbm);
  if (Number.isNaN(value)) return false;
  return value < RX_POWER_MIN_DBM || value > RX_POWER_MAX_DBM;
}

// Heurística simples de score de qualidade (0-100). Ponto de
// partida documentado -- ajustar pesos conforme dado real.
function calculateQualityScore(payload) {
  let score = 100;
  const connected = isConnected(payload.connection_status);
  if (connected === false) {
    return 0;
  }
  if (isRxPowerOutOfRange(payload.rx_power_dbm)) {
    score -= 40;
  }
  return Math.max(0, score);
}

async function ensureLifecycleRow(deviceId) {
  await pgPool.query(
    `INSERT INTO device_lifecycle (device_id)
     VALUES ($1)
     ON CONFLICT (device_id) DO NOTHING`,
    [deviceId]
  );
}

async function logEvent(deviceId, eventType, payload, receivedAt) {
  await pgPool.query(
    `INSERT INTO device_events (device_id, event_type, payload, received_at)
     VALUES ($1, $2, $3, COALESCE($4, now()))`,
    [deviceId, eventType, JSON.stringify(payload || {}), receivedAt || null]
  );
}

async function handleInform(deviceId, payload, receivedAt) {
  const previousRaw = await redis.get(redisStateKey(deviceId));
  const previous = previousRaw ? JSON.parse(previousRaw) : null;

  const qualityScore = calculateQualityScore(payload);
  const degraded = qualityScore < 100;

  await pgPool.query(
    `UPDATE device_lifecycle SET
       last_quality_score = $2,
       last_checked_at = COALESCE($3, now()),
       degradation_detected = $4,
       -- primeira vez que vemos o device = provisionamento confirmado
       provisioned_at = COALESCE(provisioned_at, COALESCE($3, now())),
       -- conectividade OK na primeira vez em que chega conectado
       connectivity_ok_at = CASE
         WHEN connectivity_ok_at IS NULL AND $5 = true THEN COALESCE($3, now())
         ELSE connectivity_ok_at
       END,
       updated_at = now()
     WHERE device_id = $1`,
    [deviceId, qualityScore, receivedAt || null, degraded, isConnected(payload.connection_status) === true]
  );

  const alerts = [];
  if (previous) {
    if (isConnected(previous.connection_status) === true && isConnected(payload.connection_status) === false) {
      alerts.push({ event_type: 'disconnection_detected', detail: { previous: previous.connection_status, current: payload.connection_status } });
    }
    const prevUptime = Number(previous.uptime_seconds);
    const curUptime = Number(payload.uptime_seconds);
    if (!Number.isNaN(prevUptime) && !Number.isNaN(curUptime) && curUptime < prevUptime) {
      alerts.push({ event_type: 'unexpected_reboot', detail: { previous_uptime_seconds: prevUptime, current_uptime_seconds: curUptime } });
    }
  }
  if (isRxPowerOutOfRange(payload.rx_power_dbm)) {
    alerts.push({ event_type: 'signal_degraded', detail: { rx_power_dbm: payload.rx_power_dbm, expected_range: [RX_POWER_MIN_DBM, RX_POWER_MAX_DBM] } });
  }

  for (const alert of alerts) {
    await logEvent(deviceId, alert.event_type, alert.detail, receivedAt);
  }
  if (alerts.length) {
    await kafkaProducer.send({
      topic: KAFKA_ALERTS_TOPIC,
      messages: alerts.map((a) => ({
        key: deviceId,
        value: JSON.stringify({ device_id: deviceId, ...a, timestamp: new Date().toISOString() }),
      })),
    });
    console.log(`[consumer] ${alerts.length} alert(s) published to "${KAFKA_ALERTS_TOPIC}" for ${deviceId}`);
  }

  await redis.set(redisStateKey(deviceId), JSON.stringify(payload));
}

async function handleInstallationCompleted(deviceId, receivedAt) {
  await pgPool.query(
    `UPDATE device_lifecycle SET installation_completed_at = COALESCE($2, now()), updated_at = now()
     WHERE device_id = $1`,
    [deviceId, receivedAt || null]
  );
}

async function handleProvisionOk(deviceId, receivedAt) {
  await pgPool.query(
    `UPDATE device_lifecycle SET provisioned_at = COALESCE($2, now()), updated_at = now()
     WHERE device_id = $1`,
    [deviceId, receivedAt || null]
  );
}

async function handlePingOk(deviceId, receivedAt) {
  await pgPool.query(
    `UPDATE device_lifecycle SET connectivity_ok_at = COALESCE($2, now()), updated_at = now()
     WHERE device_id = $1`,
    [deviceId, receivedAt || null]
  );
}

async function processMessage(message) {
  const event = JSON.parse(message.value.toString());
  const deviceId = event.device_id;
  if (!deviceId) {
    console.warn('[consumer] event without device_id, skipping:', event);
    return;
  }
  const eventType = event.event_type || 'inform';
  const payload = event.payload || {};
  const receivedAt = event.timestamp || null;

  await ensureLifecycleRow(deviceId);
  await logEvent(deviceId, eventType, payload, receivedAt);

  switch (eventType) {
    case 'inform':
      await handleInform(deviceId, payload, receivedAt);
      break;
    case 'installation_completed':
      await handleInstallationCompleted(deviceId, receivedAt);
      break;
    case 'provision_ok':
      await handleProvisionOk(deviceId, receivedAt);
      break;
    case 'ping_ok':
      await handlePingOk(deviceId, receivedAt);
      break;
    default:
      // Tipo desconhecido: já foi logado em device_events acima,
      // não precisa de tratamento especial em device_lifecycle.
      break;
  }
}

async function main() {
  await pgPool.query('SELECT 1'); // falha rápido se o schema não existir ainda
  await kafkaProducer.connect();
  await kafkaConsumer.connect();
  await kafkaConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });

  console.log(`[consumer] consuming "${KAFKA_TOPIC}", group "${CONSUMER_GROUP_ID}"`);
  console.log(`[consumer] alerts -> "${KAFKA_ALERTS_TOPIC}", accepted RX range: ${RX_POWER_MIN_DBM} to ${RX_POWER_MAX_DBM} dBm`);

  await kafkaConsumer.run({
    eachMessage: async ({ message }) => {
      try {
        await processMessage(message);
      } catch (err) {
        console.error('[consumer] error processing message:', err.message);
      }
    },
  });
}

async function shutdown() {
  console.log('[consumer] shutting down...');
  await kafkaConsumer.disconnect().catch(() => {});
  await kafkaProducer.disconnect().catch(() => {});
  await pgPool.end().catch(() => {});
  redis.disconnect();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

main().catch((err) => {
  console.error('[consumer] fatal startup error:', err);
  process.exit(1);
});