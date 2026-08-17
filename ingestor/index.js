/**
 * Ingestor GenieACS -> Kafka
 *
 * Objetivo: ser um serviço fino e stateless (exceto pelo cursor de
 * polling, guardado no Redis). Sem motor de workflow, sem histórico
 * de execução, sem overhead por evento — só consulta a NBI e publica.
 *
 * Estratégia: polling incremental. A cada POLL_INTERVAL_MS, consulta
 * dispositivos cujo `_lastInform` seja maior que o cursor salvo,
 * publica um evento por dispositivo no Kafka, e avança o cursor.
 *
 * Isso evita depender de webhook/push do GenieACS (que exigiria
 * configurar provisions/extensions) e escala de forma simples: se um
 * dia o polling virar gargalo, a saída é reduzir POLL_INTERVAL_MS
 * e/ou paralelizar por faixa de device_id — não precisa reescrever
 * a lógica.
 */

const fetch = require('node-fetch');
const { Kafka } = require('kafkajs');
const Redis = require('ioredis');

const NBI_HOST = process.env.NBI_HOST || 'nginx';
const NBI_PORT = process.env.NBI_PORT || '7557';
const NBI_BASE_URL = `http://${NBI_HOST}:${NBI_PORT}`;

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'device-events';

const REDIS_HOST = process.env.REDIS_HOST || 'redis';
const REDIS_PORT = process.env.REDIS_PORT || '6379';
const REDIS_CURSOR_KEY = 'ingestor:last_inform_cursor';

const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || '10000', 10);
const POLL_PAGE_LIMIT = parseInt(process.env.POLL_PAGE_LIMIT || '500', 10);

const redis = new Redis({ host: REDIS_HOST, port: Number(REDIS_PORT) });

const kafka = new Kafka({
  clientId: 'ingestor',
  brokers: [KAFKA_BROKER],
});
const producer = kafka.producer();

// Extrai só os campos relevantes do documento do GenieACS.
// Mantém alinhado com o que o coletor_cpe.py já lê (TR-098 com
// fallback TR-181), pra não duplicar convenção de parsing no projeto.
function getParamValue(deviceData, path) {
  const keys = path.split('.');
  let current = deviceData;
  for (const key of keys) {
    if (current && typeof current === 'object' && key in current) {
      current = current[key];
    } else {
      return null;
    }
  }
  if (current && typeof current === 'object' && '_value' in current) {
    return current._value;
  }
  return null;
}

function readPath(deviceData, tr098Path, tr181Path) {
  let val = getParamValue(deviceData, tr098Path);
  if (val === null || val === undefined) {
    val = getParamValue(deviceData, tr181Path);
  }
  return val === null || val === undefined ? null : val;
}

function buildEvent(deviceDoc) {
  return {
    device_id: deviceDoc._id,
    last_inform: deviceDoc._lastInform,
    timestamp: new Date().toISOString(),
    event_type: 'inform',
    payload: {
      connection_status: readPath(
        deviceDoc,
        'InternetGatewayDevice.WANDevice.1.WANConnectionDevice.1.WANIPConnection.1.ConnectionStatus',
        'Device.IP.Interface.1.Status'
      ),
      rx_power_dbm: readPath(
        deviceDoc,
        'InternetGatewayDevice.WANDevice.1.X_GPON_InterfaceConfig.RXPower',
        'Device.Optical.Interface.1.OpticalSignalLevel'
      ),
      tx_power_dbm: readPath(
        deviceDoc,
        'InternetGatewayDevice.WANDevice.1.X_GPON_InterfaceConfig.TXPower',
        'Device.Optical.Interface.1.TransmitOpticalLevel'
      ),
      uptime_seconds: readPath(
        deviceDoc,
        'InternetGatewayDevice.DeviceInfo.UpTime',
        'Device.DeviceInfo.UpTime'
      ),
      software_version: readPath(
        deviceDoc,
        'InternetGatewayDevice.DeviceInfo.SoftwareVersion',
        'Device.DeviceInfo.SoftwareVersion'
      ),
    },
  };
}

async function fetchChangedDevices(sinceTimestamp) {
  const query = JSON.stringify({ _lastInform: { $gt: sinceTimestamp } });
  const params = new URLSearchParams({
    query,
    limit: String(POLL_PAGE_LIMIT),
    sort: JSON.stringify({ _lastInform: 1 }),
  });
  const url = `${NBI_BASE_URL}/devices?${params.toString()}`;
  const response = await fetch(url, { timeout: 15000 });
  if (!response.ok) {
    throw new Error(`NBI respondeu ${response.status}: ${await response.text()}`);
  }
  return response.json();
}

async function pollOnce() {
  const cursorRaw = await redis.get(REDIS_CURSOR_KEY);
  // Se não há cursor salvo, começa do momento atual (não faz backfill
  // histórico automático — evita processar a base inteira na primeira
  // subida do serviço).
  const cursor = cursorRaw ? Number(cursorRaw) : Date.now();

  const devices = await fetchChangedDevices(cursor);
  if (!devices.length) {
    return;
  }

  const messages = devices.map((doc) => {
    const event = buildEvent(doc);
    return {
      key: event.device_id,
      value: JSON.stringify(event),
    };
  });

  await producer.send({
    topic: KAFKA_TOPIC,
    messages,
  });

  const newCursor = Math.max(...devices.map((d) => Number(d._lastInform) || 0));
  if (newCursor > cursor) {
    await redis.set(REDIS_CURSOR_KEY, String(newCursor));
  }

  console.log(`[ingestor] publicados ${messages.length} eventos, cursor -> ${newCursor}`);
}

async function main() {
  await producer.connect();
  console.log(`[ingestor] conectado ao Kafka (${KAFKA_BROKER}), tópico "${KAFKA_TOPIC}"`);
  console.log(`[ingestor] consultando NBI em ${NBI_BASE_URL}, poll a cada ${POLL_INTERVAL_MS}ms`);

  // Loop simples. Se pollOnce() falhar (NBI fora do ar, Kafka
  // indisponível etc.), loga o erro e tenta de novo no próximo ciclo
  // — não derruba o processo.
  setInterval(() => {
    pollOnce().catch((err) => {
      console.error('[ingestor] erro no ciclo de polling:', err.message);
    });
  }, POLL_INTERVAL_MS);
}

main().catch((err) => {
  console.error('[ingestor] falha fatal na inicialização:', err);
  process.exit(1);
});