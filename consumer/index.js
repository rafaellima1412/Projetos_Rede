const { Kafka } = require("kafkajs");
const { Pool } = require("pg");
const Redis = require("ioredis");
const http = require("http");

// --- Configuração ---
const KAFKA_BROKER = process.env.KAFKA_BROKER || "kafka:9092";
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "device-events";
const NBI_HOST = process.env.NBI_HOST || "nginx";
const NBI_PORT = process.env.NBI_PORT || 7557;

const pool = new Pool({
  host: process.env.PG_HOST || "postgres",
  port: process.env.PG_PORT || 5432,
  user: process.env.PG_USER || "postgres",
  password: process.env.PG_PASSWORD || "postgres",
  database: process.env.PG_DATABASE || "genieacs_funil",
});

const redis = new Redis({
  host: process.env.REDIS_HOST || "redis",
  port: process.env.REDIS_PORT || 6379,
});

const kafka = new Kafka({
  clientId: "consumer-postgres-redis",
  brokers: [KAFKA_BROKER],
});

// Consumer group próprio -- separado do que o n8n vai usar pra alertas,
// assim os dois leem o mesmo tópico de forma independente
const consumer = kafka.consumer({ groupId: "consumer-postgres-redis" });

// --- Helpers reaproveitados da lógica de score que já tínhamos no n8n ---
function getParamValue(deviceData, path) {
  const keys = path.split(".");
  let current = deviceData;
  for (const key of keys) {
    if (current && typeof current === "object" && key in current) {
      current = current[key];
    } else {
      return null;
    }
  }
  if (current && typeof current === "object" && "_value" in current) {
    return current._value;
  }
  return null;
}

function readPath(raw, tr098Path, tr181Path) {
  let val = getParamValue(raw, tr098Path);
  if (val === null) val = getParamValue(raw, tr181Path);
  return val;
}

function fetchDeviceFromNBI(deviceId) {
  return new Promise((resolve, reject) => {
    const query = encodeURIComponent(JSON.stringify({ _id: deviceId }));
    const path = `/devices?query=${query}`;

    http
      .get({ hostname: NBI_HOST, port: NBI_PORT, path }, (res) => {
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          try {
            const parsed = JSON.parse(data);
            resolve(parsed[0] || null);
          } catch (e) {
            reject(e);
          }
        });
      })
      .on("error", reject);
  });
}

function calcularScore(raw) {
  const rxPower = parseFloat(
    readPath(
      raw,
      "InternetGatewayDevice.WANDevice.1.X_GPON_InterfaceConfig.RXPower",
      "Device.Optical.Interface.1.OpticalSignalLevel"
    )
  );
  const uptime = parseInt(
    readPath(raw, "InternetGatewayDevice.DeviceInfo.UpTime", "Device.DeviceInfo.UpTime"),
    10
  );
  const statusConexao = readPath(
    raw,
    "InternetGatewayDevice.WANDevice.1.WANConnectionDevice.1.WANIPConnection.1.ConnectionStatus",
    "Device.IP.Interface.1.Status"
  );
  const wifiAtivo = readPath(
    raw,
    "InternetGatewayDevice.LANDevice.1.WLANConfiguration.1.Enable",
    "Device.WiFi.SSID.1.Enable"
  );

  let score = 100;
  if (!isNaN(rxPower) && (rxPower > -8 || rxPower < -27)) score -= 40;
  if (!isNaN(uptime) && uptime < 300) score -= 25;
  if (statusConexao && statusConexao !== "Connected") score -= 25;
  if (wifiAtivo === false || wifiAtivo === "false" || wifiAtivo === "0") score -= 10;
  score = Math.max(0, score);

  return { score, rxPower, uptime, statusConexao, wifiAtivo };
}

// --- Processamento de cada evento ---
async function processarEvento(deviceId, eventType, timestamp) {
  // 1. Sempre grava o evento bruto (histórico completo)
  await pool.query(
    "INSERT INTO device_events (device_id, event_type, payload) VALUES ($1, $2, $3)",
    [deviceId, eventType, JSON.stringify({ eventType, timestamp })]
  );

  // 2. Busca o estado atual do device na NBI pra calcular o score
  const raw = await fetchDeviceFromNBI(deviceId);
  if (!raw) {
    console.log(`Device ${deviceId} não encontrado na NBI, pulando cálculo de score.`);
    return;
  }

  const { score, statusConexao } = calcularScore(raw);
  const conectividadeOk = statusConexao === "Connected";
  const degradacao = score < 60;

  // 3. Atualiza o funil de sucesso no Postgres (upsert)
  await pool.query(
    `INSERT INTO device_lifecycle (
        device_id, instalacao_concluida_em, provisionado_em,
        conectividade_ok_em, ultimo_score_qualidade,
        ultima_verificacao_em, degradacao_detectada, atualizado_em
     ) VALUES ($1, now(), now(), $2, $3, now(), $4, now())
     ON CONFLICT (device_id) DO UPDATE SET
        conectividade_ok_em = CASE
            WHEN $2 THEN COALESCE(device_lifecycle.conectividade_ok_em, now())
            ELSE device_lifecycle.conectividade_ok_em
        END,
        ultimo_score_qualidade = $3,
        ultima_verificacao_em = now(),
        degradacao_detectada = $4,
        atualizado_em = now()`,
    [deviceId, conectividadeOk, score, degradacao]
  );

  // 4. Atualiza o snapshot rápido no Redis (último estado, sem histórico)
  await redis.set(
    `cpe:${deviceId}`,
    JSON.stringify({ score, statusConexao, degradacao, atualizado_em: new Date().toISOString() }),
    "EX",
    3600 // expira em 1h -- é cache, não histórico
  );

  console.log(`[OK] ${deviceId} -> score=${score} degradacao=${degradacao}`);
}

// --- Loop principal ---
async function main() {
  await consumer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const payload = JSON.parse(message.value.toString());
        await processarEvento(payload.deviceId, payload.event, payload.timestamp);
      } catch (err) {
        console.error("Erro ao processar mensagem:", err.message);
      }
    },
  });

  console.log(`Consumer rodando, escutando tópico "${KAFKA_TOPIC}"...`);
}

main().catch((err) => {
  console.error("Erro fatal no Consumer:", err);
  process.exit(1);
});