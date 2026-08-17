const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "genieacs-extension",
  brokers: ["kafka:9092"],
});

const producer = kafka.producer();
let isConnected = false;

async function ensureConnected() {
  if (!isConnected) {
    await producer.connect();
    isConnected = true;
  }
}

// Extension: publish_kafka
// Uso no provisioning script: ext("publish_kafka", deviceId)
module.exports = function (args, callback) {
  const deviceId = args[0];

  const payload = {
    deviceId: deviceId,
    event: "inform",
    timestamp: new Date().toISOString(),
  };

  ensureConnected()
    .then(() =>
      producer.send({
        topic: "device-events",
        messages: [{ key: deviceId, value: JSON.stringify(payload) }],
      })
    )
    .then(() => callback(null, [true]))
    .catch((err) => {
      // Não derruba a sessão TR-069 do CPE se o Kafka estiver fora do ar
      console.log("Erro ao publicar no Kafka:", err.message);
      callback(null, [false]);
    });
};