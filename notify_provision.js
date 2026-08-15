log("========== NOTIFY N8N ==========");

const deviceId = declare("DeviceID.ID", { value: Date.now() }).value[0];

log("Device ID: " + deviceId);

ext("notify_n8n", "notify_n8n", [deviceId]);

log("========== NOTIFY N8N FINAL ==========");