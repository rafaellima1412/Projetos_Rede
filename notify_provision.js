// Pega o ID do device e chama a extension publish_kafka
const id = declare("DeviceID.ID", { value: Date.now() }).value[0];
ext("publish_kafka", id);