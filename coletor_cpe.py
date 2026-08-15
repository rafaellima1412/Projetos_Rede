import json
import sys
from typing import Any, Dict, Optional
import requests

# --- Configurações ---
GENIEACS_NBI = "http://localhost:7557"
DEVICE_ID = "202BC1-BM632w-000000"  # Formato: <OUI>-<ProductClass>-<SerialNumber>


def get_param_value(device_data: dict, path: str) -> Optional[Any]:
    """Navega recursivamente pelo JSON do GenieACS e extrai o '_value' do parâmetro informado.

    Funciona tanto para rotas separadas por ponto (ex:
    'InternetGatewayDevice.DeviceInfo.SoftwareVersion')
    """
    keys = path.split(".")
    current = device_data

    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None

    if isinstance(current, dict) and "_value" in current:
        return current["_value"]

    return None


def fetch_device_raw(device_id: str) -> Optional[dict]:
    """Busca a árvore completa do dispositivo no banco do GenieACS."""
    url = f"{GENIEACS_NBI}/devices"
    query = json.dumps({"_id": device_id})

    try:
        response = requests.get(url, params={"query": query}, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data[0] if data else None
    except Exception as e:
        print(f"[!] Erro ao conectar na NBI do GenieACS: {e}")
        return None


def collect_device_info(raw_data: dict) -> dict:
    """Extrai e consolida os dados essenciais da CPE em um dicionário limpo."""

    # Tenta extrair do TR-098 primeiro, se não achar, tenta TR-181
    def read_path(tr098_path: str, tr181_path: str) -> str:
        val = get_param_value(raw_data, tr098_path)
        if val is None:
            val = get_param_value(raw_data, tr181_path)
        return str(val) if val is not None else "N/A"

    info = {
        "identificacao": {
            "id": raw_data.get("_id", "N/A"),
            "fabricante": read_path(
                "InternetGatewayDevice.DeviceInfo.Manufacturer",
                "Device.DeviceInfo.Manufacturer",

            ),
            "modelo": read_path(
                "InternetGatewayDevice.DeviceInfo.ProductClass",
                "Device.DeviceInfo.ModelName",
            ),
            "numero_serie": read_path(
                "InternetGatewayDevice.DeviceInfo.SerialNumber",
                "Device.DeviceInfo.SerialNumber",
            ),
            "versao_firmware": read_path(
                "InternetGatewayDevice.DeviceInfo.SoftwareVersion",
                "Device.DeviceInfo.SoftwareVersion",
            ),
            "uptime_segundos": read_path(
                "InternetGatewayDevice.DeviceInfo.UpTime",
                "Device.DeviceInfo.UpTime",
            ),
        },
        "rede_wan": {
            "ip_externo": read_path(
                "InternetGatewayDevice.WANDevice.1.WANConnectionDevice.1.WANIPConnection.1.ExternalIPAddress",
                "Device.IP.Interface.1.IPv4Address.1.IPAddress",
            ),
            "status_conexao": read_path(
                "InternetGatewayDevice.WANDevice.1.WANConnectionDevice.1.WANIPConnection.1.ConnectionStatus",
                "Device.IP.Interface.1.Status",
            ),
            "connection_request_url": read_path(
                "InternetGatewayDevice.ManagementServer.ConnectionRequestURL",
                "Device.ManagementServer.ConnectionRequestURL",
            ),
        },
        "wifi_2g": {
            "ssid": read_path(
                "InternetGatewayDevice.LANDevice.1.WLANConfiguration.1.SSID",
                "Device.WiFi.SSID.1.SSID",
            ),
            "ativo": read_path(
                "InternetGatewayDevice.LANDevice.1.WLANConfiguration.1.Enable",
                "Device.WiFi.SSID.1.Enable",
            ),
        },
        "sinal_optico": {
            "rx_power_dbm": read_path(
                "InternetGatewayDevice.WANDevice.1.X_GPON_InterfaceConfig.RXPower",
                "Device.Optical.Interface.1.OpticalSignalLevel",
            ),
            "tx_power_dbm": read_path(
                "InternetGatewayDevice.WANDevice.1.X_GPON_InterfaceConfig.TXPower",
                "Device.Optical.Interface.1.TransmitOpticalLevel",
            ),
        },
    }

    return info


def main():
    print(f"[*] Coletando dados do dispositivo: {DEVICE_ID} ...\n")

    raw_data = fetch_device_raw(DEVICE_ID)

    if not raw_data:
        print(f"[X] Dispositivo '{DEVICE_ID}' não foi encontrado no GenieACS.")
        sys.exit(1)

    # Extrai e organiza os dados
    relatorio = collect_device_info(raw_data)

    # Imprime o relatório formatado em JSON
    print(json.dumps(relatorio, indent=2, ensure_ascii=False))

    # Opcional: Salvar em arquivo local
    nome_arquivo = f"dados_{DEVICE_ID.replace('/', '_')}.json"
    with open(nome_arquivo, "w", encoding="utf-8") as f:
        json.dump(relatorio, f, indent=2, ensure_ascii=False)
    print(f"\n[✓] Relatório salvo com sucesso em: {nome_arquivo}")
    
    


if __name__ == "__main__":
    main()