"""Cliente de integração com NetBox para enriquecimento de tags."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

import requests

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeviceDetails:
    """Detalhes físicos de um dispositivo/sensor."""

    sensor_id: str
    site: str
    rack: str
    responsavel: str
    local: str = "sala_servidores"

    def as_influx_tags(self) -> dict[str, str]:
        """Converte os detalhes em tags compatíveis com InfluxDB."""
        return {
            "local": self.local,
            "rack": self.rack,
            "site": self.site,
            "responsavel": self.responsavel,
            "sensor_id": self.sensor_id,
        }


# Base de dados mockada para desenvolvimento e testes offline.
_MOCK_DEVICES: dict[str, dict[str, str]] = {
    "SENSOR-001": {
        "site": "SP-01",
        "rack": "B03",
        "responsavel": "Infra_Team",
        "local": "sala_servidores",
    },
    "SENSOR-002": {
        "site": "RJ-02",
        "rack": "A12",
        "responsavel": "Ops_NOC",
        "local": "sala_servidores",
    },
    "SENSOR-003": {
        "site": "BH-01",
        "rack": "C07",
        "responsavel": "Infra_Team",
        "local": "sala_servidores",
    },
}


class NetBoxClient:
    """Cliente para consulta de metadados de dispositivos no NetBox."""

    def __init__(
        self,
        base_url: str | None = None,
        token: str | None = None,
        use_mock: bool | None = None,
        timeout: int = 10,
    ) -> None:
        self.base_url = (base_url or os.getenv("NETBOX_URL", "")).rstrip("/")
        self.token = token or os.getenv("NETBOX_TOKEN", "")
        self.use_mock = use_mock if use_mock is not None else os.getenv("NETBOX_USE_MOCK", "true").lower() == "true"
        self.timeout = timeout

        if self.use_mock:
            logger.info("NetBoxClient iniciado em modo MOCK.")
        elif not self.base_url or not self.token:
            logger.warning(
                "NetBox URL/token não configurados. Fallback para modo MOCK."
            )
            self.use_mock = True

    def get_device_details(self, sensor_id: str) -> DeviceDetails:
        """
        Busca detalhes do dispositivo pelo ID do sensor.

        Em modo mock, retorna dados simulados. Caso contrário, consulta a API REST
        do NetBox (endpoint /api/dcim/devices/) filtrando por nome/custom field.
        """
        if self.use_mock:
            return self._get_mock_details(sensor_id)

        try:
            return self._fetch_from_api(sensor_id)
        except requests.RequestException as exc:
            logger.error(
                "Falha ao consultar NetBox para sensor %s: %s. Usando mock.",
                sensor_id,
                exc,
            )
            return self._get_mock_details(sensor_id)

    def _get_mock_details(self, sensor_id: str) -> DeviceDetails:
        """Retorna detalhes mockados com fallback genérico."""
        data = _MOCK_DEVICES.get(
            sensor_id,
            {
                "site": "SP-01",
                "rack": "RACK-01",
                "responsavel": "Infra_Team",
                "local": "sala_servidores",
            },
        )
        logger.debug("Detalhes mockados obtidos para %s: %s", sensor_id, data)
        return DeviceDetails(sensor_id=sensor_id, **data)

    def _fetch_from_api(self, sensor_id: str) -> DeviceDetails:
        """Consulta real à API NetBox (requer instância acessível)."""
        url = f"{self.base_url}/api/dcim/devices/"
        headers = {
            "Authorization": f"Token {self.token}",
            "Accept": "application/json",
        }
        params = {"name": sensor_id}

        logger.info("Consultando NetBox: %s (sensor=%s)", url, sensor_id)
        response = requests.get(
            url, headers=headers, params=params, timeout=self.timeout
        )
        response.raise_for_status()

        payload: dict[str, Any] = response.json()
        results = payload.get("results", [])

        if not results:
            logger.warning("Sensor %s não encontrado no NetBox. Usando mock.", sensor_id)
            return self._get_mock_details(sensor_id)

        device = results[0]
        site_name = device.get("site", {}).get("name", "DESCONHECIDO")
        rack_name = device.get("rack", {}).get("name", "RACK-01") if device.get("rack") else "RACK-01"
        responsavel = device.get("custom_fields", {}).get("responsavel", "Infra_Team")

        details = DeviceDetails(
            sensor_id=sensor_id,
            site=site_name,
            rack=rack_name,
            responsavel=str(responsavel),
            local="sala_servidores",
        )
        logger.info("Detalhes NetBox obtidos para %s: site=%s rack=%s", sensor_id, site_name, rack_name)
        return details
