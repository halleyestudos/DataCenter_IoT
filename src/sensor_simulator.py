"""Simulador de sensores ambientais com ruído, outliers e envio MQTT."""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import numpy as np
import paho.mqtt.client as mqtt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

MQTT_TOPIC = "datacenter/ambiente/sala_servidores"
DEFAULT_SENSOR_ID = "SENSOR-001"

# Probabilidades de artefatos artificiais nos dados simulados.
MISSING_DATA_PROB = float(os.getenv("MISSING_DATA_PROB", "0.08"))
OUTLIER_PROB = float(os.getenv("OUTLIER_PROB", "0.05"))


class SensorSimulator:
    """Gera leituras ambientais realistas e publica via MQTT."""

    def __init__(self) -> None:
        self.broker_host = os.getenv("MQTT_BROKER_HOST", "localhost")
        self.broker_port = int(os.getenv("MQTT_BROKER_PORT", "1883"))
        self.interval_seconds = float(os.getenv("SIMULATOR_INTERVAL_SECONDS", "60"))
        self.sensor_id = os.getenv("SENSOR_ID", DEFAULT_SENSOR_ID)
        self._running = True

        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"sensor-simulator-{self.sensor_id}",
        )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

    @staticmethod
    def _on_connect(client: mqtt.Client, userdata, flags, reason_code, properties) -> None:
        if reason_code == 0:
            logger.info("Conectado ao broker MQTT.")
        else:
            logger.error("Falha na conexão MQTT. Código: %s", reason_code)

    @staticmethod
    def _on_disconnect(client: mqtt.Client, userdata, flags, reason_code, properties) -> None:
        logger.warning("Desconectado do broker MQTT. Código: %s", reason_code)

    def _generate_base_readings(self) -> dict[str, float]:
        """Gera valores base dentro das faixas operacionais do datacenter."""
        return {
            "temperatura": float(np.random.uniform(18.0, 35.0)),
            "umidade": float(np.random.uniform(30.0, 80.0)),
            "consumo": float(np.random.uniform(100.0, 5000.0)),
            "fumaca": float(np.random.uniform(0.0, 100.0)),
        }

    def _apply_noise(self, readings: dict[str, float]) -> dict[str, float | None]:
        """Adiciona ruído gaussiano às leituras."""
        noisy = {}
        noise_scale = {
            "temperatura": 0.8,
            "umidade": 2.0,
            "consumo": 120.0,
            "fumaca": 3.0,
        }
        for key, value in readings.items():
            noisy[key] = round(value + np.random.normal(0, noise_scale[key]), 2)
        return noisy

    def _apply_missing_data(self, readings: dict[str, float | None]) -> dict[str, float | None]:
        """Simula leituras ausentes ocasionais (None)."""
        result = readings.copy()
        for key in result:
            if np.random.random() < MISSING_DATA_PROB:
                logger.debug("Leitura ausente simulada para campo: %s", key)
                result[key] = None
        return result

    def _apply_outlier(self, readings: dict[str, float | None]) -> dict[str, float | None]:
        """Injeta picos anormais de temperatura (> 45°C)."""
        result = readings.copy()
        if result.get("temperatura") is not None and np.random.random() < OUTLIER_PROB:
            outlier_temp = float(np.random.uniform(45.5, 52.0))
            logger.warning("Outlier de temperatura simulado: %.2f°C", outlier_temp)
            result["temperatura"] = round(outlier_temp, 2)
        return result

    def generate_payload(self) -> dict:
        """Pipeline completo de geração de uma leitura simulada."""
        base = self._generate_base_readings()
        noisy = self._apply_noise(base)
        with_missing = self._apply_missing_data(noisy)
        final = self._apply_outlier(with_missing)

        payload = {
            "sensor_id": self.sensor_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **final,
        }
        return payload

    def connect(self) -> None:
        """Estabelece conexão com o broker MQTT."""
        logger.info(
            "Conectando ao broker %s:%s (intervalo=%ss)",
            self.broker_host,
            self.broker_port,
            self.interval_seconds,
        )
        self.client.connect(self.broker_host, self.broker_port, keepalive=60)
        self.client.loop_start()

    def publish_reading(self, payload: dict) -> None:
        """Publica leitura no tópico MQTT configurado."""
        message = json.dumps(payload, ensure_ascii=False)
        result = self.client.publish(MQTT_TOPIC, message, qos=1)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            logger.info("Publicado em '%s': %s", MQTT_TOPIC, message)
        else:
            logger.error("Erro ao publicar mensagem MQTT. Código: %s", result.rc)

    def run(self) -> None:
        """Loop principal de simulação."""
        self.connect()
        logger.info(
            "Simulador iniciado. Enviando dados a cada %.1f segundo(s).",
            self.interval_seconds,
        )

        try:
            while self._running:
                payload = self.generate_payload()
                self.publish_reading(payload)
                time.sleep(self.interval_seconds)
        except KeyboardInterrupt:
            logger.info("Interrupção recebida. Encerrando simulador...")
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Encerra conexões de forma limpa."""
        self._running = False
        self.client.loop_stop()
        self.client.disconnect()
        logger.info("Simulador encerrado.")


def _handle_signal(simulator: SensorSimulator, signum, frame) -> None:
    logger.info("Sinal %s recebido.", signum)
    simulator.shutdown()
    sys.exit(0)


def main() -> None:
    simulator = SensorSimulator()
    signal.signal(signal.SIGINT, lambda s, f: _handle_signal(simulator, s, f))
    signal.signal(signal.SIGTERM, lambda s, f: _handle_signal(simulator, s, f))
    simulator.run()


if __name__ == "__main__":
    main()
