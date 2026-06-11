"""Pipeline MQTT → pré-processamento → InfluxDB."""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

import pandas as pd
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from netbox_client import NetBoxClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

MQTT_TOPIC = "datacenter/ambiente/sala_servidores"
MEASUREMENT = "ambiente_ti"
WINDOW_SIZE = int(os.getenv("PIPELINE_WINDOW_SIZE", "5"))
Z_SCORE_THRESHOLD = float(os.getenv("Z_SCORE_THRESHOLD", "2.5"))
ROLLING_WINDOW = int(os.getenv("ROLLING_WINDOW", "5"))


class DataPipeline:
    """Subscriber MQTT com janela deslizante, limpeza e persistência."""

    def __init__(self) -> None:
        self.broker_host = os.getenv("MQTT_BROKER_HOST", "localhost")
        self.broker_port = int(os.getenv("MQTT_BROKER_PORT", "1883"))

        self.influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
        self.influx_token = os.getenv("INFLUXDB_TOKEN", "my-super-secret-auth-token")
        self.influx_org = os.getenv("INFLUXDB_ORG", "ufjf")
        self.influx_bucket = os.getenv("INFLUXDB_BUCKET", "datacenter")

        self.netbox = NetBoxClient()
        self.window = pd.DataFrame(
            columns=["timestamp", "temperatura", "umidade", "consumo", "fumaca", "sensor_id"]
        )

        self.influx_client = InfluxDBClient(
            url=self.influx_url,
            token=self.influx_token,
            org=self.influx_org,
        )
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)

        self.mqtt_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id="datacenter-data-pipeline",
        )
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_message = self._on_message
        self.mqtt_client.on_disconnect = self._on_disconnect

    @staticmethod
    def _on_connect(client: mqtt.Client, userdata, flags, reason_code, properties) -> None:
        if reason_code == 0:
            client.subscribe(MQTT_TOPIC, qos=1)
            logger.info("Pipeline conectado e inscrito em '%s'.", MQTT_TOPIC)
        else:
            logger.error("Falha na conexão MQTT. Código: %s", reason_code)

    @staticmethod
    def _on_disconnect(client: mqtt.Client, userdata, flags, reason_code, properties) -> None:
        logger.warning("Pipeline desconectado do broker. Código: %s", reason_code)

    def _parse_message(self, payload_raw: str) -> dict | None:
        """Decodifica e valida payload JSON recebido via MQTT."""
        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError as exc:
            logger.error("JSON inválido recebido: %s", exc)
            return None

        required = ("sensor_id", "temperatura", "umidade", "consumo", "fumaca")
        for field in required:
            if field not in payload:
                logger.error("Campo obrigatório ausente no payload: %s", field)
                return None

        return payload

    def _append_to_window(self, payload: dict) -> None:
        """Adiciona leitura à janela deslizante em memória."""
        timestamp = payload.get("timestamp") or datetime.now(timezone.utc).isoformat()

        row = {
            "timestamp": timestamp,
            "temperatura": self._to_float_or_nan(payload.get("temperatura")),
            "umidade": self._to_float_or_nan(payload.get("umidade")),
            "consumo": self._to_float_or_nan(payload.get("consumo")),
            "fumaca": self._to_float_or_nan(payload.get("fumaca")),
            "sensor_id": payload.get("sensor_id", "SENSOR-001"),
        }

        self.window = pd.concat([self.window, pd.DataFrame([row])], ignore_index=True)
        if len(self.window) > WINDOW_SIZE:
            self.window = self.window.iloc[-WINDOW_SIZE:].reset_index(drop=True)

        logger.debug("Janela atual (%d registros):\n%s", len(self.window), self.window)

    @staticmethod
    def _to_float_or_nan(value) -> float:
        if value is None:
            return float("nan")
        try:
            return float(value)
        except (TypeError, ValueError):
            return float("nan")

    def _preprocess(self) -> dict:
        """
        Aplica interpolação linear, detecção de outliers (Z-Score) e
        média móvel sobre a janela atual.
        """
        if self.window.empty:
            return {}

        df = self.window.copy()

        # (a) Interpolação linear para dados faltantes.
        numeric_cols = ["temperatura", "umidade", "consumo", "fumaca"]
        df[numeric_cols] = df[numeric_cols].interpolate(method="linear", limit_direction="both")

        # (b) Z-Score para identificação de outliers na temperatura.
        temp_series = df["temperatura"]
        mean = temp_series.mean()
        std = temp_series.std(ddof=0)

        if std == 0 or pd.isna(std):
            df["z_score"] = 0.0
            df["is_outlier"] = False
        else:
            df["z_score"] = (temp_series - mean) / std
            df["is_outlier"] = df["z_score"].abs() > Z_SCORE_THRESHOLD

        # (c) Média móvel (rolling) para filtragem de ruído.
        df["temp_filtrada"] = temp_series.rolling(
            window=min(ROLLING_WINDOW, len(df)),
            min_periods=1,
        ).mean()

        latest = df.iloc[-1]
        logger.info(
            "Pré-processamento: temp=%.2f°C | temp_filtrada=%.2f°C | outlier=%s | z=%.2f",
            latest["temperatura"],
            latest["temp_filtrada"],
            bool(latest["is_outlier"]),
            latest["z_score"],
        )

        return {
            "temperatura": float(latest["temperatura"]),
            "temperatura_filtrada": float(latest["temp_filtrada"]),
            "umidade": float(latest["umidade"]),
            "consumo": float(latest["consumo"]),
            "fumaca": float(latest["fumaca"]),
            "is_outlier": bool(latest["is_outlier"]),
            "sensor_id": str(latest["sensor_id"]),
            "timestamp": str(latest["timestamp"]),
        }

    def _write_to_influx(self, processed: dict) -> None:
        """Persiste dados limpos e enriquecidos no InfluxDB."""
        sensor_id = processed["sensor_id"]
        device = self.netbox.get_device_details(sensor_id)
        tags = device.as_influx_tags()

        try:
            ts = datetime.fromisoformat(processed["timestamp"].replace("Z", "+00:00"))
        except ValueError:
            ts = datetime.now(timezone.utc)

        point = (
            Point(MEASUREMENT)
            .tag("local", tags["local"])
            .tag("rack", tags["rack"])
            .tag("site", tags["site"])
            .tag("responsavel", tags["responsavel"])
            .tag("sensor_id", tags["sensor_id"])
            .field("temperatura", processed["temperatura"])
            .field("temperatura_filtrada", processed["temperatura_filtrada"])
            .field("umidade", processed["umidade"])
            .field("consumo", processed["consumo"])
            .field("fumaca", processed["fumaca"])
            .field("is_outlier", processed["is_outlier"])
            .time(ts, WritePrecision.NS)
        )

        try:
            self.write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=point)
            logger.info(
                "Gravado no InfluxDB | measurement=%s | sensor=%s | rack=%s | site=%s",
                MEASUREMENT,
                sensor_id,
                tags["rack"],
                tags["site"],
            )
        except Exception as exc:
            logger.exception("Erro ao gravar no InfluxDB: %s", exc)

    def _on_message(self, client: mqtt.Client, userdata, msg: mqtt.MQTTMessage) -> None:
        """Callback principal: recebe, processa e persiste cada mensagem."""
        logger.debug("Mensagem recebida no tópico %s", msg.topic)
        payload = self._parse_message(msg.payload.decode("utf-8"))
        if payload is None:
            return

        self._append_to_window(payload)
        processed = self._preprocess()
        if processed:
            self._write_to_influx(processed)

    def run(self) -> None:
        """Inicia o subscriber MQTT e mantém o pipeline ativo."""
        logger.info(
            "Iniciando pipeline | broker=%s:%s | influx=%s | bucket=%s | janela=%d",
            self.broker_host,
            self.broker_port,
            self.influx_url,
            self.influx_bucket,
            WINDOW_SIZE,
        )
        self.mqtt_client.connect(self.broker_host, self.broker_port, keepalive=60)
        self.mqtt_client.loop_forever()

    def shutdown(self) -> None:
        """Encerra conexões MQTT e InfluxDB."""
        logger.info("Encerrando pipeline...")
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        self.write_api.close()
        self.influx_client.close()
        logger.info("Pipeline encerrado.")


def main() -> None:
    pipeline = DataPipeline()

    def _handle_signal(signum, frame) -> None:
        logger.info("Sinal %s recebido.", signum)
        pipeline.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        pipeline.run()
    except KeyboardInterrupt:
        pipeline.shutdown()


if __name__ == "__main__":
    main()
