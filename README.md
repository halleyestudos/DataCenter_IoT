# Sistema IoT para Monitoramento Ambiental de Datacenter

Protótipo funcional de monitoramento ambiental em datacenter utilizando **Python**, **MQTT**, **InfluxDB** e **Grafana**, com enriquecimento opcional de metadados via **NetBox**.

## Arquitetura

```
┌─────────────────┐     MQTT      ┌──────────────────┐     Write API    ┌───────────┐
│ sensor_simulator│ ────────────► │  data_pipeline   │ ───────────────► │ InfluxDB  │
│  (publicador)   │  JSON/5s      │ (subscriber +    │  ambiente_ti     │           │
└─────────────────┘               │  pré-process.)   │                  └─────┬─────┘
                                  └────────┬─────────┘                        │
                                           │                                   │ Flux
                                           ▼                                   ▼
                                  ┌─────────────────┐                  ┌───────────┐
                                  │  netbox_client  │                  │  Grafana  │
                                  │  (tags extras)  │                  │ Dashboard │
                                  └─────────────────┘                  └───────────┘
```

## Estrutura do Projeto

```
├── src/
│   ├── sensor_simulator.py   # Simulador de sensores com ruído, outliers e MQTT
│   ├── data_pipeline.py      # Subscriber, Z-Score, média móvel e InfluxDB
│   └── netbox_client.py      # Enriquecimento de tags via API NetBox (mock/real)
├── config/
│   ├── mosquitto/mosquitto.conf
│   └── grafana/provisioning/datasources/influxdb.yml
├── docker-compose.yml        # Mosquitto + InfluxDB + Grafana
├── requirements.txt
├── .env.example
└── README.md
```

## Pré-requisitos

- [Docker](https://www.docker.com/) e Docker Compose
- Python 3.10+
- pip

## Instalação Rápida

### 1. Subir a infraestrutura

```bash
docker compose up -d
```

Serviços disponíveis:

| Serviço    | URL                         | Credenciais              |
|------------|-----------------------------|--------------------------|
| Mosquitto  | `localhost:1883`            | anônimo (dev)            |
| InfluxDB   | http://localhost:8086       | admin / adminpassword    |
| Grafana    | http://localhost:3000       | admin / admin            |

### 2. Instalar dependências Python

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/macOS
source .venv/bin/activate

pip install -r requirements.txt
```

### 3. Configurar variáveis de ambiente

```bash
copy .env.example .env   # Windows
# cp .env.example .env   # Linux/macOS
```

Para testes rápidos, mantenha `SIMULATOR_INTERVAL_SECONDS=5`. Em simulação de produção, use `60`.

### 4. Executar os componentes

Abra **dois terminais** na pasta `src/`:

**Terminal 1 — Pipeline de processamento:**
```bash
cd src
python data_pipeline.py
```

**Terminal 2 — Simulador de sensores:**
```bash
cd src
python sensor_simulator.py
```

## Componentes

### Simulador (`sensor_simulator.py`)

Publica leituras JSON a cada intervalo configurável no tópico `datacenter/ambiente/sala_servidores`.

| Variável       | Faixa          |
|----------------|----------------|
| Temperatura    | 18°C – 35°C    |
| Umidade        | 30% – 80%      |
| Consumo        | 100W – 5000W   |
| Fumaça         | 0 – 100 ppm    |

**Artefatos simulados:**
- Ruído gaussiano (`np.random.normal`)
- Leituras ausentes ocasionais (`null` no JSON)
- Outliers de temperatura (> 45°C)

Exemplo de payload:

```json
{
  "sensor_id": "SENSOR-001",
  "timestamp": "2026-06-11T14:30:00+00:00",
  "temperatura": 24.73,
  "umidade": 55.12,
  "consumo": 1842.5,
  "fumaca": 8.4
}
```

### Pipeline (`data_pipeline.py`)

1. **Assina** o tópico MQTT e mantém janela deslizante de 5 leituras (`pandas.DataFrame`)
2. **Interpola** valores faltantes (interpolação linear)
3. **Detecta outliers** via Z-Score (|z| > 2,5 por padrão)
4. **Filtra ruído** com média móvel de 5 períodos → `temperatura_filtrada`
5. **Enriquece** tags via `NetBoxClient`
6. **Persiste** no InfluxDB

**Measurement:** `ambiente_ti`

| Tipo   | Nome                  | Descrição                          |
|--------|-----------------------|------------------------------------|
| Tag    | `local`               | sala_servidores                    |
| Tag    | `rack`                | Ex: B03 (NetBox ou mock)           |
| Tag    | `site`                | Ex: SP-01                          |
| Tag    | `responsavel`         | Ex: Infra_Team                     |
| Tag    | `sensor_id`           | ID do sensor                       |
| Field  | `temperatura`         | Valor bruto (pós-interpolação)     |
| Field  | `temperatura_filtrada`| Média móvel                        |
| Field  | `umidade`             | Umidade relativa (%)               |
| Field  | `consumo`             | Consumo elétrico (W)               |
| Field  | `fumaca`              | Detecção de fumaça (ppm)           |
| Field  | `is_outlier`          | Boolean — outlier detectado        |

### NetBox (`netbox_client.py`)

Classe `NetBoxClient` com modo **mock** (padrão) e suporte a API real:

```python
from netbox_client import NetBoxClient

client = NetBoxClient(use_mock=True)
details = client.get_device_details("SENSOR-001")
print(details.as_influx_tags())
# {'local': 'sala_servidores', 'rack': 'B03', 'site': 'SP-01', ...}
```

Para usar API real, configure `NETBOX_USE_MOCK=false`, `NETBOX_URL` e `NETBOX_TOKEN`.

## Consultas no Grafana

Após subir a stack, acesse Grafana em http://localhost:3000. O datasource InfluxDB já vem provisionado.

**Exemplo de query Flux — temperatura filtrada:**

```flux
from(bucket: "datacenter")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "ambiente_ti")
  |> filter(fn: (r) => r._field == "temperatura_filtrada")
```

**Exemplo — outliers detectados:**

```flux
from(bucket: "datacenter")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "ambiente_ti")
  |> filter(fn: (r) => r._field == "is_outlier" and r._value == true)
```

## Variáveis de Ambiente

| Variável                    | Padrão                          | Descrição                        |
|-----------------------------|---------------------------------|----------------------------------|
| `MQTT_BROKER_HOST`          | localhost                       | Host do broker Mosquitto         |
| `MQTT_BROKER_PORT`          | 1883                            | Porta MQTT                       |
| `SIMULATOR_INTERVAL_SECONDS`| 60                              | Intervalo entre leituras (s)     |
| `SENSOR_ID`                 | SENSOR-001                      | ID do sensor simulado            |
| `INFLUXDB_URL`              | http://localhost:8086           | URL do InfluxDB                  |
| `INFLUXDB_TOKEN`            | my-super-secret-auth-token      | Token de escrita                 |
| `INFLUXDB_ORG`              | ufjf                            | Organização InfluxDB             |
| `INFLUXDB_BUCKET`           | datacenter                      | Bucket de destino                |
| `PIPELINE_WINDOW_SIZE`      | 5                               | Tamanho da janela em memória     |
| `Z_SCORE_THRESHOLD`         | 2.5                             | Limiar para detecção de outlier  |
| `NETBOX_USE_MOCK`           | true                            | Usar dados mockados do NetBox    |

## Encerramento

```bash
# Parar simulador e pipeline: Ctrl+C em cada terminal
docker compose down
```

## Licença

Projeto acadêmico — Pós-graduação UFJF.
