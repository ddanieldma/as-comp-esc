import pandas as pd
import numpy as np

import random
from datetime import datetime, timedelta
from pathlib import Path
import logging
import json


# ---- Setup ---
# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Paths setup
DATA_DIR = Path().resolve() / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)
DATA_PATH = DATA_DIR / "metereology_data.parquet"
GROUND_TRUTH_PATH = DATA_DIR / "ground_truth_anomalies.parquet"

# Reproducibility
random.seed(42)

# Defining regions
REGIONS = [
    'Norte',
    'Nordeste',
    'Sudeste',
    'Sul',
    'Centro-oeste',
]

# -- Defining stations --
NUM_STATIONS = 200
# 'station': 'station's region'
stations = {}
# Distributing stations across regions.
for i in range(1, NUM_STATIONS + 1):
    station_region = random.choice(REGIONS)
    stations[i] = station_region

# -- Distributions --
# Distributtions parameters for generating the data correctly
NORMAL_STATS = {
    'temperatura': {'mean': 22.0, 'std': 4.0},
    'umidade': {'mean': 60.0, 'std': 10.0},
    'pressao': {'mean': 1015.0, 'std': 5.0}
}
SENSOR_TYPES = list(NORMAL_STATS.keys())
ANOMALY_THRESHOLD = 3.0 # 3 times std


# --- Generating the data ---
def generate_synthetic_data(
    num_samples: int = 5000,
    anomaly_percentage: float = 0.05,
    data_filename: str | Path = DATA_PATH,
    ground_truth_anomalies_filename: str | Path = GROUND_TRUTH_PATH
):
    if not isinstance(num_samples, int) or num_samples <= 0:
        raise ValueError(f"'num_samples' must be a positive non-zero integer, got: {type(num_samples)},{num_samples}")
    if not isinstance(anomaly_percentage, float) or anomaly_percentage <= 0 or anomaly_percentage >= 1:
        raise ValueError(f"'anomaly_percentage' must be a float between 0 and 1, got: {type(anomaly_percentage)},{anomaly_percentage}")
    
    logger.info(f"Iniciando a geração de {num_samples} amostras dividas em 24h, com {anomaly_percentage * 100}% de anomalias...")

    data = []
    anomaly_log = []

    # Defining start of time and time increment between each measure
    start_time = datetime.now() - timedelta(days=1)
    total_duration = timedelta(hours=24)
    time_increment = total_duration / num_samples

    stations_ids = list(stations.keys())

    for i in range(num_samples):
        timestamp = start_time + i * time_increment

        station_id = random.choice(stations_ids)
        region = stations[station_id]

        temp_hum_pres = {}
        for measure_type in SENSOR_TYPES:
            measure_raw = np.random.normal(loc=NORMAL_STATS[measure_type]['mean'], scale=NORMAL_STATS[measure_type]['std'])
            measure = np.clip(
                measure_raw,
                NORMAL_STATS[measure_type]['mean'] - ANOMALY_THRESHOLD * NORMAL_STATS[measure_type]['std'],
                NORMAL_STATS[measure_type]['mean'] + ANOMALY_THRESHOLD * NORMAL_STATS[measure_type]['std']
            )

            temp_hum_pres[measure_type] = measure

        is_anomaly = random.random() < (anomaly_percentage)

        if is_anomaly:
            sensor_to_alter = random.choice(SENSOR_TYPES)

            # Creates deviation of 3 to 5 stds
            offset = (ANOMALY_THRESHOLD + random.uniform(0, 2)) * NORMAL_STATS[sensor_to_alter]['std']
            temp_hum_pres[sensor_to_alter] += random.choice([-1, 1]) * offset

            anomaly_log.append({
                'timestamp': timestamp, 'id_estacao': station_id, 'sensor_anomalo': sensor_to_alter
            })
        
        # Rounding values and clipping humidity
        for measure_type, measure in temp_hum_pres.items():
            if measure_type == 'umidade':
                measure = np.clip(measure, 0, 100)
            temp_hum_pres[measure_type] = round(float(measure), 2)

        data.append({
            'timestamp': timestamp,
            'id_estacao': station_id,
            'regiao': region,
            **temp_hum_pres
        })

    df_data = pd.DataFrame(data)
    df_data['timestamp'] = df_data['timestamp'].astype('datetime64[us]')
    data_path = DATA_DIR / f'{data_filename}'
    df_data.to_parquet(data_path, index=False)
    logger.info(f"Dados meteorológicos salvos em '{str(data_path)}'")

    if anomaly_log:
        ground_truth_path = DATA_DIR / f'{ground_truth_anomalies_filename}'
        df_truth = pd.DataFrame(anomaly_log)
        df_truth['timestamp'] = df_truth['timestamp'].astype('datetime64[us]')
        df_truth.to_parquet(ground_truth_path, index=False)
        logger.info(f"Gabarito de anomalias salvo em '{str(ground_truth_path)}'")


if __name__ == "__main__":
    generate_synthetic_data(500_000)