import os
import logging
from pathlib import Path
from collections import defaultdict
import numpy as np
import pandas as pd

from celery_app import app


# ---- Setup ----
SENSOR_TYPES = ['temperatura', 'umidade', 'pressao']
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# --- Phase 1: global statistics calculation ---
# Partial global stats
@app.task(name='tasks.calculate_partial_stats')
def calculate_partial_stats_task(file_path: str, start_row: int, end_row: int) -> tuple[dict, int]:
    """
    Reads a data chunk from the parquet and calculates partial statistics.
    """
    
    logger.info(f"Starting partial statistics calculation for rows: {start_row}-{end_row}.")

    # TODO: read only the necessary chunk
    df = pd.read_parquet(file_path)
    df_chunk = df.iloc[start_row:end_row]

    count = len(df_chunk)
    stats = {}
    for sensor_type in SENSOR_TYPES:
        sum_ = df_chunk[sensor_type].sum()
        sum_squares = (df_chunk[sensor_type] ** 2).sum()
        stats[sensor_type] = (sum_, sum_squares)
    
    logger.info(f"Partial calculation successfull for {count} rows.")
    return (stats, count)


# --- Phase 2: calculating meteorological metrics ---
@app.task(bind=True, name='tasks.calculate_metrics')
def calculate_metrics_task(self, file_path: str, station_ids_chunk: list, global_stats: dict, temp_dir: str) -> dict:
    """
    Processes a subset of the stations for calculating metrics 1 and 3 and
    saves the non-anomalous data in a temporary parquet file.
    """
    task_id = self.request.id
    logger.info(f"[Task {task_id}] Starting metrics calculation for {len(station_ids_chunk)} stations.")

    # TODO: reading only necessary data
    main_df = pd.read_parquet(file_path)
    main_df['timestamp'] = pd.to_datetime(main_df['timestamp'])
    df_chunk = main_df[main_df['id_estacao'].isin(station_ids_chunk)]

    batch_results = []
    clean_data_frames = []

    for station_id, station_df_raw in df_chunk.groupby('id_estacao'):
        station_df = station_df_raw.copy().sort_values(by='timestamp')

        # Detecting anomalies
        for sensor_type in SENSOR_TYPES:
            mean = global_stats[sensor_type]['mean']
            std = global_stats[sensor_type]['std']
            is_anomaly = (station_df[sensor_type] - mean).abs() > 3 * std
            station_df[f'is_anomaly_{sensor_type}'] = is_anomaly

        # Metric 1
        anomaly_pct = {s: station_df[f'is_anomaly_{s}'].mean() for s in SENSOR_TYPES}
        
        # Metric 3
        any_anomaly = station_df[[f'is_anomaly_{s}' for s in SENSOR_TYPES]].any(axis=1)
        non_anomalous_df = station_df.loc[~any_anomaly, ['timestamp', 'regiao'] + SENSOR_TYPES]
        clean_data_frames.append(non_anomalous_df)

        had_anomalies = station_df.rolling('10min', on='timestamp')[[f'is_anomaly_{s}' for s in SENSOR_TYPES]].max().astype(bool)
        distinct_anomalies_in_window = had_anomalies.sum(axis=1)
        is_incident_window = distinct_anomalies_in_window > 1
        previous_state = is_incident_window.shift(1, fill_value=False)
        period_starts = is_incident_window & ~previous_state
        metric3_result = period_starts.sum()

        batch_results.append({
            'station_id': station_id,
            'region': station_df['regiao'].iloc[0],
            'metric1_anomaly_pct': anomaly_pct,
            'metric3_distinct_anomaly_periods': metric3_result
        })

    # Aggregating and saving the cleaned data in a temporary parquet file
    full_clean_df_chunk = pd.concat(clean_data_frames, ignore_index=True) if clean_data_frames else pd.DataFrame()
    
    # Creating unique filepath
    temp_file_path = Path(temp_dir) / f"clean_data_{task_id}.parquet"
    if not full_clean_df_chunk.empty:
        full_clean_df_chunk.to_parquet(temp_file_path)
        logger.info(f"[Task {task_id}] Cleaned data saved to: {temp_file_path}")
    else:
        logger.info(f"[Task {task_id}] None clean data from this chunk")
        # Criamos um arquivo vazio para simplificar a lógica do redutor
        pd.DataFrame().to_parquet(temp_file_path)

    return {
        'metrics_1_and_3': batch_results,
        'clean_data_path': str(temp_file_path)
    }