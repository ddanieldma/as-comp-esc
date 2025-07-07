import numpy as np
import pandas as pd
from pandas.core.groupby.generic import DataFrameGroupBy

from multiprocessing import pool as mp
from multiprocessing import shared_memory
from collections import defaultdict
from pathlib import Path
import logging
import os
import time
import argparse  # 1. Import argparse

from generating_data import SENSOR_TYPES, generate_synthetic_data, DATA_PATH


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


# ---- Utility functions for Shared Memory ----
def setup_shared_memory(df: pd.DataFrame) -> dict:
    """
    Deconstructs a DataFrame and moves its columns into shared memory blocks.
    """
    shm_info = {'columns': {}, 'index_name': df.index.name}
    
    # Store the index in shared memory
    shm_index = shared_memory.SharedMemory(create=True, size=df.index.values.nbytes)
    shm_index_np = np.ndarray(df.index.shape, dtype=df.index.dtype, buffer=shm_index.buf)
    shm_index_np[:] = df.index.values[:]
    shm_info['index'] = {'name': shm_index.name, 'dtype': df.index.dtype, 'shape': df.index.shape}

    # Store each column in shared memory
    for col_name, col_series in df.items():
        is_categorical = isinstance(col_series.dtype, pd.CategoricalDtype)
        
        # Determine the actual data and dtype to store
        if is_categorical:
            data_to_store = col_series.cat.codes.values
            dtype_to_store = col_series.cat.codes.dtype
        else:
            data_to_store = col_series.values
            dtype_to_store = col_series.dtype

        # Create the shared memory block and copy data
        shm_col = shared_memory.SharedMemory(create=True, size=data_to_store.nbytes)
        shm_col_np = np.ndarray(data_to_store.shape, dtype=dtype_to_store, buffer=shm_col.buf)
        shm_col_np[:] = data_to_store[:]
        
        # Store the metadata needed for reconstruction
        col_info = {
            'name': shm_col.name,
            'dtype': dtype_to_store,
            'shape': data_to_store.shape,
            'is_categorical': is_categorical,
            'categories': col_series.cat.categories if is_categorical else None
        }
        shm_info['columns'][col_name] = col_info

    return shm_info

def cleanup_shared_memory(shm_info: dict):
    """
    Attaches to and unlinks all shared memory blocks based on the info dict.
    """
    logger.info("Cleaning up shared memory blocks...")
    # Clean up index
    try:
        shm = shared_memory.SharedMemory(name=shm_info['index']['name'])
        shm.close()
        shm.unlink()
    except FileNotFoundError:
        pass # Already cleaned up

    # Clean up columns
    for col_name, info in shm_info['columns'].items():
        try:
            shm = shared_memory.SharedMemory(name=info['name'])
            shm.close()
            shm.unlink()
        except FileNotFoundError:
            pass # Already cleaned up

def reconstruct_df_from_shm(shm_info: dict, indices: np.ndarray = None) -> pd.DataFrame:
    """
    Reconstructs a full or partial DataFrame in a worker process from shared memory info.
    """
    reconstructed_cols = {}
    for col_name, info in shm_info['columns'].items():
        existing_shm = shared_memory.SharedMemory(name=info['name'])
        col_array = np.ndarray(info['shape'], dtype=info['dtype'], buffer=existing_shm.buf)
        
        if indices is not None:
            col_array = col_array[indices]

        # Use the 'is_categorical' flag to correctly rebuild the column
        if info.get('is_categorical'):
            reconstructed_cols[col_name] = pd.Categorical.from_codes(
                codes=col_array, categories=info['categories']
            )
        else:
            reconstructed_cols[col_name] = col_array

    # Reconstruct index from shared memory
    existing_shm_index = shared_memory.SharedMemory(name=shm_info['index']['name'])
    index_array = np.ndarray(shm_info['index']['shape'], dtype=shm_info['index']['dtype'], buffer=existing_shm_index.buf)
    if indices is not None:
        index_array = index_array[indices]
    
    return pd.DataFrame(reconstructed_cols, index=pd.Index(index_array, name=shm_info['index']['name']))


# --- Phase 1: global statistics calculation ---
def calculate_partial_stats_worker(args : tuple[np.ndarray, dict]):
    """
    Worker function for the first part of phase 1. The worker processes 
    it's data chunk for the partial results: sum and sum of squares.
    """
    
    indices_chunk, shm_info = args

    # Reconstructing only the necessary part of the dataframe from shared memory
    df_chunk = reconstruct_df_from_shm(shm_info, indices=indices_chunk)

    logger.info(f"{30 * "="}\n[Worker {os.getpid()}] Calculating partial stats on {len(df_chunk)} rows...")

    count = len(df_chunk)
    stats = {}
    for sensor_type in SENSOR_TYPES:
        sum_ = df_chunk[sensor_type].sum()
        sum_squares = (df_chunk[sensor_type] ** 2).sum()
        stats[sensor_type] = (sum_, sum_squares)
    
    return (stats, count)

def calculate_global_stats(total_rows: int, shm_info: dict, pool: mp.Pool) -> dict:
    """
    Orchestrates Phase 1.
    """

    logger.info(f"{60 * "="}\nInitializing global stats calculations...")
    num_processes = pool._processes
    
    # Chunking the indices of the dataframe
    index_chunks = np.array_split(np.arange(total_rows), num_processes)

    # Preparing arguments for workers
    workers_args = [(chunk, shm_info) for chunk in index_chunks]

    logger.info(f"Distributing chunks to {num_processes} workers...")
    # Mapping: sending dataframe chunks for the works
    partial_results = pool.map(calculate_partial_stats_worker, workers_args)

    logger.info(f"Executing reduce phase")
    # Reducing
    total_stats = defaultdict(lambda: {'sum_': 0.0, 'sum_squares': 0.0})
    total_count = 0

    # Iterating through
    for stats, count in partial_results:
        total_count += count
        for sensor_type, (sum_, sum_squares) in stats.items():
            total_stats[sensor_type]['sum_'] += sum_
            total_stats[sensor_type]['sum_squares'] += sum_squares

    global_stats = {}
    for sensor_type, values in total_stats.items():
        mean = values['sum_'] / total_count
        std = np.sqrt((values['sum_squares'] / total_count) - mean**2)
        global_stats[sensor_type] = {
            'mean': mean,
            'std': std,
        }

    logger.info(f"Global stats calculated succesfully!")
    return global_stats


# --- Phase 2: calculating meteorological metrics ---
def worker_meteorological_metrics(args: tuple[np.ndarray, dict, dict]):
    """
    Worker function for the first part of phase 2. The worker calculates
    the metrics for its subset of stations.
    """

    stations_ids_chunk: np.ndarray
    shm_info: dict
    global_stats: dict[str, dict[str, float]]

    stations_ids_chunk, shm_info, global_stats = args

    # Current worker results
    batch_results = []

    # Saving the non_anomaly df for stations in this batch
    clean_data_frames = []

    for station_id in stations_ids_chunk:
        station_df: pd.DataFrame

        # Reconstructing only necessary part of the dataframe from the shared memory
        station_df = reconstruct_df_from_shm(shm_info, indices=station_id)
        station_df = station_df.sort_values(by='timestamp') # Sorting is still needed
        station_id = station_df['id_estacao'].iloc[0]

        # Get current station data from the GroupBy object and sorting
        station_df = main_df.loc[main_df['id_estacao'] == station_id].copy().sort_values(by='timestamp')
        # station_df = grouped_by_station.get_group(station_id).copy().sort_values(by='timestamp')

        # Tagging anomalies
        is_anomaly_map = {}
        for sensor_type in SENSOR_TYPES:
            mean = global_stats[sensor_type]['mean']
            std = global_stats[sensor_type]['std']
            # Detecting anomalies
            is_anomaly = (station_df[sensor_type] - mean).abs() > 3 * std
            # Persisting anomalies found
            station_df[f'is_anomaly_{sensor_type}'] = is_anomaly

        # - Calculating metric 1 -
        anomaly_pct = {s: station_df[f'is_anomaly_{s}'].mean() for s in SENSOR_TYPES}
        
        # - Calculating metric 3 -
        # Getting all anomalies
        any_anomaly = station_df['is_anomaly_temperatura'] | station_df['is_anomaly_umidade'] | station_df['is_anomaly_pressao']
        non_anomalous_df = station_df.loc[~any_anomaly, ['timestamp', 'regiao', 'temperatura', 'umidade', 'pressao']]
        clean_data_frames.append(non_anomalous_df)

        # For each sensor, cheking any anomalies in the sliding window of 10min
        had_temp_anomaly = station_df.rolling('10min', on='timestamp')['is_anomaly_temperatura'].max().astype(bool)
        had_hum_anomaly = station_df.rolling('10min', on='timestamp')['is_anomaly_umidade'].max().astype(bool)
        had_press_anomaly = station_df.rolling('10min', on='timestamp')['is_anomaly_pressao'].max().astype(bool)
        
        # Identifyin moments where there are 2 anomalies in the 10min sliding window.
        # Summing the booleans in the sliding window means that any value > 1
        # (more than 1 sensor had anomalies in the window) is our desired event.
        distinct_anomalies_in_window = had_temp_anomaly.astype(int) + had_hum_anomaly.astype(int) + had_press_anomaly.astype(int)

        is_incident_window = distinct_anomalies_in_window > 1

        # For avoiding counting the same periodo two times, we count only
        # the transtiion between False to True in 'is_incident_window'
        previous_state = is_incident_window.shift(1, fill_value=False)
        period_starts = is_incident_window & ~previous_state

        # Final metric 3 is the sum of all period_start
        metric3_result = period_starts.sum()

        # Adding to final result
        batch_results.append({
            'station_id': station_id,
            'region': station_df['regiao'].iloc[0],
            'metric1_anomaly_pct': anomaly_pct,
            'metric3_distinct_anomaly_periods': metric3_result
        })

    return (batch_results, clean_data_frames)

def calculate_final_metrics(main_df: pd.DataFrame, shm_info:dict, global_stats: dict, pool: mp.Pool) -> tuple[list[dict], pd.DataFrame]:
    """
    Orchestrates Phase 2. Returns metrics 1 and 3 results and a big df 
    with all non-anomalous data
    """

    logger.info(f"{60 * "="}\nStarting metrics calculation...")

    num_processes = pool._processes
    
    station_indices_map = main_df.groupby('id_estacao').groups
    all_station_groups = list(station_indices_map.values())
    all_station_groups_array = np.array(all_station_groups, dtype=object)
    # Splitting across workers
    station_indices_chunks = np.array_split(all_station_groups_array, num_processes)

    logger.info(f"Distribuitin data across {num_processes} workers...")
    workers_args = [(chunk, shm_info, global_stats) for chunk in station_indices_chunks]

    workers_outputs = pool.map(worker_meteorological_metrics, workers_args)
    
    logger.info(f"Executing reducing phase...")
    # Separating calculated metrics
    final_metrics_list = []
    all_clean_data_frames = []
    
    for metrics, dfs in workers_outputs:
        final_metrics_list.extend(metrics)
        all_clean_data_frames.extend(dfs)

    # Concatenating all the cleaned dataframe
    full_clean_df = pd.concat(all_clean_data_frames, ignore_index=True)

    logger.info(f"Final metrics calculated succesfully!")
    return final_metrics_list, full_clean_df

def run_full_pipeline(main_df: pd.DataFrame, degree_of_parallelism: int):
    """
    Executes the whole pipeline.
    """

    # Initializing to ensure it's in the scope
    shm_info = None

    # Putting main df in shared memory
    logger.info("Setting up shared memory for the main DataFrame...")
    # Converting string columns to categorical
    main_df['regiao'] = main_df['regiao'].astype('category')
    main_df['timestamp'] = pd.to_datetime(main_df['timestamp'])
    # Reset index to easily slice by integer location (iloc)
    main_df.reset_index(drop=True, inplace=True)
    shm_info = setup_shared_memory(main_df)

    final_results = {}

    logger.info(f"{60 * "="}\nCreating worker pool with {degree_of_parallelism} workers")
    with mp.Pool(processes=degree_of_parallelism) as pool:
        # Phase 1: global stats
        global_stats = calculate_global_stats(len(main_df), shm_info, pool)
        
        # Phase 2: returns partial results and cleaned df
        metrics_1_and_3_list, clean_df = calculate_final_metrics(
            main_df,
            shm_info,
            global_stats,
            pool
        )

    # Phase 3: final aggregation
    # Transform the results of metrics 1 and 3 into a df
    results_df_m1_m3 = pd.DataFrame(metrics_1_and_3_list)
    final_results['metrics_per_station'] = results_df_m1_m3

    # Calcuting metric 2
    logger.info(f"Aggregating metric 2 results")
    if not clean_df.empty:
        metric2_series_by_region = (
            clean_df.set_index('timestamp') \
            .groupby('regiao')[['temperatura', 'umidade', 'pressao']] \
            .resample('h').mean() # 'h' for hourly
        ) 
        
        final_results['metric2_hourly_avg_by_region'] = metric2_series_by_region
    else:
        final_results['metric2_hourly_avg_by_region'] = pd.DataFrame() # Caso não haja dados limpos

    logger.info(f"{60 * "="}\nPipeline finished succesfully!")
        
    if shm_info:
        cleanup_shared_memory(shm_info)

    return final_results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the multiprocessing pipeline with a specific number of workers.")
    parser.add_argument(
        '--workers', 
        type=int, 
        default=os.cpu_count(),
        help=f'Number of worker processes to use. Defaults to the number of CPU cores ({os.cpu_count()}).'
    )
    args = parser.parse_args()

    num_workers = args.workers
    
    logger.info("Reading parquet data")
    main_df = pd.read_parquet(DATA_PATH)
    logger.info(60 * "=")
    logger.info(f"Data size: {len(main_df)}")
    logger.info(f"Running pipeline with {num_workers} worker(s)...")
    logger.info(60 * "=")

    start_time = time.perf_counter()
    # 4. Run the pipeline a single time with the specified number of workers
    final_results = run_full_pipeline(main_df.copy(), num_workers)
    elapsed_time = time.perf_counter() - start_time

    print(f"\n--- Final Results ---")
    print("\nMetrics per station (Metrics 1 & 3):")
    print(final_results['metrics_per_station'])
    print("\nHourly average by region (Metric 2):")
    print(final_results['metric2_hourly_avg_by_region'])
    
    print(f"\nTotal execution time with {num_workers} worker(s): {elapsed_time:.2f} seconds")