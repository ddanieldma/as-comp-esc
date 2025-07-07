import logging
import os
import shutil
import time
from pathlib import Path
from collections import defaultdict

import numpy as np
import pandas as pd
from celery import group

from celery_app.tasks import calculate_metrics_task, calculate_partial_stats_task
from generating_data import DATA_PATH, DATA_DIR


# ---- Setup ----
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s][CONTROLLER] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

TEMP_DIR = DATA_DIR / 'temp_results'
LOG_FILE_PATH = "mbw_execution_times.csv"

def setup_log_file():
    with open(LOG_FILE_PATH, "w") as f:
        f.write("degree_of_parallelism,execution_time\n")

def log_execution_time(degree, execution_time):
    with open(LOG_FILE_PATH, "a") as f:
        f.write(f"{degree},{execution_time}\n")


def run_celery_pipeline(degree_of_parallelism: int):
    """
    Orchestrates and exexutes the complet data processing pipeline using
    celery workers.
    """

    # Ensuring temp dir exists and its clean before beggining
    if TEMP_DIR.exists():
        shutil.rmtree(TEMP_DIR)
    TEMP_DIR.mkdir(parents=True)

    #  --- Handler 1: global statistics ---
    logger.info(f"{60 * '='}Starting handler 1...")
    logger.info(f"Using {degree_of_parallelism} workers")

    # - Map phase -
    logger.info("Mapping: splitting data and dispatching...")
    total_rows = pd.read_parquet(DATA_PATH, columns=['id_estacao']).shape[0]
    index_chunks = np.array_split(np.arange(total_rows), degree_of_parallelism)

    # Creates task group with the args
    map_stats_tasks = group(
        calculate_partial_stats_task.s(str(DATA_PATH), int(chunk[0]), int(chunk[-1] + 1))
        for chunk in index_chunks if chunk.size > 0
    )
    
    # Executes the task group
    result_group_stats = map_stats_tasks.apply_async()

    # - Reduce phase -
    logger.info("Reducing: Waiting and aggregating partial results...")
    partial_results = result_group_stats.get()

    # Partial results aggregation
    total_stats = defaultdict(lambda: {'sum_': 0.0, 'sum_squares': 0.0})
    total_count = 0
    for stats, count in partial_results:
        total_count += count
        for sensor_type, (sum_, sum_squares) in stats.items():
            total_stats[sensor_type]['sum_'] += sum_
            total_stats[sensor_type]['sum_squares'] += sum_squares

    # Final calculation of global statistics
    global_stats = {}
    for sensor_type, values in total_stats.items():
        mean = values['sum_'] / total_count
        std = np.sqrt((values['sum_squares'] / total_count) - mean**2)
        global_stats[sensor_type] = {'mean': mean, 'std': std}

    logger.info(f"Gloabal stats calculated: {global_stats}")


    # --- Handler 2 ----
    logger.info(f"{60 * '='}Starting Handler 2...")
        
    # --- Map phase ---
    logger.info("Mapping: Splitting stations and dispatching...")
    station_ids = pd.read_parquet(DATA_PATH, columns=['id_estacao'])['id_estacao'].unique()
    station_id_chunks = np.array_split(station_ids, degree_of_parallelism)
    
    map_metrics_tasks = group(
        calculate_metrics_task.s(str(DATA_PATH), chunk.tolist(), global_stats, str(TEMP_DIR))
        for chunk in station_id_chunks if chunk.size > 0
    )
    result_group_metrics = map_metrics_tasks.apply_async()

    # --- Reduce phase ---
    logger.info("Reducing: Waiting and aggregating metrics results...")
    workers_outputs = result_group_metrics.get()

    final_metrics_list = []
    clean_data_paths = []
    for output in workers_outputs:
        final_metrics_list.extend(output['metrics_1_and_3'])
        clean_data_paths.append(output['clean_data_path'])
        
    final_results = {}
    final_results['metrics_per_station'] = pd.DataFrame(final_metrics_list)


    # --- Phase 3: Final aggregation ---
    logger.info(f"{60 * '='}\nStarting phase 3: metric 2 final aggregation...")

    # Reading all temp files and concatenating them
    clean_dfs = [pd.read_parquet(path) for path in clean_data_paths if Path(path).exists and Path(path).stat().st_size > 0]

    if clean_dfs:
        full_clean_df = pd.concat(clean_dfs, ignore_index=True)

        # Final calculation for metric 2
        metric2_series_by_region = (
            full_clean_df.set_index('timestamp')
            .groupby('regiao')[['temperatura', 'umidade', 'pressao']]
            .resample('h').mean()
        )
        final_results['metric2_hourly_avg_by_region'] = metric2_series_by_region
    else:
            final_results['metric2_hourly_avg_by_region'] = pd.DataFrame()

    # Ensuring temp dir is removed at the end
    logger.info("Cleaning temp files...")
    if TEMP_DIR.exists():
        shutil.rmtree(TEMP_DIR)
    logger.info("Cleaning completed!")
    
    logger.info(f"Pipeline finished successfully!")
    return final_results

if __name__ == "__main__":
    logger.info("Setting up log file")
    setup_log_file()
    
    # SAME NUMBER AS WORKERS IN EXECUTION
    DEGREE_OF_PARALLELISM = int(os.environ.get('DEGREE_OF_PARALLELISM', 4))
    
    start_time = time.perf_counter()
    
    # Verifica se o arquivo de dados existe antes de iniciar
    if not DATA_PATH.exists():
        logger.error(f"Data file not found at '{DATA_PATH}'.")
        logger.error("Execute the data generation script first")
    else:
        results = run_celery_pipeline(DEGREE_OF_PARALLELISM)
        execution_time = time.perf_counter() - start_time

        logger.info("Writing execution time to log file...")
        log_execution_time(DEGREE_OF_PARALLELISM, execution_time)


        logger.info(f"{60 * '*'}")
        logger.info(f"Celery pipeline finished!")
        logger.info(f"Workers: {DEGREE_OF_PARALLELISM}")
        logger.info(f"Total execution time: {execution_time:.4f} seconds")
        logger.info(f"{60 * '*'}")

        # Exibe um resumo dos resultados
        if results:
            logger.info("\nResumo dos resultados (Métricas 1 e 3 por estação):")
            print(results['metrics_per_station'].head())
            logger.info("\nResumo dos resultados (Métrica 2 - Média horária por região):")
            print(results['metric2_hourly_avg_by_region'].head())