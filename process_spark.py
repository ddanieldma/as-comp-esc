from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

DATA_PATH = 'data/metereology_data.parquet'

def main():
    # Initializing spark session
    spark = SparkSession.builder \
        .appName("WeatherMetricsExperiment") \
        .getOrCreate()
        # .config("spark.sql.parquet.int64AsTimestamp.enabled", "true") \

    df = spark.read.parquet(DATA_PATH)

    df.cache()

    # Calculating global metrics
    stats = df.agg(
        F.avg("temperatura").alias("mean_temp"),
        F.stddev_samp("temperatura").alias("std_temp"),
        F.avg("umidade").alias("mean_hum"),
        F.stddev_samp("umidade").alias("std_hum"),
        F.avg("pressao").alias("mean_press"),
        F.stddev_samp("pressao").alias("std_press")
    ).first()
    mean_temp = stats["mean_temp"]
    std_temp = stats["std_temp"]
    mean_hum = stats["mean_hum"]
    std_hum = stats["std_hum"]
    mean_press = stats["mean_press"]
    std_press = stats["std_press"]

    # Detecting anomalies
    df_with_anomalies = df.withColumn(
        "is_temp_anomaly",
        F.abs(df["temperatura"] - mean_temp) > (3 * std_temp)
    ).withColumn(
        "is_hum_anomaly",
        F.abs(df["umidade"] - mean_hum) > (3 * std_hum)
    ).withColumn(
        "is_press_anomaly",
        F.abs(df["pressao"] - mean_press) > (3 * std_press)
    )

    # - Metric 1 -
    metric_1 = df_with_anomalies.groupBy("id_estacao") \
        .agg(
            (F.sum(F.col("is_temp_anomaly").cast("integer")) / F.count("*") * 100).alias("perc_anomaly_temp"),
            (F.sum(F.col("is_hum_anomaly").cast("integer")) / F.count("*") * 100).alias("perc_anomaly_hum"),
            (F.sum(F.col("is_press_anomaly").cast("integer")) / F.count("*") * 100).alias("perc_anomaly_press")
        )

    # Saving result
    # TODO: more efficient way
    metric_1.write.mode("overwrite").csv("output/metric_1")

    
    # - Metric 2 -
    # Filtering out non-anomalies
    df_non_anomalous = df_with_anomalies.filter(
        ~F.col("is_temp_anomaly") & ~F.col("is_hum_anomaly") & ~F.col("is_press_anomaly")
    )

    # Defines the window: per region, time-sorted and by hour
    window_spec_1hr = (
        Window.partitionBy("regiao") \
        .orderBy(F.to_unix_timestamp("timestamp")) \
        .rangeBetween(-3600, 0)
    )

    # Calculates moving average for each sensor
    metric_2 = df_non_anomalous.withColumn(
        "moving_avg_temp", F.avg("temperatura").over(window_spec_1hr)
    ).withColumn(
        "moving_avg_hum", F.avg("umidade").over(window_spec_1hr)
    ).withColumn(
        "moving_avg_press", F.avg("pressao").over(window_spec_1hr)
    )

    metric_2.select("id_estacao", "regiao", "timestamp", "moving_avg_temp", "moving_avg_hum", "moving_avg_press") \
            .write.mode("overwrite").parquet("output/metric_2")


    # - Metric 3 -
    # Creates numeric flags for anomalies
    df_anomaly_flags = (
        df_with_anomalies.withColumn("temp_flag", F.col("is_temp_anomaly").cast("integer")) \
        .withColumn("hum_flag", F.col("is_hum_anomaly").cast("integer")) \
        .withColumn("press_flag", F.col("is_press_anomaly").cast("integer"))
    )

    # Groups events in 10 minutes windows
    df_windowed = df_anomaly_flags.groupBy(
        "id_estacao",
        F.window("timestamp", "10 minutes")
    ).agg(
        F.max("temp_flag").alias("has_temp_anomaly"),
        F.max("hum_flag").alias("has_hum_anomaly"),
        F.max("press_flag").alias("has_press_anomaly")
    )

    # Counting how many sensors with anomalies in the windows
    df_anomaly_counts_per_window = df_windowed.withColumn(
        "distinct_anomaly_types",
        F.col("has_temp_anomaly") + F.col("has_hum_anomaly") + F.col("has_press_anomaly")
    )

    # Filter windows with 2 or more sensors in a window
    df_target_windows = df_anomaly_counts_per_window.filter(F.col("distinct_anomaly_types") >= 2)

    # Counts the total number of these per station
    metric_3 = df_target_windows.groupBy("id_estacao").count() \
                            .withColumnRenamed("count", "num_periods_multi_anomaly")

    # Salva o resultado
    metric_3.write.mode("overwrite").csv("output/metric_3")

    spark.stop()

if __name__ == "__main__":
    main()