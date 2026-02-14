from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, sum as spark_sum, round as spark_round
import pandas as pd
from minio import Minio
import io
import logging
import pyarrow.parquet as pq
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def run_spark_etl(**context):
    logging.basicConfig(level=logging.INFO)
    logging.info("Запуск Spark ETL")
    start_time = time.time()
    
    spark = SparkSession.builder \
        .appName("Airflow Spark ETL") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        stage_start = time.time() 
        input_path = "/opt/airflow/raw_data"
        logging.info(f"Чтение из: {input_path}")
        
        logging.info("Чтение users.parquet через Pandas...")
        users_pd = pd.read_parquet(f"{input_path}/user.parquet")
        
        logging.info("Чтение stores.parquet через Pandas...")
        stores_pd = pd.read_parquet(f"{input_path}/store.parquet")
        
        logging.info("Чтение orders.parquet через Pandas...")
        orders_pd = pd.read_parquet(f"{input_path}/order.parquet")
        stage_end = time.time()
        logging.info(f"Чтение данных завершено за {stage_end - stage_start} секунд")

        logging.info("Конвертация timestamp из наносекунд")
        users_pd['created_at'] = pd.to_datetime(users_pd['created_at'], unit='ns')
        
        logging.info(f"Загружено: user={len(users_pd)}, store={len(stores_pd)}, order={len(orders_pd)}")
        stage_start = time.time()
        logging.info("Конвертация из Pandas в Spark")
        users_df = spark.createDataFrame(users_pd)
        stores_df = spark.createDataFrame(stores_pd)
        orders_df = spark.createDataFrame(orders_pd)
        stage_end = time.time()
        logging.info(f"Конвертация в Spark завершена за {stage_end - stage_start} секунд")
        logging.info("Схема users после конвертации:")
        users_df.printSchema()
        
        stage_start = time.time()
        users_2025 = users_df.filter(year(col("created_at")) == 2025)
        
        result_df = orders_df \
            .join(users_2025, orders_df.user_id == users_2025.id) \
            .join(stores_df, orders_df.store_id == stores_df.id) \
            .groupBy(stores_df.city, stores_df.name) \
            .agg(spark_round(spark_sum("amount"), 2).alias("total_amount")) \
            .orderBy(col("total_amount").desc()) \
            .limit(3) \
            .select(
                col("city"),
                col("name").alias("store_name"),
                col("total_amount")
            )
        
        stage_end = time.time()
        logging.info(f"Spark трансформации завершены за {stage_end - stage_start} секунд")

        logging.info("Результат:")
        result_df.show(truncate=False)
        
        result_pd = result_df.toPandas()
        
        stage_start = time.time()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        


        minio_client = Minio(
            "minio:9000",
            access_key="s3admin",
            secret_key="s3admin123",
            secure=False
        )
        
        bucket_name = "processed-data"
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logging.info(f"Создан bucket: {bucket_name}")
        
        buffer = io.BytesIO()
        result_pd.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=f"top_stores/{timestamp}/result.parquet",
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/parquet"
        )
        logging.info(f"Загружен: top_stores/{timestamp}/result.parquet")
        
        stage_end = time.time()
        logging.info(f"Загрузка в MinIO завершена за {stage_end - stage_start} секунд")
         
        end_time = time.time() 
        logging.info(f"  Общее время: {(end_time - start_time)} секунд")

        return f"ETL завершен! Результат в {bucket_name}/top_stores/{timestamp}/"
        
    except Exception as e:
        logging.error(f"Ошибка: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

with DAG(
    'spark_etl_pandas',
    default_args=default_args,
    description='Spark ETL with Pandas',
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'pandas'],
) as dag:

    spark_task = PythonOperator(
        task_id='run_spark_etl',
        python_callable=run_spark_etl,
        retries=3,
        retry_delay=timedelta(seconds=30),
    )