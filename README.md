# ETL_SPARK-PANDAS-MINIO

ETL pipeline для обработки данных с использованием Spark, Pandas и MinIO.


# Создайте виртуальное окружение
python -m venv venv
source venv/Scripts/activate

# Запускаем docker-compose

```bash
docker-compose up -d
```

## Структура проекта
```
ETL_SPARK-PANDAS-MINIO/
├── airflow_dockerfile/    # Docker образ для airflow              
│   ├── Dockerfile
│   └── req.txt
├── dags/                 
│   └── with_pyspark.py                   
├── raw_data                # Сырые данные 
│   ├── order.parquet        
│   ├── store.parquet
│   └── user.parquet
├── .gitignore
├── README.md 
└── docker-compose.yaml    # Docker Compose
```



