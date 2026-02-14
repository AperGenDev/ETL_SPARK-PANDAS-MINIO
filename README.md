# ETL_SPARK-PANDAS-MINIO

ETL pipeline для обработки данных с использованием Spark, Pandas и MinIO.


# Создайте виртуальное окружение
python -m venv venv
source venv/Scripts/activate

# Запуск

1) В файл .env необходимо указать следующие атрибуты (не меняйте имена переменных, так как на них есть ссылки в проекте):

```
FERNET_KEY=xMnujsVMPx3t3XTBStsl-MmSoYe0cVyUfBzS0nVENzM=

_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
AIRFLOW_UID=501
AIRFLOW_GID=0

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow


MINIO_ROOT_USER=s3admin
MINIO_ROOT_PASSWORD=s3admin123

AWS_ACCESS_KEY_ID=key_inside_minio
AWS_SECRET_ACCESS_KEY=key_inside_minio
```

2) Запускаем docker-compose

```bash
docker-compose up -d
```
3) После скачивания всех образов и поднятия всех контейнеров открыть браузер и прописать в поисковой строке:
```
localhost:8080
```
4) Зайти в airflow (по умолчанию логин и пароль - admin)
5) Запустить DAG 
6) Перейти на новую вкладку и прописать:
```
  localhost:9001
```
7) входим в minio(по умолчанию логин - s3admin и пароль - s3admin123)
8) в бакете processed-data будет папка top_stores в которой будут храниться обработанные данные
<img width="1869" height="595" alt="изображение" src="https://github.com/user-attachments/assets/7336b06e-bb22-431a-89ae-cc53d7abe735" />


# Структура проекта
```
ETL_SPARK-PANDAS-MINIO/
├── airflow_dockerfile/    # Docker образ для airflow              
│   ├── Dockerfile
│   └── req.txt
├── dags/                 
│   └── with_pyspark.py
├── logs/                   # Логи 
├── raw_data/               # Сырые данные 
│   ├── order.parquet        
│   ├── store.parquet
│   └── user.parquet
├── s3_storage/             # Данные из minio
├── .gitignore
├── README.md 
└── docker-compose.yaml
```

В директории raw_data хранятся тестовые файлы в формате .parquet их можно заменить на свои собственные.


# Используемые инструменты:
Docker
Airflow
Python
Spark
Minio


