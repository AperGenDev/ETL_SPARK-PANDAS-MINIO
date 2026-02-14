FROM apache/airflow:2.10.5-python3.9

USER root
# Устанавливаем Java
RUN apt update && \
    apt install -y openjdk-17-jdk && \
    apt clean

# Копируем requirements.txt
COPY req.txt /req.txt

# Переключаемся на airflow пользователя для установки Python пакетов
USER airflow
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r /req.txt