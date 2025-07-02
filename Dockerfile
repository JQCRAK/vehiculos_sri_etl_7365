FROM apache/airflow:2.7.3

# Cambiar a usuario root para instalar paquetes
USER root

# Instalar dependencias del sistema si es necesario
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && apt-get clean

# Cambiar de vuelta al usuario airflow
USER airflow

# Copiar requirements
COPY config/requirements.txt /opt/airflow/requirements.txt

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
