
from datetime import datetime, timedelta
import pandas as pd
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator as DummyOperator
from google.cloud import storage, bigquery
from google.oauth2 import service_account
import io
import os

# Configuración de credenciales
CREDENTIALS_PATH = '/opt/airflow/config/service-account-key.json'
PROJECT_ID = 'etl-vehiculos-sri'  
BUCKET_NAME = 'etl-vehiculos-sri'
DATASET_ID = 'dw_vehiculos_ecuador'

# Configuración por defecto del DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
dag = DAG(
    'vehiculos_sri_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL para datos de vehículos del SRI Ecuador',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'vehiculos', 'sri', 'ecuador']
)

def get_gcp_clients():
    """
    Inicializa los clientes de Google Cloud Platform
    """
    try:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
        storage_client = storage.Client(credentials=credentials, project=PROJECT_ID)
        bigquery_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        
        logging.info("Clientes de GCP inicializados correctamente")
        return storage_client, bigquery_client
    except Exception as e:
        logging.error(f"Error inicializando clientes GCP: {str(e)}")
        raise

def read_csv_from_gcs(storage_client, bucket_name, file_path):
    """
    Lee un archivo CSV desde Google Cloud Storage
    """
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        content = blob.download_as_text()
        df = pd.read_csv(io.StringIO(content))
        logging.info(f"Archivo {file_path} leído correctamente. Registros: {len(df)}")
        return df
    except Exception as e:
        logging.error(f"Error leyendo archivo {file_path}: {str(e)}")
        raise

def load_dataframe_to_bigquery(bigquery_client, dataframe, table_id, write_disposition='WRITE_TRUNCATE'):
    """
    Carga un DataFrame a BigQuery
    """
    try:
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=True
        )
        
        job = bigquery_client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        )
        job.result()  # Esperar a que termine
        
        logging.info(f"Tabla {table_id} cargada correctamente. Registros: {len(dataframe)}")
    except Exception as e:
        logging.error(f"Error cargando tabla {table_id}: {str(e)}")
        raise

def extract_and_load_dim_vehiculo(**context):
    """
    Procesa la dimensión DIM_VEHICULO
    """
    logging.info("Iniciando procesamiento de DIM_VEHICULO")
    
    storage_client, bigquery_client = get_gcp_clients()
    
    # Leer archivo principal
    df_main = read_csv_from_gcs(storage_client, BUCKET_NAME, 'raw-data/matriculaciones_2024.csv')
    
    # Leer diccionario de marcas si existe
    try:
        df_marcas = read_csv_from_gcs(storage_client, BUCKET_NAME, 'raw-data/diccionarios/diccionario_marcas.csv')
        logging.info("Diccionario de marcas cargado")
    except:
        logging.warning("Diccionario de marcas no encontrado, usando datos directos")
        df_marcas = pd.DataFrame()
    
    # Procesar dimensión vehículo
    dim_vehiculo = df_main[['MARCA', 'MODELO', 'PAÍS', 'AÑO MODELO', 'CLASE', 'SUB CLASE', 'TIPO']].copy()
    
    # Limpiar y normalizar datos
    dim_vehiculo['MARCA'] = dim_vehiculo['MARCA'].str.upper().str.strip()
    dim_vehiculo['MODELO'] = dim_vehiculo['MODELO'].str.upper().str.strip()
    dim_vehiculo['PAÍS'] = dim_vehiculo['PAÍS'].str.upper().str.strip()
    
    # Eliminar duplicados
    dim_vehiculo = dim_vehiculo.drop_duplicates()
    
    # Generar clave subrogada
    dim_vehiculo.reset_index(drop=True, inplace=True)
    dim_vehiculo['vehiculo_key'] = range(1, len(dim_vehiculo) + 1)
    
    # Reordenar columnas
    dim_vehiculo = dim_vehiculo[['vehiculo_key', 'MARCA', 'MODELO', 'PAÍS', 'AÑO MODELO', 'CLASE', 'SUB CLASE', 'TIPO']]
    
    # Cargar a BigQuery
    table_id = f"{PROJECT_ID}.{DATASET_ID}.DIM_VEHICULO"
    load_dataframe_to_bigquery(bigquery_client, dim_vehiculo, table_id)
    
    logging.info(f"DIM_VEHICULO procesada: {len(dim_vehiculo)} registros")

def extract_and_load_dim_tecnologia(**context):
    """
    Procesa la dimensión DIM_TECNOLOGIA
    """
    logging.info("Iniciando procesamiento de DIM_TECNOLOGIA")
    
    storage_client, bigquery_client = get_gcp_clients()
    
    # Leer archivo principal
    df_main = read_csv_from_gcs(storage_client, BUCKET_NAME, 'raw-data/matriculaciones_2024.csv')
    
    # Procesar dimensión tecnología
    dim_tecnologia = df_main[['TIPO COMBUSTIBLE', 'CILINDRAJE']].copy()
    
    # Limpiar datos
    dim_tecnologia['TIPO COMBUSTIBLE'] = dim_tecnologia['TIPO COMBUSTIBLE'].str.upper().str.strip()
    dim_tecnologia['CILINDRAJE'] = pd.to_numeric(dim_tecnologia['CILINDRAJE'], errors='coerce')
    
    # Crear categorías de cilindraje
    def categorizar_cilindraje(cc):
        if pd.isna(cc):
            return 'NO ESPECIFICADO'
        elif cc <= 1000:
            return 'PEQUEÑO (≤1000cc)'
        elif cc <= 2000:
            return 'MEDIANO (1001-2000cc)'
        elif cc <= 3000:
            return 'GRANDE (2001-3000cc)'
        else:
            return 'EXTRA GRANDE (>3000cc)'
    
    dim_tecnologia['categoria_cilindraje'] = dim_tecnologia['CILINDRAJE'].apply(categorizar_cilindraje)
    
    # Eliminar duplicados
    dim_tecnologia = dim_tecnologia.drop_duplicates()
    
    # Generar clave subrogada
    dim_tecnologia.reset_index(drop=True, inplace=True)
    dim_tecnologia['tecnologia_key'] = range(1, len(dim_tecnologia) + 1)
    
    # Reordenar columnas
    dim_tecnologia = dim_tecnologia[['tecnologia_key', 'TIPO COMBUSTIBLE', 'CILINDRAJE', 'categoria_cilindraje']]
    
    # Cargar a BigQuery
    table_id = f"{PROJECT_ID}.{DATASET_ID}.DIM_TECNOLOGIA"
    load_dataframe_to_bigquery(bigquery_client, dim_tecnologia, table_id)
    
    logging.info(f"DIM_TECNOLOGIA procesada: {len(dim_tecnologia)} registros")

def extract_and_load_dim_ubicacion(**context):
    """
    Procesa la dimensión DIM_UBICACION
    """
    logging.info("Iniciando procesamiento de DIM_UBICACION")
    
    storage_client, bigquery_client = get_gcp_clients()
    
    # Leer archivo principal
    df_main = read_csv_from_gcs(storage_client, BUCKET_NAME, 'raw-data/matriculaciones_2024.csv')
    
    # Procesar dimensión ubicación
    dim_ubicacion = df_main[['CANTÓN']].copy()
    dim_ubicacion = dim_ubicacion.drop_duplicates()
    
    # Limpiar datos
    dim_ubicacion['CANTÓN'] = dim_ubicacion['CANTÓN'].str.strip()
    
    # Mapeo básico de provincias (simplificado)
    provincia_mapping = {
        '01': 'AZUAY', '02': 'BOLÍVAR', '03': 'CAÑAR', '04': 'CARCHI',
        '05': 'COTOPAXI', '06': 'CHIMBORAZO', '07': 'EL ORO', '08': 'ESMERALDAS',
        '09': 'GUAYAS', '10': 'IMBABURA', '11': 'LOJA', '12': 'LOS RÍOS',
        '13': 'MANABÍ', '14': 'MORONA SANTIAGO', '15': 'NAPO', '16': 'PASTAZA',
        '17': 'PICHINCHA', '18': 'TUNGURAHUA', '19': 'ZAMORA CHINCHIPE',
        '20': 'GALÁPAGOS', '21': 'SUCUMBÍOS', '22': 'ORELLANA', '23': 'SANTO DOMINGO',
        '24': 'SANTA ELENA'
    }
    
    # Extraer código de provincia (primeros 2 dígitos)
    dim_ubicacion['provincia_codigo'] = dim_ubicacion['CANTÓN'].str[:2]
    dim_ubicacion['provincia_nombre'] = dim_ubicacion['provincia_codigo'].map(provincia_mapping).fillna('NO IDENTIFICADA')
    
    # Generar clave subrogada
    dim_ubicacion.reset_index(drop=True, inplace=True)
    dim_ubicacion['ubicacion_key'] = range(1, len(dim_ubicacion) + 1)
    
    # Reordenar columnas
    dim_ubicacion = dim_ubicacion[['ubicacion_key', 'CANTÓN', 'provincia_codigo', 'provincia_nombre']]
    
    # Cargar a BigQuery
    table_id = f"{PROJECT_ID}.{DATASET_ID}.DIM_UBICACION"
    load_dataframe_to_bigquery(bigquery_client, dim_ubicacion, table_id)
    
    logging.info(f"DIM_UBICACION procesada: {len(dim_ubicacion)} registros")

def extract_and_load_dim_tiempo(**context):
    """
    Procesa la dimensión DIM_TIEMPO
    """
    logging.info("Iniciando procesamiento de DIM_TIEMPO")
    
    storage_client, bigquery_client = get_gcp_clients()
    
    # Leer archivo principal
    df_main = read_csv_from_gcs(storage_client, BUCKET_NAME, 'raw-data/matriculaciones_2024.csv')
    
    # Procesar fechas
    df_main['FECHA PROCESO'] = pd.to_datetime(df_main['FECHA PROCESO'], errors='coerce')
    
    # Crear dimensión tiempo
    fechas_unicas = df_main['FECHA PROCESO'].dropna().dt.date.unique()
    
    dim_tiempo_data = []
    for fecha in fechas_unicas:
        fecha_dt = pd.to_datetime(fecha)
        dim_tiempo_data.append({
            'fecha': fecha,
            'año': fecha_dt.year,
            'mes': fecha_dt.month,
            'día': fecha_dt.day,
            'trimestre': f"Q{(fecha_dt.month-1)//3 + 1}",
            'día_semana': fecha_dt.day_name(),
            'es_fin_semana': fecha_dt.weekday() >= 5
        })
    
    dim_tiempo = pd.DataFrame(dim_tiempo_data)
    
    # Generar clave subrogada
    dim_tiempo.reset_index(drop=True, inplace=True)
    dim_tiempo['tiempo_key'] = range(1, len(dim_tiempo) + 1)
    
    # Reordenar columnas
    dim_tiempo = dim_tiempo[['tiempo_key', 'fecha', 'año', 'mes', 'día', 'trimestre', 'día_semana', 'es_fin_semana']]
    
    # Cargar a BigQuery
    table_id = f"{PROJECT_ID}.{DATASET_ID}.DIM_TIEMPO"
    load_dataframe_to_bigquery(bigquery_client, dim_tiempo, table_id)
    
    logging.info(f"DIM_TIEMPO procesada: {len(dim_tiempo)} registros")

def extract_and_load_fact_matriculaciones(**context):
    """
    Procesa la tabla de hechos FACT_MATRICULACIONES
    """
    logging.info("Iniciando procesamiento de FACT_MATRICULACIONES")
    
    storage_client, bigquery_client = get_gcp_clients()
    
    # Leer archivo principal
    df_main = read_csv_from_gcs(storage_client, BUCKET_NAME, 'raw-data/matriculaciones_2024.csv')
    
    # Leer dimensiones desde BigQuery para hacer lookups
    dim_vehiculo = bigquery_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DIM_VEHICULO`").to_dataframe()
    dim_tecnologia = bigquery_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DIM_TECNOLOGIA`").to_dataframe()
    dim_ubicacion = bigquery_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DIM_UBICACION`").to_dataframe()
    dim_tiempo = bigquery_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DIM_TIEMPO`").to_dataframe()
    
    # Preparar datos para los lookups
    df_main['FECHA PROCESO'] = pd.to_datetime(df_main['FECHA PROCESO'], errors='coerce').dt.date
    df_main['MARCA'] = df_main['MARCA'].str.upper().str.strip()
    df_main['MODELO'] = df_main['MODELO'].str.upper().str.strip()
    df_main['TIPO COMBUSTIBLE'] = df_main['TIPO COMBUSTIBLE'].str.upper().str.strip()
    df_main['CILINDRAJE'] = pd.to_numeric(df_main['CILINDRAJE'], errors='coerce')
    df_main['AVALÚO'] = pd.to_numeric(df_main['AVALÚO'], errors='coerce')
    
    # Realizar lookups para obtener claves foráneas
    fact_table = df_main.merge(
        dim_vehiculo[['vehiculo_key', 'MARCA', 'MODELO', 'PAÍS', 'AÑO MODELO', 'CLASE', 'SUB CLASE', 'TIPO']],
        on=['MARCA', 'MODELO', 'PAÍS', 'AÑO MODELO', 'CLASE', 'SUB CLASE', 'TIPO'],
        how='left'
    )
    
    fact_table = fact_table.merge(
        dim_tecnologia[['tecnologia_key', 'TIPO COMBUSTIBLE', 'CILINDRAJE']],
        on=['TIPO COMBUSTIBLE', 'CILINDRAJE'],
        how='left'
    )
    
    fact_table = fact_table.merge(
        dim_ubicacion[['ubicacion_key', 'CANTÓN']],
        on=['CANTÓN'],
        how='left'
    )
    
    fact_table = fact_table.merge(
        dim_tiempo[['tiempo_key', 'fecha']],
        left_on=['FECHA PROCESO'],
        right_on=['fecha'],
        how='left'
    )
    
    # Seleccionar solo las columnas necesarias para la tabla de hechos
    fact_matriculaciones = fact_table[[
        'CATEGORÍA', 'vehiculo_key', 'tecnologia_key', 'ubicacion_key', 'tiempo_key',
        'AVALÚO', 'CILINDRAJE'
    ]].copy()
    
    # Limpiar registros con claves faltantes
    fact_matriculaciones = fact_matriculaciones.dropna(subset=['vehiculo_key', 'tecnologia_key', 'ubicacion_key', 'tiempo_key'])
    
    # Cargar a BigQuery
    table_id = f"{PROJECT_ID}.{DATASET_ID}.FACT_MATRICULACIONES"
    load_dataframe_to_bigquery(bigquery_client, fact_matriculaciones, table_id)
    
    logging.info(f"FACT_MATRICULACIONES procesada: {len(fact_matriculaciones)} registros")

# Definición de tareas del DAG
inicio = DummyOperator(
    task_id='inicio_pipeline',
    dag=dag
)

# Tareas para procesar dimensiones
task_dim_vehiculo = PythonOperator(
    task_id='extract_load_dim_vehiculo',
    python_callable=extract_and_load_dim_vehiculo,
    dag=dag
)

task_dim_tecnologia = PythonOperator(
    task_id='extract_load_dim_tecnologia',
    python_callable=extract_and_load_dim_tecnologia,
    dag=dag
)

task_dim_ubicacion = PythonOperator(
    task_id='extract_load_dim_ubicacion',
    python_callable=extract_and_load_dim_ubicacion,
    dag=dag
)

task_dim_tiempo = PythonOperator(
    task_id='extract_load_dim_tiempo',
    python_callable=extract_and_load_dim_tiempo,
    dag=dag
)

# Tarea para procesar tabla de hechos
task_fact_matriculaciones = PythonOperator(
    task_id='extract_load_fact_matriculaciones',
    python_callable=extract_and_load_fact_matriculaciones,
    dag=dag
)

fin = DummyOperator(
    task_id='fin_pipeline',
    dag=dag
)

# Definir dependencias del pipeline
inicio >> [task_dim_vehiculo, task_dim_tecnologia, task_dim_ubicacion, task_dim_tiempo]
[task_dim_vehiculo, task_dim_tecnologia, task_dim_ubicacion, task_dim_tiempo] >> task_fact_matriculaciones
task_fact_matriculaciones >> fin