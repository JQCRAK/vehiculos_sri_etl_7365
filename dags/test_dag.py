from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

def test_function():
    print("¡Airflow está funcionando correctamente!")
    print("Conexión establecida exitosamente")
    return "success"

# Configuración del DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Crear el DAG
dag = DAG(
    'test_installation',
    default_args=default_args,
    description='DAG de prueba para verificar instalación',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Definir tareas
start = DummyOperator(
    task_id='start',
    dag=dag
)

test_task = PythonOperator(
    task_id='test_airflow',
    python_callable=test_function,
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

# Definir dependencias
start >> test_task >> end