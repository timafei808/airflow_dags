from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Функция, которая будет выполняться в таске
def print_hello():
    print("Hello from Airflow!")

# Определяем DAG
with DAG(
    dag_id="example_dag",  # Название DAG
    start_date=datetime(2024, 1, 1),  # Дата начала
    schedule_interval="@daily",  # Запуск каждый день
    catchup=False,  # Отключаем ретроактивные запуски
) as dag:

    # Определяем таску
    task = PythonOperator(
        task_id="print_hello",  # Название таски
        python_callable=print_hello,  # Функция, которая выполнится
    )

    task  # Запускаем таску

