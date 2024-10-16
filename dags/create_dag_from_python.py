from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'admin',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# Function to be called in the task
def main(name,age):
    print(f"hello world {name}, i'm {age}")

def get_name():
    return 'Tomzuke'

# Define the DAG
with DAG(
    dag_id='my_first_dag_v03',
    default_args=default_args,
    description='Our first dag with python V2',
    start_date=datetime(2024, 10, 11), 
    schedule_interval='@daily'
) as dag:
    
    # Define the PythonOperator task
    task1 = PythonOperator(
        task_id='main',  
        python_callable=main,  
        op_kwargs ={'name': 'Math', 'age': '37'}
    )

    task2 = PythonOperator(
        task_id='get_name',  
        python_callable=get_name,  
        
    )

    task1
    task2  # Task assignment should not cause an error
