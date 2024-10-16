from airflow import DAG  # Import direct de DAG
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from datetime import datetime, timedelta

# DAG configuration variables
default_args = {
    'owner': 'admin',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='data_fusion_dag',
    default_args=default_args,
    description='A DAG to trigger a Data Fusion pipeline',
    start_date=datetime(2024, 10, 10),  # La date de début est bien définie
    schedule_interval='@daily'
) as dag:

    start_pipeline = CloudDataFusionStartPipelineOperator(
        location='eu-west1',  # Localisation de l'instance Data Fusion
        pipeline_name='test_dag',  # Nom du pipeline Data Fusion
        instance_name='datalec-df01-euw1-prd',  # Nom de l'instance
        task_id="start_pipeline",  # Nom unique de la tâche dans le DAG
        namespace='dev',  # Namespace du pipeline dans Data Fusion
    )

    start_pipeline  # Déclaration explicite de la tâche dans le DAG
