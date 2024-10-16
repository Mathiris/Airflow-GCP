import random
import uuid
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator

# Définition par défaut des paramètres pour le DAG
default_args = {
    'owner': 'admin',
    'start_date': days_ago(1),
    'retries': 5,
}

# Fonction Python pour générer des données aléatoires
def generate_random_data(**kwargs):
    first_names = ['Lisa', 'Jeremy', 'Heather', 'Daniel', 'Kelly', 'Emily', 'Natalie', 'Julia', 'Joe', 'Cody']

    random_data = {
        'first_name': random.choice(first_names),  # Sélectionne un prénom aléatoire
    }
    
    # Pousser les données dans XComs pour les passer à la tâche suivante
    kwargs['ti'].xcom_push(key='random_data', value=random_data)

# Création du DAG
with DAG(
    dag_id='example_bigquery_dag_select_from_python',
    default_args=default_args,
    schedule_interval='*/20 * * * *',  # Exécute toutes les 20 minutes
    catchup=True,
) as dag:

    # Tâche Python pour générer des données aléatoires
    generate_random_data_task = PythonOperator(
        task_id='generate_random_data',
        python_callable=generate_random_data,
        provide_context=True,
    )

    # Tâche BigQuery pour sélectionner des données basées sur le prénom généré
    bigquery_select_task = BigQueryInsertJobOperator(
        task_id='run_bigquery_where',
        configuration={
            "query": {
                "query": """
                SELECT * 
                FROM `skilled-loader-436608-i6.dbt_tutorial.custom_shop_customers`
                WHERE first_name = '{{ ti.xcom_pull(task_ids='generate_random_data', key='random_data')['first_name'] }}';
                """,
                "useLegacySql": False
            }
        },
        location='EU',
        gcp_conn_id='google_cloud_default'
    )

    # Tâche BigQuery pour exécuter une requête SQL pour récupérer les 1000 premiers enregistrements
    bigquery_limit_task = BigQueryInsertJobOperator(
        task_id='run_bigquery_query',
        configuration={
            "query": {
                "query": "SELECT * FROM `skilled-loader-436608-i6.dbt_tutorial.custom_shop_customers` LIMIT 1000;",
                "useLegacySql": False,
            }
        },
        location='EU',
        gcp_conn_id='google_cloud_default'
    )

    # Définition des dépendances
    generate_random_data_task >> bigquery_select_task >> bigquery_limit_task
