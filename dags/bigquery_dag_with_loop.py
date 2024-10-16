from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# Définition des paramètres par défaut pour le DAG
default_args = {
    'owner': 'admin',
    'start_date': days_ago(1),
    'retries': 5,
}

# Liste des valeurs à utiliser dans les requêtes
list_of_values = ['312ea5fd-3f30-4db2-9cbb-8c7148022ded', 'd07f5eac-b411-4fd0-bbbc-8271041babc7', 'b9f5f88e-71b9-4e73-8bc8-6117f04d44be']

# Création du DAG
with DAG(
    dag_id='example_bigquery_dag_with_loop',
    default_args=default_args,
    schedule_interval='0 12 * * *',  # Exécute le DAG tous les jours à 12h00
    catchup=False,
) as dag:

    # Début du DAG
    start_task = EmptyOperator(
        task_id='start',
    )

    # Utilisation d'un TaskGroup pour organiser les tâches BigQuery multiples
    with TaskGroup("run_bigquery_queries", tooltip="Exécuter des requêtes BigQuery pour chaque valeur") as tg1:

        for value in list_of_values:
            bigquery_task = BigQueryInsertJobOperator(
                task_id=f'run_bigquery_query_{value}',  # Tâche unique pour chaque valeur
                configuration={
                    "query": {
                        # Requête SQL modifiée pour utiliser la valeur courante
                        "query": f"SELECT * FROM `skilled-loader-436608-i6.test.custom` WHERE customer_id = '{value}' LIMIT 1000;",
                        "useLegacySql": False,
                    }
                },
                location='EU',  # Spécifiez la région de votre BigQuery
                gcp_conn_id='google_cloud_default'
            )

    # Fin du DAG
    end_task = EmptyOperator(
        task_id='end',
    )

    # Définition des dépendances
    start_task >> tg1 >> end_task
