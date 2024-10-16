from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator  


# Définition par défaut des paramètres pour le DAG
default_args = {
    'owner': 'admin',
    'start_date': days_ago(1),
    'retries': 5,
}

# Création du DAG
with DAG(
    dag_id='example_bigquery_dag',
    default_args=default_args,
    schedule_interval='0 12 * * *',  # Exécute le DAG tous les jours à 12h00
    catchup=False,
) as dag:

    # Début du DAG
    start_task = EmptyOperator(
        task_id='start',
    )

    # Tâche BigQuery pour exécuter une requête SQL
    bigquery_task = BigQueryInsertJobOperator(
        task_id='run_bigquery_query',
        configuration={
            "query": {
                "query": "SELECT * FROM `skilled-loader-436608-i6.test.custom` LIMIT 1000;",
                "useLegacySql": False,
            }
        },
        location='EU',  # Spécifiez la région de votre BigQuery (par exemple US, EU)
        gcp_conn_id='google_cloud_default'
    )

    # Fin du DAG
    end_task = EmptyOperator(
        task_id='end',
    )

    # Définition des dépendances
    start_task >> bigquery_task >> end_task
