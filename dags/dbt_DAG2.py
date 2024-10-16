from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Définition des paramètres par défaut pour le DAG
default_args = {
    'owner': 'admin',
    'start_date': days_ago(1),
    'retries': 1,
}

# Création du DAG
with DAG(
    dag_id='dbt_airflow_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Tâche dbt pour exécuter un modèle dbt
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /dbt-core-demo/dbt_core_demo && dbt run --profiles-dir .',
    )

    # Tâche dbt pour tester les résultats des modèles
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /dbt-core-demo/dbt_core_demo && dbt run --profiles-dir .',
    )

    # Ordre des tâches dans le workflow
    dbt_run >> dbt_test
