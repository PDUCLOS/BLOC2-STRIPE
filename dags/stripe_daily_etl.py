"""DAG Airflow — export batch quotidien PostgreSQL → Snowflake.

Orchestre les deux scripts déjà existants (etl/snowflake_setup.py,
etl/load_snowflake.py) au lieu de réimplémenter leur logique — ce DAG est
une couche d'orchestration/planification, pas un second pipeline.

Remplace le script cron documenté comme limite assumée dans
docs/PRESENTATION.md §8.1 ("Pas d'Airflow : juste un script etl/load_snowflake.py").

Planification : 02:00 UTC quotidien, cohérent avec la mention dans
docs/OLAP_SCHEMA_DESIGN.md et docs/PRESENTATION.md ("Airflow (batch · 02:00 UTC)").
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stripe_daily_etl",
    description="Export quotidien PostgreSQL (OLTP) -> Snowflake (OLAP)",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["stripe", "etl", "snowflake"],
) as dag:

    # Note : la création du schéma (etl/snowflake_setup.py) N'EST PAS dans ce
    # DAG — c'est un bootstrap ponctuel (make snowflake-setup), pas une étape
    # à rejouer chaque nuit. La faire tourner ici échouerait bruyamment tous
    # les jours tant qu'aucun compte Snowflake réel n'est configuré, alors
    # que le schéma n'a besoin d'être créé qu'une seule fois.
    #
    # Sans SNOWFLAKE_ACCOUNT configuré, cette tâche tourne en dry-run (affiche
    # ce qui serait chargé sans écrire) plutôt que d'échouer — cf.
    # load_to_snowflake() dans etl/load_snowflake.py. Permet au DAG de
    # s'exécuter de bout en bout même sans compte Snowflake réel connecté
    # (contexte démo/certification).
    export_transactions = BashOperator(
        task_id="snowflake_export",
        bash_command="python /opt/airflow/etl/load_snowflake.py",
    )
