"""
DAG pour la couche Silver du projet E-commerce.
Ce DAG transforme les données brutes en données nettoyées et structurées.
Il déclenche automatiquement le DAG Gold une fois le traitement Silver terminé.
"""

# === Importation des modules nécessaires ===
import sys
sys.path.append("/opt/airflow/ETL")  # on force Python à inclure le dossier ETL

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

# === Importation des fonctions ETL depuis le package ETL ===
try:
    from ETL.etl_e_commerce_Silver import (
        start_message,
        extract_data_postgresql,
        transform_data,
        loading_data,
        end_message
    )
except ModuleNotFoundError as e:
    raise ModuleNotFoundError(f"Erreur lors de l'importation des modules ETL : {e}")

# === Configuration des arguments par défaut du DAG ===
default_args = {
    'owner': 'franck_BATTY',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 6, 9),
}

# === Définition du DAG Silver ===
with DAG(
    'Projet_E_commerce_ETL_Silver',
    default_args=default_args,
    description="DAG Silver pour transformer les données brutes en données nettoyées et structurées",
    schedule_interval=None,
    catchup=False,
    tags=['e-commerce', 'silver', 'etl']
) as dag:

    attente_signal_bronze = PythonSensor(
        task_id="attente_signal_bronze",
        python_callable=lambda **kwargs: kwargs['dag_run'].conf.get('bronze_termine') is True,
        mode="poke",
        poke_interval=30,
        timeout=3600
    )

    start = PythonOperator(
        task_id="START",
        python_callable=start_message
    )

    extract = PythonOperator(
        task_id="EXTRACT",
        python_callable=extract_data_postgresql,
        op_kwargs={
            "date_traitement": "{{ dag_run.conf.date_traitement }}"
        }
    )

    transform = PythonOperator(
        task_id="TRANSFORM",
        python_callable=transform_data,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    load = PythonOperator(
        task_id="LOAD",
        python_callable=loading_data,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    end = PythonOperator(
        task_id="END",
        python_callable=end_message,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="TRIGGER_GOLD_DAG",
        trigger_dag_id="Projet_E_commerce_ETL_Gold",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    attente_signal_bronze >> start >> extract >> transform >> load >> end >> trigger_gold
