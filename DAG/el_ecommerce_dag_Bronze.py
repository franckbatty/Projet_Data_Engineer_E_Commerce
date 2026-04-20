## == Pipeline ETL pour le projet E-commerce couche Bronze avec Airflow et Python == ##
# Ce script définit un DAG (Directed Acyclic Graph) pour exécuter un pipeline EL (Extract, Load) pour le projet E-commerce.

# === Importation des modules nécessaires ===
import sys
sys.path.append("/opt/airflow/ETL")  # on force Python à inclure le dossier ETL

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

# === Importation des fonctions ETL depuis le package ETL ===
try:
    from ETL.el_e_commerce_Bronze import (
        start_message,
        extract_data,
        loading_data,
        end_message
    )
except ModuleNotFoundError as e:
    raise ModuleNotFoundError(f"Erreur lors de l'importation des modules ETL : {e}")

# === Paramètres par défaut du DAG ===
default_args = {
    "owner": "franck_BATTY",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# === Définition du DAG Bronze ===
with DAG(
    "Projet_E_commerce_ETL_Bronze",
    default_args=default_args,
    description="DAG Bronze pour le pipeline ETL du projet E-commerce",
    schedule_interval="@daily",
    start_date=datetime(2025, 6, 9),
    catchup=False,
    tags=['Bronze', 'e-commerce', 'EL']
) as dag:

    start = PythonOperator(
        task_id="START",
        python_callable=start_message
    )

    extract = PythonOperator(
        task_id="EXTRACT",
        python_callable=extract_data,
        op_kwargs={
            "clients_file": "/home/franck/airflow/Data_Tous_Projets/e_commerce/clients.csv",
            "commandes_file": "/home/franck/airflow/Data_Tous_Projets/e_commerce/commandes.csv",
            "details_commandes_file": "/home/franck/airflow/Data_Tous_Projets/e_commerce/details_commandes.csv",
            "paiements_file": "/home/franck/airflow/Data_Tous_Projets/e_commerce/paiements.csv",
            "produits_file": "/home/franck/airflow/Data_Tous_Projets/e_commerce/produits.csv"
        }
    )

    load = PythonOperator(
        task_id="LOAD",
        python_callable=loading_data
    )

    end = PythonOperator(
        task_id="END",
        python_callable=end_message
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="Projet_E_commerce_ETL_Silver",
        conf={
            "source": "bronze",
            "date_traitement": "{{ ds }}",
            "bronze_termine": True
        },
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    start >> extract >> load >> end >> trigger_silver
