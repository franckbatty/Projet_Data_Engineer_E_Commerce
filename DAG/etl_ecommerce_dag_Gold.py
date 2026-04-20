"""
DAG pour la vérification des vues de la couche Gold du projet E-commerce.
Ce DAG s'assure que toutes les vues nécessaires existent dans le schéma Gold.
"""

# === Importation des modules nécessaires ===
import sys
sys.path.append("/opt/airflow/ETL")  # on force Python à inclure le dossier ETL

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# === Importation des fonctions ETL depuis le package ETL ===
try:
    from ETL.etl_e_commerce_Gold import (
        start_message,
        verify_gold_views,
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

# === Définition du DAG Gold ===
with DAG(
    'Projet_E_commerce_ETL_Gold',
    default_args=default_args,
    description="DAG Gold pour vérifier l'existence des vues nécessaires dans le schéma Gold",
    schedule_interval=None,
    catchup=False,
    tags=['e-commerce', 'gold', 'vues']
) as dag:

    start_task = PythonOperator(
        task_id='START',
        python_callable=start_message
    )

    verify_gold_views_task = PythonOperator(
        task_id='VERIFY_GOLD_VIEWS',
        python_callable=verify_gold_views,
        doc_md='''
        # Vérification des vues Gold
        Cette tâche vérifie l'existence de toutes les vues nécessaires dans le schéma Gold.
        Si une vue est manquante, la tâche échoue et génère une erreur.

        ## Vues vérifiées
        - Tables de dimension (dim_client, dim_produit, dim_commande)
        - Tables de faits (fait_ventes_aggregees)
        - Vues analytiques (ventes_par_categorie, performance_clients, tendances_ventes_mensuelles)
        '''
    )

    end_task = PythonOperator(
        task_id='END',
        python_callable=end_message
    )

    start_task >> verify_gold_views_task >> end_task
