"""
DAG pour la vérification des vues de la couche Gold du projet E-commerce.
Ce DAG s'assure que toutes les vues nécessaires existent dans le schéma Gold.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Ajout du chemin vers le fichier ETL
sys.path.append('/home/franck/airflow/Script_ETL_Tous_PROJETS/e_commerce')

from etl_e_commerce_Gold import start_message, verify_gold_views, end_message


# Configuration des arguments par défaut du DAG 
default_args = {
    'owner': 'franck_BATTY',              # Propriétaire du DAG
    'depends_on_past': False,              # Ne dépend pas des exécutions précédentes
    'email_on_failure': False,             # Pas d'email en cas d'échec
    'email_on_retry': False,               # Pas d'email en cas de nouvelle tentative
    'retries': 1,                          # Une seule tentative en cas d'échec
    'retry_delay': timedelta(minutes=5),   # Attente de 5 minutes avant nouvelle tentative
    'start_date': datetime(2025, 6, 9),    # Date de début du DAG
}

# Définition du DAG
with DAG(
    'Projet_E_commerce_ETL_Gold',          # Identifiant unique du DAG
    default_args=default_args,             # Arguments par défaut
    description='''
        DAG pour la vérification des vues de la couche Gold.
        Vérifie l'existence des vues suivantes :
        - dim_client
        - dim_produit
        - dim_commande
        - fait_ventes_aggregees
    ''', 
    schedule_interval=None,                # Pas de planification automatique c'est-à-dire que le DAG ne s'exécutera pas automatiquement à des intervalles réguliers.
    catchup=False,                         # Pas de rattrapage des exécutions manquées c'est-à-dire que le DAG ne s'exécutera pas pour les dates passées.
    tags=['e-commerce', 'gold', 'vues'],   # Tags pour faciliter la recherche
) as dag:

    # Tâche de début du DAG
    start_task = PythonOperator(
        task_id='START',
        python_callable=start_message
    )
    # Définition de la tâche de vérification des vues
    verify_gold_views_task = PythonOperator(
        task_id='VERIFY_GOLD_VIEWS',           # Identifiant de la tâche
        python_callable=verify_gold_views,     # Fonction à exécuter
        doc_md='''
        # Vérification des vues Gold
        
        Cette tâche vérifie l'existence de toutes les vues nécessaires dans le schéma Gold.
        Si une vue est manquante, la tâche échoue et génère une erreur.
        
        ## Vues vérifiées
        - Tables de dimension (dim_client, dim_produit, dim_commande)
        - Tables de faits (fait_ventes_aggregees)
        - Vues analytiques (ventes_par_categorie, performance_clients, tendances_ventes_mensuelles)
        
        ## Processus
        1. Connexion à la base de données
        2. Vérification des vues accessibles
        3. Vérification de chaque vue requise
        4. Génération d'erreur si une vue est manquante
        '''
    )

    # Tache de Fin du DAG
    end_task = PythonOperator(
        task_id='END',
        python_callable=end_message
    )

    # Définition du flux d'exécution 
    start_task >> verify_gold_views_task >> end_task
     