"""
DAG pour la couche Silver du projet E-commerce.
Ce DAG transforme les données brutes en données nettoyées et structurées.
Il déclenche automatiquement le DAG Gold une fois le traitement Silver terminé.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys

# Ajout du chemin vers le fichier ETL
sys.path.append('/home/franck/airflow/Script_ETL_Tous_PROJETS/e_commerce')

# Importation des fonctions ETL
try:
    from etl_e_commerce_Silver import start_message, extract_data_postgresql, transform_data, loading_data, end_message
except ModuleNotFoundError as e:
    raise ModuleNotFoundError(f"Erreur lors de l'importation des modules ETL : {e}")

# Configuration des arguments par défaut du DAG
default_args = {
    'owner': 'franck_BATTY',                    # Propriétaire du DAG
    'depends_on_past': False,              # Ne dépend pas des exécutions précédentes
    'email_on_failure': False,             # Pas d'email en cas d'échec
    'email_on_retry': False,               # Pas d'email en cas de nouvelle tentative
    'retries': 1,                          # Une seule tentative en cas d'échec
    'retry_delay': timedelta(minutes=5),   # Attente de 5 minutes avant nouvelle tentative
    'start_date': datetime(2025, 6, 9),    # Date de début du DAG
}

# Définition du DAG
with DAG(
    'Projet_E_commerce_ETL_Silver',        # Identifiant unique du DAG
    default_args=default_args,             # Arguments par défaut
    description='''
        DAG pour la couche Silver du projet E-commerce.
        Transforme les données brutes en données nettoyées et structurées.
        Déclenche automatiquement le DAG Gold une fois terminé.
    ''',
    schedule_interval=None,   # Pas de planification automatique
    catchup=False,                         # Pas de rattrapage des exécutions manquées
    tags=['e-commerce', 'silver', 'etl']   # Tags pour faciliter la recherche
) as dag:

    # ===== TÂCHE D'ATTENTE DU SIGNAL BRONZE =====
    # Cette tâche attend que le DAG Bronze ait terminé son traitement
    attente_signal_bronze = PythonSensor(
        task_id="attente_signal_bronze",
        python_callable=lambda **kwargs: kwargs['dag_run'].conf.get('bronze_termine') is True,
        mode="poke", # Le Mode "poke" permet de vérifier périodiquement une condition 
        # c'est-à-dire que la tâche va vérifier régulièrement si le signal de fin du DAG Bronze est reçu.
        poke_interval=30,  # Vérifie toutes les 30 secondes
        timeout=3600,  # Timeout permet de définir une durée maximale d'attente (1 heure ici) 
        # c'est-à-dire que si le signal n'est pas reçu dans ce délai, la tâche échouera.
    )

    # ===== TÂCHES DU PIPELINE SILVER =====
    
    # Tâche : Début du pipeline
    start = PythonOperator(
        task_id="START",
        python_callable=start_message
    )

    # ===== TÂCHE D'EXTRACTION CRUCIALE =====
    # Cette tâche est fondamentale car elle :
    # 1. Lit les données depuis la couche Bronze (PostgreSQL)
    # 2. Vérifie l'intégrité des données
    # 3. Prépare les données pour la transformation
    extract = PythonOperator(
        task_id="EXTRACT",
        python_callable=extract_data_postgresql,
        op_kwargs={
            "date_traitement": "{{ dag_run.conf.date_traitement }}"
        }
    )

    # ===== TÂCHE DE TRANSFORMATION CRUCIALE =====
    # Cette tâche est essentielle car elle :
    # 1. Nettoie et standardise les données
    # 2. Applique les règles métier
    # 3. Crée les agrégations nécessaires
    # 4. Prépare les données pour le chargement
    transform = PythonOperator(
        task_id="TRANSFORM",
        python_callable=transform_data,
        trigger_rule=TriggerRule.ALL_SUCCESS  # S'exécute uniquement si l'extraction a réussi
    )

    # ===== TÂCHE DE CHARGEMENT CRUCIALE =====
    # Cette tâche est cruciale car elle :
    # 1. Charge les données transformées dans la couche Silver
    # 2. Vérifie la cohérence des données chargées
    # 3. Met à jour les métadonnées
    load = PythonOperator(
        task_id="LOAD",
        python_callable=loading_data,
        trigger_rule=TriggerRule.ALL_SUCCESS  # S'exécute uniquement si la transformation a réussi
    )

    # Tâche : Fin du pipeline
    end = PythonOperator(
        task_id="END",
        python_callable=end_message,
        trigger_rule=TriggerRule.ALL_SUCCESS  # S'exécute uniquement si le chargement a réussi
    )

    # ===== TÂCHE DE DÉCLENCHEMENT DU DAG GOLD =====
    # Cette tâche déclenche le DAG Gold une fois que le pipeline Silver est terminé
    trigger_gold = TriggerDagRunOperator(
        task_id="TRIGGER_GOLD_DAG",
        trigger_dag_id="Projet_E_commerce_ETL_Gold",
        trigger_rule=TriggerRule.ALL_SUCCESS,  # S'exécute uniquement si toutes les tâches précédentes ont réussi
        doc_md='''
        # Déclenchement du DAG Gold
        
        Cette tâche déclenche automatiquement le DAG Gold une fois que le traitement Silver est terminé.
        Le DAG Gold vérifiera ensuite l'existence des vues nécessaires.
        '''
    )

    # ===== DÉFINITION DES DÉPENDANCES =====
    # Le flux d'exécution est le suivant :
    # 1. Attente du signal Bronze
    # 2. Début du pipeline
    # 3. Extraction des données depuis Bronze
    # 4. Transformation des données
    # 5. Chargement dans Silver
    # 6. Fin du pipeline
    # 7. Déclenchement du DAG Gold
    attente_signal_bronze >> start >> extract >> transform >> load >> end >> trigger_gold 