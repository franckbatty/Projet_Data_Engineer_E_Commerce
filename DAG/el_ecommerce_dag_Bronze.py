## == Pipeline ETL pour le projet E-commerce couche Bronze avec Airflow et Python == ##
# Ce script définit un DAG (Directed Acyclic Graph) pour exécuter un pipeline EL (Extract, Load) pour le projet E-commerce.

# Importation des modules nécessaires
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # Importation de TriggerDagRunOperator pour déclencher d'autres DAGs
from airflow.utils.trigger_rule import TriggerRule # Importation de TriggerRule pour définir les règles de déclenchement
from datetime import datetime, timedelta
import sys

# Ajout du chemin vers le fichier ETL
sys.path.append('/home/franck/airflow/Script_ETL_Tous_PROJETS/e_commerce')

# Importation des fonctions ETL
try:
    from el_e_commerce_Bronze import start_message, extract_data, loading_data, end_message
except ModuleNotFoundError as e:
    raise ModuleNotFoundError(f"Erreur lors de l'importation des modules ETL : {e}")

# ===== PARAMÈTRES CRUCIAUX DU DAG =====
# Ces paramètres définissent le comportement de base du DAG
# - depends_on_past=False : Permet l'exécution indépendante des runs, c'est-à-dire que chaque exécution du DAG ne dépend pas des exécutions passées d'autres runs.
# - retries=1 : Nombre de tentatives en cas d'échec
# - retry_delay=5 minutes : Délai entre les tentatives
default_args = {
    "owner": "franck_BATTY",
    "depends_on_past": False, 
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Définition du DAG Bronze
with DAG(
    "Projet_E_commerce_ETL_Bronze",
    default_args=default_args,
    description="DAG Bronze pour le pipeline ETL du projet E-commerce",
    schedule_interval="@daily",  # Exécution quotidienne
    start_date=datetime(2025, 6, 9),
    catchup=False,  # Permet de ne pas rattraper les exécutions manquées c'est-à-dire 
    # que le DAG ne s'exécutera qu'à partir de la date de début spécifiée.
    tags=['Bronze', 'e-commerce', 'EL']   # Tags pour faciliter la recherche
) as dag:

    # ===== TÂCHES DU PIPELINE BRONZE =====
    
    # Tâche : Début du pipeline
    start = PythonOperator(
        task_id="START",
        python_callable=start_message
    )

    # ===== TÂCHE D'EXTRACTION CRUCIALE =====
    # Cette tâche est fondamentale car elle :
    # 1. Lit tous les fichiers source CSV
    # 2. Vérifie leur intégrité
    # 3. Prépare les données pour le chargement
    
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

    # Tâche : Chargement des données
    # Cette tâche charge les données extraites dans la couche Bronze
    load = PythonOperator(
        task_id="LOAD",
        python_callable=loading_data
    )

    # Tâche : Fin du pipeline
    end = PythonOperator(
        task_id="END",
        python_callable=end_message
    )

    # ===== TÂCHES DE SYNCHRONISATION AVEC SILVER =====
    
    # ===== TÂCHE DE DÉCLENCHEMENT DE SILVER =====
    # Cette tâche est cruciale pour l'enchaînement automatique :
    # 1. Elle lance automatiquement le DAG Silver
    # 2. Elle passe des métadonnées importantes (date de traitement)
    # 3. Elle permet un traitement continu des données
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="Projet_E_commerce_ETL_Silver",
        conf={  
            "source": "bronze",
            "date_traitement": "{{ ds }}",
            "bronze_termine": True  # Signal de fin du DAG Bronze
        },
        wait_for_completion=False,  # Ne pas attendre la fin de Silver
        reset_dag_run=True,  # Réinitialiser le DAG Silver s'il existe déjà
        trigger_rule=TriggerRule.ALL_SUCCESS  # S'exécute uniquement si toutes les tâches précédentes ont réussi
    )

    # ===== DÉFINITION DES DÉPENDANCES =====
    # Le flux d'exécution est le suivant :
    # 1. Début du pipeline
    # 2. Extraction des données (étape critique)
    # 3. Chargement dans Bronze
    # 4. Fin du pipeline
    # 5. Déclenchement de Silver (enchaînement)
    start >> extract >> load >> end >> trigger_silver 