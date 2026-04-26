# == ETL E-COMMERCE COUCHE BRONZE ==
"""
Cette couche bronze est la première étape du pipeline ETL pour le projet E-commerce.
Elle importe les données brutes (CSV) et les stocke dans PostgreSQL sans transformation.
"""

# === Importation des bibliothèques nécessaires ===
import psycopg2              # Connexion et requêtes PostgreSQL
import os                    # Gestion des chemins et fichiers
import pandas as pd          # Manipulation des données tabulaires
import sys                   # Gestion du PYTHONPATH
from datetime import datetime # Gestion des dates
from dotenv import load_dotenv
# Charger les variables depuis le fichier .env
load_dotenv()

# ✅ Ajout du chemin du package ETL
sys.path.append("/opt/airflow/ETL")

# ✅ Chemins des fichiers CSV (montés via docker-compose.yml)
clients_file = "/opt/airflow/data/clients.csv"
commandes_file = "/opt/airflow/data/commandes.csv"
details_commandes_file = "/opt/airflow/data/details_commandes.csv"
paiements_file = "/opt/airflow/data/paiements.csv"
produits_file = "/opt/airflow/data/produits.csv"

# ✅ Paramètres de connexion centralisés
DB_PARAMS = {
    "dbname": os.getenv("POSTGRES_DB"),       # Base définie dans .env
    "user": os.getenv("POSTGRES_USER"),       # Utilisateur défini dans .env
    "password": os.getenv("POSTGRES_PASSWORD"), # Mot de passe défini dans .env
    "host": os.getenv("POSTGRES_HOST"),       # Host défini dans .env (service Docker)
    "port": os.getenv("POSTGRES_PORT")        # Port défini dans .env
}

# === Fonctions utilitaires ===
def start_message():
    print("=== DÉBUT DU DAG ===")

def end_message():
    print("=== FIN DU DAG ===")

# === Fonction EXTRACT ===
def extract_data(ti, **kwargs):
    """
    Extraction des données à partir des fichiers CSV.
    Vérifie l'existence des fichiers et extrait les données dans des DataFrames pandas.
    """
    try:
        print("Début d'extraction des fichiers CSV...")

        # Vérifier que les fichiers existent
        for file in [clients_file, commandes_file, details_commandes_file, paiements_file, produits_file]:
            if not os.path.exists(file):
                raise FileNotFoundError(f"Le fichier {file} est introuvable.")

        # Charger les fichiers CSV
        clients_df = pd.read_csv(clients_file)
        commandes_df = pd.read_csv(commandes_file)
        details_commandes_df = pd.read_csv(details_commandes_file)
        paiements_df = pd.read_csv(paiements_file)
        produits_df = pd.read_csv(produits_file)

        print(f"Clients : {clients_df.shape}, Commandes : {commandes_df.shape}, "
              f"Détails Commandes : {details_commandes_df.shape}, "
              f"Paiements : {paiements_df.shape}, Produits : {produits_df.shape}")

        # Stocker dans XCom
        extracted_data = {
            "clients": clients_df.to_dict(),
            "commandes": commandes_df.to_dict(),
            "details_commandes": details_commandes_df.to_dict(),
            "paiements": paiements_df.to_dict(),
            "produits": produits_df.to_dict()
        }
        ti.xcom_push(key="extracted_data", value=extracted_data)

        print("✅ Extraction réussie.")
    except Exception as e:
        print(f"❌ Erreur lors de l'extraction : {str(e)}")
        raise e

# === Fonction LOAD ===
def loading_data(ti):
    print("Début du chargement dans PostgreSQL...")

    # Récupérer les données extraites
    extracted_data = ti.xcom_pull(task_ids="EXTRACT", key="extracted_data")

    # Créer des DataFrames
    clients_df = pd.DataFrame(extracted_data["clients"])
    commandes_df = pd.DataFrame(extracted_data["commandes"])
    details_commandes_df = pd.DataFrame(extracted_data["details_commandes"])
    paiements_df = pd.DataFrame(extracted_data["paiements"])
    produits_df = pd.DataFrame(extracted_data["produits"])

    # Conversion en tuples
    clients_data = [tuple(row) for row in clients_df.itertuples(index=False, name=None)]
    commandes_data = [tuple(row) for row in commandes_df.itertuples(index=False, name=None)]
    details_data = [tuple(row) for row in details_commandes_df.itertuples(index=False, name=None)]
    paiements_data = [tuple(row) for row in paiements_df.itertuples(index=False, name=None)]
    produits_data = [tuple(row) for row in produits_df.itertuples(index=False, name=None)]

    # Connexion PostgreSQL
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    print("Connexion PostgreSQL réussie.")

    try:
        # Vidage des tables
        for table in [
            "e_commerce_couche_bronze.paiements",
            "e_commerce_couche_bronze.details_commandes",
            "e_commerce_couche_bronze.commandes",
            "e_commerce_couche_bronze.produits",
            "e_commerce_couche_bronze.clients"
        ]:
            cursor.execute(f"TRUNCATE TABLE {table} CASCADE;")
            print(f"Table {table} vidée.")
        conn.commit()

        # Insertion ordonnée
        cursor.executemany("""
            INSERT INTO e_commerce_couche_bronze.clients 
            (id_client, nom, prenom, gender, email, telephone, adresse)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, clients_data)
        conn.commit()

        cursor.executemany("""
            INSERT INTO e_commerce_couche_bronze.commandes 
            (id_commande, id_client, date_commande, statut)
            VALUES (%s, %s, %s, %s);
        """, commandes_data)
        conn.commit()

        cursor.executemany("""
            INSERT INTO e_commerce_couche_bronze.produits 
            (id_produit, nom, categorie, prix, quantite_stock)
            VALUES (%s, %s, %s, %s, %s);
        """, produits_data)
        conn.commit()

        cursor.executemany("""
            INSERT INTO e_commerce_couche_bronze.details_commandes 
            (id_detail_commande, id_commande, id_produit, quantite, prix_total)
            VALUES (%s, %s, %s, %s, %s);
        """, details_data)
        conn.commit()

        cursor.executemany("""
            INSERT INTO e_commerce_couche_bronze.paiements 
            (id_paiement, id_commande, methode_paiement, montant, date_paiement)
            VALUES (%s, %s, %s, %s, %s);
        """, paiements_data)
        conn.commit()

        print("✅ Chargement terminé avec succès.")
    except Exception as e:
        print(f"❌ Erreur lors du chargement : {str(e)}")
        conn.rollback() 
        raise e
    finally:
        cursor.close()
        conn.close()
        print("Connexion PostgreSQL fermée.")
