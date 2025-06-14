## == ETL E-COMMERCE COUCHE BRONZE ==
''' Cette couche bronze est la première étape de mon pipeline ETL pour le projet E-commerce.
    Car, elle permet d'importer les données brutes et de les stocker dans la base de données PostgreSQL.
    Ici il consite à importer ces données brutes et les charger sans transformation dans la base de données 
    PostgreSQL.
    Cette couche est essentielle pour garantir l'intégrité des données et servir de base pour les étapes 
    de transformation ultérieures.
''' 

# Importation des bibliothèques nécessaires
import psycopg2  
import os # Bibliothèque os pour la gestion des fichiers
import pandas as pd # Bibliothèque pandas pour la manipulation des données 
import sys # Bibliothèque sys pour la gestion des chemins d'importation 
from datetime import datetime # Pour la gestion des dates
 
# Ajouter dynamiquement le chemin des base de données CSV 
sys.path.append('/home/franck/airflow/Data_Tous_Projets/e_commerce')  

# === Chemins des fichiers CSV === 
clients_file = "home/franck/airflow/Data_Tous_Projets/e_commerce/clients.csv"
commandes_file = "home/franck/airflow/Data_Tous_Projets/e_commerce/commandes.csv"
details_commandes_file = "home/franck/airflow/Data_Tous_Projets/e_commerce/details_commandes.csv"
paiements_file = "home/franck/airflow/Data_Tous_Projets/e_commerce/paiements.csv"
produits_file = "home/franck/airflow/Data_Tous_Projets/e_commerce/produits.csv"

# === Fonction pour afficher le message de début === 
def start_message():
    print("=== DÉBUT DU DAG ===")

# === Fonction pour afficher le message de fin ===
def end_message():
    print("=== FIN DU DAG ===") 

# === Fonction EXTRACT ===
def extract_data(ti, **kwargs):
    # **kwargs permet de passer des arguments dynamiques à la fonction c'est-à-dire les chemins des fichiers CSV.
    """
    Extraction des données à partir des fichiers CSV.
    Vérifie l'existence des fichiers et extrait les données dans des DataFrames pandas.
    """
    try: 
        print("Début d'extraction des fichiers.")

        # Récupérer les chemins des fichiers CSV depuis kwargs
        clients_file = kwargs.get("clients_file")
        commandes_file = kwargs.get("commandes_file")
        details_commandes_file = kwargs.get("details_commandes_file")
        paiements_file = kwargs.get("paiements_file")
        produits_file = kwargs.get("produits_file")

        # Vérifier que les fichiers existent 
        for file in [clients_file, commandes_file, details_commandes_file, paiements_file, produits_file]:
            if not file or not os.path.exists(file):
                raise FileNotFoundError(f"Le fichier {file} est introuvable.")

        # Charger les fichiers CSV dans des DataFrames pandas
        # IMPORTANT: Les données sont chargées brutes, sans aucun traitement
        clients_df = pd.read_csv(clients_file)
        commandes_df = pd.read_csv(commandes_file)
        details_commandes_df = pd.read_csv(details_commandes_file)
        paiements_df = pd.read_csv(paiements_file)
        produits_df = pd.read_csv(produits_file)

        # Afficher un aperçu rapide des dimensions des fichiers
        print(f"Clients : {clients_df.shape}, Commandes : {commandes_df.shape}, Détails Commandes : {details_commandes_df.shape}, Paiements : {paiements_df.shape}, Produits : {produits_df.shape}")

        # Stocker les données dans un dictionnaire pour les envoyer dans XCom
        extracted_data = {
            "clients": clients_df.to_dict(),
            "commandes": commandes_df.to_dict(),
            "details_commandes": details_commandes_df.to_dict(),
            "paiements": paiements_df.to_dict(),
            "produits": produits_df.to_dict()
        }
        ti.xcom_push(key="extracted_data", value=extracted_data)

        print("Extraction réussie.")
    except Exception as e:
        print(f"Erreur lors de l'extraction : {str(e)}")
        raise e

# === Fonction LOAD === 
def loading_data(ti): 
    print("Début du chargement dans PostgreSQL.")

    # Récupérer les données transformées depuis XCom
    extracted_data = ti.xcom_pull(task_ids="EXTRACT", key="extracted_data")

    # Créer des DataFrames pour chaque table
    clients_df = pd.DataFrame(extracted_data["clients"]) 
    commandes_df = pd.DataFrame(extracted_data["commandes"])
    details_commandes_df = pd.DataFrame(extracted_data["details_commandes"])
    paiements_df = pd.DataFrame(extracted_data["paiements"])
    produits_df = pd.DataFrame(extracted_data["produits"])

    # PostgreSQL ne reconnait pas les tables en format dataframe, donc il faut les convertir 
    # en liste de tuples ou en dictionnaire 
    # Preparer les tuples pour l'insertion dans PostgreSQL
    clients_data_to_insert = [tuple(row) for row in clients_df.itertuples(index=False, name=None)]
    commandes_data_to_insert = [tuple(row) for row in commandes_df.itertuples(index=False, name=None)]
    details_commandes_data_to_insert = [tuple(row) for row in details_commandes_df.itertuples(index=False, name=None)]
    paiements_data_to_insert = [tuple(row) for row in paiements_df.itertuples(index=False, name=None)]
    produits_data_to_insert = [tuple(row) for row in produits_df.itertuples(index=False, name=None)]

    '''
    Un tuple est une structure de données immuable en Python, similaire à une liste, mais avec des parenthèses
    au lieu de crochets.
    La méthode itertuples() est utilisée pour parcourir les lignes d'un DataFrame et les convertir en tuples.
    '''
    # immuable signifie que les tuples ne peuvent pas être modifiés une fois créés, contrairement aux listes.
    
 
    # Connexion à PostgreSQL
    connection_params = {
        "dbname": "********",  
        "user": "postgres", 
        "password": "********",  
        "host": "********",        
        "port": 5432               
    } 
    conn = psycopg2.connect(**connection_params) # Connexion à la base de données PostgreSQL
    cursor = conn.cursor() # Créer un curseur pour exécuter des requêtes SQL
    print("Connexion à PostgreSQL réussie.")

    try:
        # ===== VIDAGE DES TABLES =====
        # IMPORTANT: On vide les tables avant chaque nouveau chargement pour éviter les doublons
        # L'ordre est important à cause des clés étrangères (paiements -> details_commandes -> commandes -> produits -> clients)
        print("Vidage des tables existantes...")
        tables_to_clear = [
            "e_commerce_couche_bronze.paiements",
            "e_commerce_couche_bronze.details_commandes",
            "e_commerce_couche_bronze.commandes",
            "e_commerce_couche_bronze.produits",
            "e_commerce_couche_bronze.clients"
        ]
        
        for table in tables_to_clear:
            cursor.execute(f"TRUNCATE TABLE {table} CASCADE;")
            print(f"Table {table} vidée.")
        
        conn.commit()
        print("Toutes les tables ont été vidées.")

        # Charger les données dans chaque table avec insertion par batch

        # Insertion de la premiere table clients
        print("Insertion dans la table clients...")
        # Utiliser executemany pour insérer plusieurs lignes à la fois
        cursor.executemany("""
            INSERT INTO e_commerce_couche_bronze.clients (id_client, nom, prenom, gender, email, telephone, adresse)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, clients_data_to_insert)  
        conn.commit() 
        # Fermer la base de données après chaque insertion
        # Le nom du shema et de la table sont important pour éviter les erreurs de nom de table.
        # Schema est comme un carton dans lequel on trouve plusieur tables. 
        # Ici le schema est "e_commerce_couche_bronze" et la table est "clients".
        

        # Insertion de la deuxieme table commandes
        print("Insertion dans la table commandes...")
        cursor.executemany("""
            INSERT INTO e_commerce_couche_bronze.commandes (id_commande, id_client, date_commande, statut)
            VALUES (%s, %s, %s, %s);
        """, commandes_data_to_insert)
        conn.commit() # Fermer la transaction après chaque insertion

        # Insertion de la quatrieme table produits
        print("Insertion dans la table produits...")
        cursor.executemany("""
            INSERT INTO e_commerce_couche_bronze.produits (id_produit, nom, categorie, prix, quantite_stock)
            VALUES (%s, %s, %s, %s, %s);
        """, produits_data_to_insert) 
        conn.commit() 
        
        # Insertion de la troisieme table details_commandes
        print("Insertion dans la table details_commandes...")
        cursor.executemany("""
            INSERT INTO e_commerce_couche_bronze.details_commandes (id_detail_commande, id_commande, id_produit, quantite, prix_total)
            VALUES (%s, %s, %s, %s, %s);
        """, details_commandes_data_to_insert)  
        conn.commit() 

        # Insertion de la cinquieme table paiements
        print("Insertion dans la table paiements...")
        cursor.executemany("""
            INSERT INTO e_commerce_couche_bronze.paiements (id_paiement, id_commande, methode_paiement, montant, date_paiement)
            VALUES (%s, %s, %s, %s, %s);
        """, paiements_data_to_insert)  
        conn.commit() 

        # Affichage des logs après insertion
        print(f"Lignes insérées : clients {len(clients_data_to_insert)}, commandes {len(commandes_data_to_insert)}, détails_commandes {len(details_commandes_data_to_insert)}, paiements {len(paiements_data_to_insert)}, produits {len(produits_data_to_insert)}")
       
        # Message de fin de chargement
        print("Chargement terminé avec succès.")
    # Gestion des erreurs
    except Exception as e:
        print(f"Erreur lors du chargement : {str(e)}")
        conn.rollback() # Annuler la transaction en cas d'erreur
        raise e
    finally:
        # Fermer la connexion
        cursor.close() # Fermer le curseur que j'avais ouvert pour exécuter les requêtes SQL
        conn.close() # Fermer la connexion à la base de données que j'avais ouverte 
        print("Chargement terminé et connexion PostgreSQL fermée.") 