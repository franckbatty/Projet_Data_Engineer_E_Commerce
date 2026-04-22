## == ETL E-COMMERCE COUCHE SILVER ==
''' Cette couche Silver du projet E-commerce est dédiée à l'extraction, la transformation et
    le chargement des données dans une base de données PostgreSQL.
    Elle permet de nettoyer et transformer les données brutes de la couche Bronze pour les rendre
    exploitables pour l'analyse.
'''

# Importation des bibliothèques nécessaires
import psycopg2  
import pandas as pd
from datetime import datetime

# === Fonction pour afficher un message de début ===
def start_message(): 
    print("=== DÉBUT DU DAG ===")

# === Fonction pour afficher un message de fin ===
def end_message():
    print("=== FIN DU DAG ===") 

# === Fonction EXTRACT === 
def extract_data_postgresql(ti):
    """
    Extraction des données brutes depuis PostgreSQL.
    Récupère les données de la couche Bronze pour les transformer.
    """
    try:
        print("Début d'extraction des données brutes depuis PostgreSQL.")

        # Paramètres de connexion
        connection_params = {
            "dbname": "postgres",
            "user": "postgres",
            "password": "frenecker",
            "host": "airflow_db",
            "port": 5432,
        }
        conn = psycopg2.connect(**connection_params)

        # Requêtes SQL pour récupérer les tables brutes
        Base_deonnees_PostgreSQL = {
            "clients": "SELECT * FROM e_commerce_couche_bronze.clients;",
            "commandes": "SELECT * FROM e_commerce_couche_bronze.commandes;",
            "details_commandes": "SELECT * FROM e_commerce_couche_bronze.details_commandes;",
            "paiements": "SELECT * FROM e_commerce_couche_bronze.paiements;",
            "produits": "SELECT * FROM e_commerce_couche_bronze.produits;",
        }

        extracted_data = {}
        for key, query in Base_deonnees_PostgreSQL.items(): 
            # Exécuter la requête avec pandas 
            df = pd.read_sql_query(query, conn)
            extracted_data[key] = df.to_dict(orient="records")
            
        # Envoyer les données extraites dans XCom
        ti.xcom_push(key="extracted_data", value=extracted_data)
        print("Extraction réussie.")

    except Exception as e:
        print(f"Erreur lors de l'extraction : {str(e)}")
        raise e
 
    finally:
            conn.close()
            print("Connexion à PostgreSQL fermée.")

# === Fonction TRANSFORM ===
def transform_data(ti):
    """
    Transformation des données extraites et validation des relations avec les clés étrangères.
    Nettoyage des doublons et des valeurs manquantes pour garantir des données exploitables et traitement
    des données.
    """ 
    try:
        print("🔄 Début de la transformation des données...")

        # Récupération des données extraites depuis XCom
        extracted_data = ti.xcom_pull(key="extracted_data", task_ids="EXTRACT")

        # Création des DataFrames à partir des données extraites
        clients = pd.DataFrame(extracted_data["clients"])
        commandes = pd.DataFrame(extracted_data["commandes"])
        details_commandes = pd.DataFrame(extracted_data["details_commandes"])
        paiements = pd.DataFrame(extracted_data["paiements"])
        produits = pd.DataFrame(extracted_data["produits"])

        dataframes = {
            "clients": clients,
            "commandes": commandes,
            "details_commandes": details_commandes,
            "paiements": paiements,
            "produits": produits
        }

        # Afficher les tailles initiales pour le suivi
        for key, df in dataframes.items():
            print(f"📊 Avant nettoyage {key}: {df.shape}")

        # === TRANSFORMATION DES DONNÉES ===
        # 1. Normalisation des genres
        clients['gender'] = clients['gender'].apply(lambda x: x if x in ['Female', 'Male'] else 'Autres')

        # 2. Traduction des statuts de commande
        commandes['statut'] = commandes['statut'].map({
            'cancelled': 'annulee',
            'shipped': 'expediee',
            'pending': 'en attente',
            'processing': 'en cours de traitement',
            'delivered': 'livree'
        })  
        
        # 3. Formatage des dates
        commandes["date_commande"] = pd.to_datetime(commandes["date_commande"]).dt.strftime('%Y-%m-%d %H:%M:%S')
        # cette ligne assure que la date est au format standard pour PostgreSQL

        # 4. Simplification des méthodes de paiement
        paiements['methode_paiement'] = paiements['methode_paiement'].apply(lambda x: 'cash' if x == 'cash' else 'mobile money')
        
        # 5. Filtrage et traduction des catégories de produits
        produits = produits[produits['categorie'].isin(['Kitchen', 'Home'])]
        produits["categorie"] = produits["categorie"].map({
            'Kitchen': 'Cuisine',
            'Home': 'Maison'
        })

        # === VALIDATION DES CLÉS ÉTRANGÈRES ===
        # IMPORTANT: L'ordre de validation est crucial pour maintenir l'intégrité des données
        print("🔍 Validation des clés étrangères...")

        # 1. Clients avec commandes valides
        clients_valides = clients[clients['id_client'].isin(commandes['id_client'])]

        # 2. Commandes avec clients valides
        commandes_valides = commandes[commandes['id_client'].isin(clients_valides['id_client'])] 

        # 3. Produits présents dans les détails de commande
        produits_valides = produits[produits['id_produit'].isin(details_commandes['id_produit'])]

        # 4. Détails de commande avec commandes et produits valides
        details_commandes_valides = details_commandes[
            details_commandes['id_commande'].isin(commandes_valides['id_commande']) &
            details_commandes['id_produit'].isin(produits_valides['id_produit'])
        ]

        # 5. Paiements avec commandes valides
        paiements_valides = paiements[paiements['id_commande'].isin(commandes_valides['id_commande'])]

        # Préparation des données pour XCom
        dataframes_valides = {
            "clients": clients_valides, 
            "commandes": commandes_valides,
            "details_commandes": details_commandes_valides,
            "paiements": paiements_valides,
            "produits": produits_valides
        }

        # Affichage des statistiques de transformation
        print("\n📊 Résumé de la transformation:")
        for nom, df in dataframes_valides.items():
            original_shape = dataframes[nom].shape[0]
            print(f"📉 {nom}: {original_shape} → {df.shape[0]} lignes")

        # Envoi des données transformées dans XCom
        ti.xcom_push(key="transformed_data", value={
            "clients": dataframes_valides["clients"].to_dict(),
            "commandes": dataframes_valides["commandes"].to_dict(),
            "details_commandes": dataframes_valides["details_commandes"].to_dict(),
            "paiements": dataframes_valides["paiements"].to_dict(),
            "produits": dataframes_valides["produits"].to_dict()
        })

        print("✅ Transformation terminée avec succès.")

    except Exception as e:
        print(f"❌ Erreur lors de la transformation : {str(e)}")
        raise e

# === Fonction LOAD ===
def loading_data(ti):
    """
    Chargement des données transformées dans la base PostgreSQL.
    Utilise ON CONFLICT DO NOTHING pour gérer les doublons et maintenir l'intégrité des données.
    """
    print("Début du chargement dans PostgreSQL.")

    # Récupérer les données transformées depuis XCom
    transformed_data = ti.xcom_pull(task_ids="TRANSFORM", key="transformed_data")

    # Créer des DataFrames pour chaque table
    clients_df = pd.DataFrame(transformed_data["clients"]) 
    commandes_df = pd.DataFrame(transformed_data["commandes"])
    details_commandes_df = pd.DataFrame(transformed_data["details_commandes"])
    paiements_df = pd.DataFrame(transformed_data["paiements"])
    produits_df = pd.DataFrame(transformed_data["produits"])

    # Conversion en tuples pour l'insertion PostgreSQL
    clients_data_to_insert = [tuple(row) for row in clients_df.itertuples(index=False, name=None)]
    commandes_data_to_insert = [tuple(row) for row in commandes_df.itertuples(index=False, name=None)]
    details_commandes_data_to_insert = [tuple(row) for row in details_commandes_df.itertuples(index=False, name=None)]
    paiements_data_to_insert = [tuple(row) for row in paiements_df.itertuples(index=False, name=None)]
    produits_data_to_insert = [tuple(row) for row in produits_df.itertuples(index=False, name=None)]

    # Connexion à PostgreSQL
    connection_params = {
    "dbname": "postgres",       # même base que Bronze
    "user": "postgres",         # défini dans docker-compose.yml
    "password": "frenecker",    # ton mot de passe
    "host": "airflow_db",       # nom du service Docker
    "port": 5432                # port interne du conteneur
    }
 
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    print("Connexion à PostgreSQL réussie.")

    try:
        # === INSERTION DES DONNÉES ===
        # IMPORTANT: L'ordre d'insertion respecte les contraintes de clés étrangères
        print("\nInsertion des données transformées...")

        # 1. Clients
        print("Insertion dans la table clients...")
        cursor.executemany("""
            INSERT INTO e_commerce_couche_silver.clients 
            (id_client, nom, prenom, gender, email, telephone, adresse)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_client) DO NOTHING;
        """, clients_data_to_insert)  
        conn.commit()

        # 2. Produits
        print("Insertion dans la table produits...")
        cursor.executemany("""
            INSERT INTO e_commerce_couche_silver.produits 
            (id_produit, nom, categorie, prix, quantite_stock)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id_produit) DO NOTHING;
        """, produits_data_to_insert) 
        conn.commit()

        # 3. Commandes
        print("Insertion dans la table commandes...")
        cursor.executemany("""
            INSERT INTO e_commerce_couche_silver.commandes 
            (id_commande, id_client, date_commande, statut)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id_commande) DO NOTHING;
        """, commandes_data_to_insert)
        conn.commit()

        # 4. Détails des commandes
        print("Insertion dans la table details_commandes...")
        cursor.executemany("""
            INSERT INTO e_commerce_couche_silver.details_commandes 
            (id_detail_commande, id_commande, id_produit, quantite, prix_total)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id_detail_commande) DO NOTHING;
        """, details_commandes_data_to_insert)  
        conn.commit()

        # 5. Paiements
        print("Insertion dans la table paiements...")
        cursor.executemany("""
            INSERT INTO e_commerce_couche_silver.paiements 
            (id_paiement, id_commande, methode_paiement, montant, date_paiement)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id_paiement) DO NOTHING;
        """, paiements_data_to_insert) 
        conn.commit()

        print("✅ Chargement des données terminé avec succès.")

    except Exception as e:
        print(f"❌ Erreur lors du chargement : {str(e)}")
        conn.rollback() 
        raise e

    finally:
        # Fermeture de la connexion
            cursor.close()
            print("Fermeture du curseur.")
            conn.close()
            print("Connexion à PostgreSQL fermée.") 