import psycopg2

# === Fonction pour afficher un message de début ===
def start_message(): 
    print("=== DÉBUT DU DAG ===")

# === Fonction pour afficher un message de fin ===
def end_message():
    print("=== FIN DU DAG ===")

def get_db_connection():
    """Établit une connexion à la base de données PostgreSQL."""
    try:
        print("Tentative de connexion à la base de données PostgreSQL...")
        conn = psycopg2.connect(
            host="*******",
            database="*******",
            user="postgres",
            password="*******",
            port=5432
        )
        print("Connexion à la base de données établie avec succès")
        
        # Vérification de la connexion avec les tables de vues
        cur = conn.cursor()
        cur.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.views 
            WHERE table_schema = 'e_commerce_couche_gold'
        """)
        vues = cur.fetchall() # Récupération des vues dans le schéma Gold.
        # La methode fetchall() retourne une liste contenant toutes les lignes du résultat de la requête.
        #  Chaque ligne est généralement représentée sous forme de tuple.
        print(f"Connexion établie avec {len(vues)} vues dans le schéma Gold")
        for schema, vue in vues:
            print(f"Vue accessible : {schema}.{vue}")
        
        return conn
    except Exception as e:
        print(f"Erreur de connexion à la base de données : {e}")
        raise

def get_view_size(cur, view_name):
    """Récupère la taille d'une vue en nombre de lignes."""
    try:
        cur.execute(f"SELECT COUNT(*) FROM e_commerce_couche_gold.{view_name}")
        count = cur.fetchone()[0] # Récupération du nombre de lignes dans la vue.
        return count
    except Exception as e:
        print(f"Erreur lors de la récupération de la taille de la vue {view_name} : {e}")
        return 0

def verify_gold_views():
    """Vérifie l'existence des vues dans la couche Gold."""
    try:
        print("Début de la vérification des vues Gold...")
        conn = get_db_connection()
        cur = conn.cursor()
        
        vues_a_verifier = [
            'dim_client',                # Vue dimension client
            'dim_produit',               # Vue dimension produit
            'dim_commande',              # Vue dimension commande
            'fait_ventes_aggregees'      # Vue de faits agrégés des ventes
        ]
        
        print(f"Vérification de {len(vues_a_verifier)} vues dans le schéma Gold")
        print("\n=== Taille des vues Gold ===")
        
        for vue in vues_a_verifier:
            print(f"\nVérification de la vue : {vue}")
            cur.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.views 
                    WHERE table_schema = 'e_commerce_couche_gold' 
                    AND table_name = '{vue}'
                ); 
            """) # elle va retourner True si la vue existe, sinon False.
            existe = cur.fetchone()[0]
            if not existe:
                error_msg = f"La vue {vue} est manquante dans la couche Gold."
                print(f"ERREUR : {error_msg}")
                raise Exception(error_msg) # Lève une exception si la vue n'existe pas.
            print(f"Vue {vue} trouvée avec succès")
            
            # Récupération et affichage de la taille de la vue
            taille = get_view_size(cur, vue)
            print(f"Taille de la vue {vue} : {taille:,} lignes")
        
        conn.close()
        print("\n=== Résumé ===")
        print("Toutes les vues Gold ont été vérifiées avec succès")
        print("Les tailles des vues ont été affichées ci-dessus")
        
    except Exception as e:
        print(f"Erreur lors de la vérification des vues Gold : {e}")
        raise 