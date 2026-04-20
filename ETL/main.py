# ==========================================
# ETL PIPELINE - BRONZE / SILVER / GOLD
# VERSION CORRIGÉE + COMMENTÉE
# ==========================================


# ==========================
# IMPORT BRONZE LAYER
# ==========================
# On importe les fonctions responsables de la couche Bronze
# (ingestion des données brutes depuis CSV vers PostgreSQL)

from el_e_commerce_Bronze import (
    start_message as bronze_start,   # message de début
    extract_data,                    # extraction des données CSV
    loading_data,                   # chargement vers PostgreSQL (Bronze)
    end_message as bronze_end       # message de fin
)


# ==========================
# IMPORT SILVER LAYER
# ==========================
# Couche Silver = nettoyage + transformation des données Bronze

from etl_e_commerce_Silver import (
    start_message as silver_start,        # message début Silver
    extract_data_postgresql,             # lecture depuis Bronze (DB)
    transform_data,                      # nettoyage / transformation
    loading_data as silver_load,         # chargement vers Silver
    end_message as silver_end            # message fin Silver
)


# ==========================
# IMPORT GOLD LAYER
# ==========================
# Couche Gold = données prêtes pour analyse (BI / KPI)

from etl_e_commerce_Gold import (
    start_message as gold_start,     # début Gold
    verify_gold_views,              # vérification des vues SQL
    end_message as gold_end         # fin Gold
)


# ==========================================
# BRONZE LAYER
# ==========================================
def run_bronze():
    """
    Étape 1 du pipeline :
    - lecture des données brutes
    - chargement dans PostgreSQL (Bronze)
    """

    print("\n🔵 ===== BRONZE LAYER =====")

    # message de démarrage
    bronze_start()

    # extraction des données CSV (brutes)
    print("📥 Extraction des données CSV...")
    extract_data()

    # chargement vers base Bronze
    print("💾 Chargement vers PostgreSQL Bronze...")
    loading_data()

    # message de fin
    bronze_end()


# ==========================================
# SILVER LAYER
# ==========================================
def run_silver():
    """
    Étape 2 du pipeline :
    - lecture des données Bronze
    - nettoyage / transformation
    - chargement vers Silver
    """

    print("\n⚪ ===== SILVER LAYER =====")

    silver_start()

    # lecture depuis Bronze (PostgreSQL)
    print("📥 Extraction depuis Bronze...")
    extract_data_postgresql()

    # transformation des données
    print("🔄 Transformation des données...")
    transform_data()

    # chargement vers Silver
    print("💾 Chargement vers Silver...")
    silver_load()

    silver_end()


# ==========================================
# GOLD LAYER
# ==========================================
def run_gold():
    """
    Étape 3 du pipeline :
    - validation des vues SQL
    - données prêtes pour BI / reporting
    """

    print("\n🟡 ===== GOLD LAYER =====")

    gold_start()

    # vérification des vues analytiques
    print("🔍 Vérification des vues Gold...")
    verify_gold_views()

    gold_end()


# ==========================================
# PIPELINE GLOBAL (TEST LOCAL)
# ==========================================
def main():
    """
    Orchestration locale du pipeline.
    ⚠️ En production Airflow remplacera cette fonction.
    """

    print("\n===================================")
    print("🚀 PIPELINE E-COMMERCE ETL START")
    print("===================================\n")

    try:
        # exécution séquentielle des 3 couches
        run_bronze()
        run_silver()
        run_gold()

        print("\n🎉 PIPELINE TERMINÉ AVEC SUCCÈS")

    except Exception as e:
        # gestion des erreurs globales
        print(f"\n❌ ERREUR PIPELINE : {e}")

    finally:
        # exécution quoi qu’il arrive
        print("\n===================================")
        print("🏁 FIN DU PIPELINE")
        print("===================================\n")


# ==========================================
# POINT D’ENTRÉE DU SCRIPT
# ==========================================
# Ce bloc s’exécute uniquement si on lance le fichier directement
# (et pas quand Airflow l’importe comme module)

if __name__ == "__main__":
    main()