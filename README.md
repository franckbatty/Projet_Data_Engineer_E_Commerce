

# 🛍️ Projet Data E-Commerce – Pipeline ETL & Modélisation OLAP

![Architecture Haut niveau Data Warehouse](https://github.com/user-attachments/assets/454374c3-214c-4b85-b713-c72a9f3b53dc)

---

## 📌 Contexte  
Ce projet simule la plateforme d’un site e-commerce fictif.  
L’objectif est de construire un pipeline de données en architecture **Bronze → Silver → Gold**, orchestré par **Apache Airflow**, afin de :  
- Structurer les données de vente.  
- Garantir la traçabilité et la qualité des données.  
- Fournir une base analytique exploitable par la **Business Intelligence (BI)**.  

---

## 🧱 Architecture des Données


### 🔹 Couche Bronze – Zone de dépôt brute  
- Ingestion **automatisée** via **Airflow**  
- Stockage des **fichiers CSV bruts** sans traitement  
- Objectif : **conserver l’état original des données** pour traçabilité et réexploitation  

### 🔹 Couche Silver – Préparation des données  
- Nettoyage et normalisation des formats (dates, chaînes de caractères, emails)  
- Suppression des doublons et valeurs incohérentes  
- Objectif : rendre les données **cohérentes et fiables**  

### 🔹 Couche Gold – Couches analytiques (OLAP)  
- Modélisation **en étoile** :  
  - `fait_ventes_aggregees` (table de faits)  
  - `dim_client`, `dim_produit`, `dim_commande` (tables de dimensions)  
- Créées sous forme de **vues PostgreSQL**  
- Objectif : exposer les **indicateurs clés** à la BI  

---

## 🧰 Technologies utilisées

| Composant          | Outil / Technologie              |
|--------------------|----------------------------------|
| Données brutes     | [Mockaroo](https://www.mockaroo.com/) |
| Orchestration      | **Apache Airflow** (DAG quotidien)   |
| Transformation     | Python, Pandas, SQL              |
| Base de données    | PostgreSQL (Docker)              |
| Modélisation OLAP  | Vues SQL (modèle étoile)         |
| BI / Reporting     | Power BI                        |

---

## 🚀 Exécution du pipeline

1. **Bronze** → Airflow déclenche la collecte depuis les fichiers bruts (CSV)  
2. **Silver** → Les données sont nettoyées et formatées  
3. **Gold** → Les vues OLAP sont générées automatiquement  
4. **BI** → Les résultats sont exploitables via Power BI  

---

## 📊 Connexion Power BI ↔ Couche Gold

1. Vérifier que PostgreSQL est exposé dans `docker-compose.yml` :  
   ```yaml
   ports:
     - "5433:5432"



> Réalisé par **Franck 🇨🇬** – Projet Data Engineering orienté bonnes pratiques (pipeline automatisé, modèle analytique propre)

