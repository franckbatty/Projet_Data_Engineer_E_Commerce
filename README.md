![Architecture Haut niveau Data Werehouse](https://github.com/user-attachments/assets/454374c3-214c-4b85-b713-c72a9f3b53dc)


# 🛍️ Projet Data E-Commerce – Pipeline ETL & Modélisation OLAP

## 📌 Contexte  
Ce projet simule la plateforme d’un site e-commerce fictif. L’objectif est de construire un pipeline de données en architecture **Bronze → Silver → Gold**, orchestré par **Airflow**, pour structurer les données de vente et fournir une base analytique exploitable.

---

## 🧱 Architecture des Données

```
Sources (Mockaroo / CSV simulés)
   ↓
[ Bronze ]
   ↓
[ Silver ]
   ↓
[ Gold ]
   ↓
   BI
```

### 🔹 Couche Bronze – Zone de dépôt brute  
- Ingestion **automatisée** via **Airflow**  
- Stockage des **fichiers CSV bruts** sans traitement  
- Objectif : **conserver l’état original des données** pour traçabilité et réexploitation

### 🔹 Couche Silver – Préparation des données  
- Nettoyage des formats (dates, chaine de caratère)    

### 🔹 Couche Gold – Couches analytiques (OLAP)  
- Modélisation **en étoile** :  
  - `fait_ventes_aggregees` (table de faits)  
  - `dim_client`, `dim_produit`, `dim_commande` (dimensions)  
- Créées sous forme de **vues PostgreSQL**  
- Objectif : exposer les indicateurs clés à la BI. 

---

## 🧰 Technologies utilisées

| Composant          | Outil / Technologie              |
|--------------------|----------------------------------|
| Données brutes     | [Mockaroo](https://www.mockaroo.com/) |
| Orchestration      | **Apache Airflow** (DAG quotidien)   |
| Transformation     | Python, Pandas, SQL              |
| Base de données    | PostgreSQL                      |
| Modélisation OLAP  | Vues SQL (modèle étoile)         |

---

## 🚀 Exécution du pipeline

1. Airflow déclenche la collecte depuis les fichiers bruts (Bronze)  
2. Les données sont nettoyées et formatées (Silver)  
3. Les vues OLAP sont générées automatiquement (Gold)   
4. Les résultats sont exploitables via un outil de BI comme Power BI.

---

> Réalisé par **Franck 🇨🇬** – Projet Data Engineering orienté bonnes pratiques (pipeline automatisé, modèle analytique propre)

