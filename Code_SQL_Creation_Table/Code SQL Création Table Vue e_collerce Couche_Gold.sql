-- ## CREATION DE LA COUCHE GOLD (OR) ## ----
'''
	La couche Gold est le niveau final du pipeline ELT. Elle contient des données prêtes à la consommation : 
	nettoyées, agrégées, et structurées pour répondre aux besoins métiers (KPI, reporting, API, dashboards). 
	Elle est le point d’entrée pour les outils d’analyse, les utilisateurs finaux ou les interfaces web
'''

'''
	la couche Gold consiste à créer des vues métier (souvent sous forme de tables de fait et 
	de tables de dimensions combinées)
'''

SELECT * FROM e_commerce_couche_silver.clients;
SELECT * FROM e_commerce_couche_silver.commandes;
SELECT * FROM e_commerce_couche_silver.produits;
SELECT * FROM e_commerce_couche_silver.details_commandes; 
SELECT * FROM e_commerce_couche_silver.paiements;

-- CREATION DE SCHEMA DE LA COUCHE GOLD
CREATE SCHEMA e_commerce_couche_gold;

-- ## Table de dimension client
CREATE VIEW e_commerce_couche_gold.dim_client 
AS
SELECT id_client, nom, prenom, gender, email, telephone, adresse
FROM e_commerce_couche_silver.clients;

-- ## Table de dimension produit
CREATE VIEW e_commerce_couche_gold.dim_produit 
AS
SELECT id_produit, nom, categorie
FROM e_commerce_couche_silver.produits;

-- ## Table de dimension commande
CREATE VIEW e_commerce_couche_gold.dim_commande 
AS
SELECT id_commande, date_commande, statut
FROM e_commerce_couche_silver.commandes;


-- #### VUE DE LA TABLE DE FAIT VENTE ### --- 
CREATE VIEW e_commerce_couche_gold.fait_ventes_aggregees 
AS
SELECT 
    com.id_commande,
    c.id_client,
    p.id_produit,
    SUM(dc.quantite) AS quantite_totale,
    SUM(dc.prix_total) AS vente_totale
FROM e_commerce_couche_silver.details_commandes dc
JOIN e_commerce_couche_silver.commandes com ON dc.id_commande = com.id_commande
JOIN e_commerce_couche_silver.clients c ON com.id_client = c.id_client
JOIN e_commerce_couche_silver.produits p ON dc.id_produit = p.id_produit
GROUP BY 
    com.id_commande,
    c.id_client, 
    p.id_produit;

-- DELECTION DES VUES
SELECT * FROM e_commerce_couche_gold.fait_ventes_aggregees;


