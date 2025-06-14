-- CREATION DE SCHEMA
CREATE SCHEMA e_commerce_couche_bronze; 

-- Table Clients -- 
CREATE TABLE IF NOT EXISTS e_commerce_couche_bronze.clients (
    id_client INT,
    nom VARCHAR,
    prenom VARCHAR,
    gender VARCHAR,
    email VARCHAR,
    telephone VARCHAR,
    adresse VARCHAR
);
SELECT * FROM e_commerce_couche_bronze.clients; 

-- Table commandes
CREATE TABLE IF NOT EXISTS e_commerce_couche_bronze.commandes (
    id_commande INT,
    id_client INT,
    date_commande VARCHAR(50),
    statut VARCHAR(50)
);
SELECT * FROM e_commerce_couche_bronze.commandes;

-- Table produits
CREATE TABLE e_commerce_couche_bronze.produits (
    id_produit INT,
    nom VARCHAR(255),
    categorie VARCHAR(100),
    prix FLOAT,
    quantite_stock INT
); 
SELECT * FROM e_commerce_couche_bronze.produits;

-- Table details commandes
CREATE TABLE e_commerce_couche_bronze.details_commandes (
    id_detail_commande INT,
    id_commande INT,
    id_produit INT,
    quantite INT,
    prix_total FLOAT
);
SELECT * FROM e_commerce_couche_bronze.details_commandes;

-- Table paiements
CREATE TABLE e_commerce_couche_bronze.paiements (
    id_paiement INT,
    id_commande INT,
    methode_paiement VARCHAR(50),
    montant FLOAT,
    date_paiement VARCHAR(50)
);
SELECT * FROM e_commerce_couche_bronze.paiements;  
