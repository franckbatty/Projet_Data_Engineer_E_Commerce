-- CREATION DE SCHEMA
CREATE SCHEMA e_commerce_couche_silver; 

-- Table Clients -- 
CREATE TABLE IF NOT EXISTS e_commerce_couche_silver.clients (
    id_client INT PRIMARY KEY,
    nom VARCHAR(255) NOT NULL,
    prenom VARCHAR(255) NOT NULL,
    gender VARCHAR(50) NOT NULL,
    email VARCHAR(255) NOT NULL,
    telephone VARCHAR(50) NOT NULL,
    adresse VARCHAR(255) NOT NULL
);

SELECT * FROM e_commerce_couche_silver.clients;

-- Table commandes
CREATE TABLE IF NOT EXISTS e_commerce_couche_silver.commandes (
    id_commande INT PRIMARY KEY,
    id_client INT NOT NULL,
    date_commande VARCHAR(50) NOT NULL,
    statut VARCHAR(50) NOT NULL,
    FOREIGN KEY (id_client) REFERENCES e_commerce_couche_silver.clients(id_client)
);

SELECT * FROM e_commerce_couche_silver.commandes;

-- Table produits
CREATE TABLE e_commerce_couche_silver.produits (
    id_produit INT PRIMARY KEY,
    nom VARCHAR(255) NOT NULL,
    categorie VARCHAR(100) NOT NULL,
    prix FLOAT NOT NULL,
    quantite_stock INT NOT NULL
);
SELECT * FROM e_commerce_couche_silver.produits;

-- Table details commandes
CREATE TABLE e_commerce_couche_Silver.details_commandes (
    id_detail_commande INT PRIMARY KEY,
    id_commande INT NOT NULL,
    id_produit INT NOT NULL,
    quantite INT NOT NULL,
    prix_total FLOAT NOT NULL,
    FOREIGN KEY (id_commande) REFERENCES e_commerce_couche_silver.commandes(id_commande),
    FOREIGN KEY (id_produit) REFERENCES e_commerce_couche_silver.produits(id_produit)
);
SELECT * FROM e_commerce_couche_silver.details_commandes; 

-- Table paiements
CREATE TABLE e_commerce_couche_silver.paiements (
    id_paiement INT PRIMARY KEY,
    id_commande INT NOT NULL,
    methode_paiement VARCHAR(50) NOT NULL,
    montant FLOAT NOT NULL,
    date_paiement VARCHAR(50) NOT NULL,
    FOREIGN KEY (id_commande) REFERENCES e_commerce_couche_silver.commandes(id_commande)
);
SELECT * FROM e_commerce_couche_silver.paiements;
