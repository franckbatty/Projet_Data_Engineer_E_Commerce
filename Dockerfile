# Image de base Python légère (slim = plus petit, plus rapide)
FROM python:3.10-slim

# Définir le dossier de travail dans le container
WORKDIR /app

# Copier uniquement le fichier requirements pour optimiser le cache Docker
COPY requirements.txt .

# Installer les dépendances Python sans garder le cache (image plus légère)
RUN pip install --no-cache-dir -r requirements.txt

# Copier tout le projet dans le container
COPY . .

# Commande par défaut : exécuter le pipeline ETL principal
CMD ["python", "ETL/main.py"]