FROM python:3.9-slim

WORKDIR /app

# Installer les dépendances
RUN pip install --no-cache-dir streamlit pandas matplotlib plotly

# Copier le script Streamlit
COPY src/main/python/taxi_dashboard.py .

# Exposer le port utilisé par Streamlit
EXPOSE 8501

# Créer les répertoires pour les données
RUN mkdir -p data/processed/raw data/processed/pickup_agg data/processed/dropoff_agg data/processed/combined_agg

# Commande pour démarrer l'application Streamlit
CMD ["streamlit", "run", "taxi_dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]