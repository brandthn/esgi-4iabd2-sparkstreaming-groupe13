import streamlit as st
import pandas as pd
import json
import os
import glob
import time
from datetime import datetime, timedelta

# Configuration
DATA_DIR = "data/processed"
RAW_DATA_PATH = f"{DATA_DIR}/raw"
PICKUP_AGG_PATH = f"{DATA_DIR}/pickup_agg"
DROPOFF_AGG_PATH = f"{DATA_DIR}/dropoff_agg"
COMBINED_AGG_PATH = f"{DATA_DIR}/combined_agg"
REFRESH_INTERVAL = 5  # secondes

# Fonction pour charger les données les plus récentes (2 derniers batchs)
def load_latest_data(dir_path, max_batches=2):
    if not os.path.exists(dir_path):
        return pd.DataFrame()
    
    # Trouver tous les fichiers JSON dans le répertoire
    all_files = glob.glob(f"{dir_path}/*.json")
    
    if not all_files:
        return pd.DataFrame()
    
    # Obtenez les derniers fichiers modifiés
    latest_files = sorted(all_files, key=os.path.getmtime, reverse=True)[:max_batches]
    
    # Charger et combiner les données
    dfs = []
    for file in latest_files:
        try:
            # Lire les lignes JSON une par une
            with open(file, 'r') as f:
                lines = f.readlines()
            
            # Parsez chaque ligne JSON comme un enregistrement distinct
            records = [json.loads(line) for line in lines if line.strip()]
            
            if records:
                df = pd.DataFrame(records)
                dfs.append(df)
        except Exception as e:
            st.error(f"Erreur lors du chargement du fichier {file}: {e}")
    
    if not dfs:
        return pd.DataFrame()
    
    # Combiner tous les DataFrames
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Si la colonne 'batch_id' existe, trier par elle
    if 'batch_id' in combined_df.columns:
        combined_df = combined_df.sort_values(by='batch_id', ascending=False)
    
    return combined_df

def format_timestamp(ts):
    if pd.isna(ts):
        return "N/A"
    try:
        if isinstance(ts, str):
            return ts
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return str(ts)

# UI Streamlit
st.set_page_config(
    page_title="Tableau de bord des taxis en temps réel",
    page_icon="🚕",
    layout="wide"
)

st.title("🚕 Tableau de bord des taxis jaunes NYC en temps réel - Groupe 13")

# Informations sur les derniers batchs
st.sidebar.header("Informations sur les batchs")

# Fonction pour obtenir l'heure du dernier batch
def get_last_batch_time():
    if not os.path.exists(RAW_DATA_PATH):
        return None
    
    all_files = glob.glob(f"{RAW_DATA_PATH}/*.json")
    if not all_files:
        return None
    
    # Obtenir le dernier fichier modifié
    latest_file = max(all_files, key=os.path.getmtime)
    last_modified = os.path.getmtime(latest_file)
    
    return datetime.fromtimestamp(last_modified)

last_batch_time = get_last_batch_time()
if last_batch_time:
    st.sidebar.success(f"Dernier batch reçu: {last_batch_time.strftime('%H:%M:%S')}")
    time_diff = datetime.now() - last_batch_time
    seconds_ago = int(time_diff.total_seconds())
    st.sidebar.text(f"Il y a {seconds_ago} secondes")
else:
    st.sidebar.warning("Aucun batch reçu pour l'instant")

# Vérifier les fichiers de debug
debug_file = "data/debug/sent_batches.txt"
if os.path.exists(debug_file):
    st.sidebar.subheader("Diagnostic Producer")
    
    # Lire les dernières lignes du fichier de debug
    with open(debug_file, 'r') as f:
        lines = f.readlines()
        last_20_lines = lines[-20:] if len(lines) > 20 else lines
        debug_content = "".join(last_20_lines)
    
    st.sidebar.text_area("Derniers batchs envoyés", debug_content, height=200)

# Ajouter case à cocher pour rafraîchissement automatique
auto_refresh = st.sidebar.checkbox("Rafraîchissement automatique", value=True)

# Ajouter bouton de rafraîchissement manuel
if st.button("Rafraîchir les données"):
    st.rerun()

# Créer des colonnes pour l'affichage
col1, col2 = st.columns(2)

with col1:
    st.subheader("📊 Derniers trajets reçus")
    
    # Afficher les dernières données brutes
    raw_data = load_latest_data(RAW_DATA_PATH)
    
    if not raw_data.empty:
        # Formatage des timestamps
        if 'tpep_pickup_datetime' in raw_data.columns:
            raw_data['tpep_pickup_datetime'] = raw_data['tpep_pickup_datetime'].apply(format_timestamp)
        if 'tpep_dropoff_datetime' in raw_data.columns:
            raw_data['tpep_dropoff_datetime'] = raw_data['tpep_dropoff_datetime'].apply(format_timestamp)
        
        # Sélectionner les colonnes les plus importantes
        columns_to_display = [
            'batch_id', 'VendorID', 'tpep_pickup_datetime', 'PULocationID',
            'DOLocationID', 'passenger_count', 'trip_distance', 'fare_amount'
        ]
        
        # Filtrer les colonnes qui existent réellement dans le DataFrame
        existing_columns = [col for col in columns_to_display if col in raw_data.columns]
        
        if existing_columns:
            st.dataframe(raw_data[existing_columns], use_container_width=True)
            st.text(f"Total des trajets affichés: {len(raw_data)}")
        else:
            st.warning("Aucune donnée disponible dans le format attendu.")
    else:
        st.info("Aucune donnée brute disponible. Attendez le prochain batch...")

with col2:
    st.subheader("📍 Aggrégations par lieu")
    
    # Afficher les données agrégées par lieu de prise en charge
    tab1, tab2 = st.tabs(["📥 Lieux de prise en charge", "📤 Lieux de dépose"])
    
    with tab1:
        pickup_data = load_latest_data(PICKUP_AGG_PATH)
        
        if not pickup_data.empty:
            if 'PULocationID' in pickup_data.columns:
                pickup_data = pickup_data.rename(columns={'PULocationID': 'location_id'})
            
            if 'location_id' in pickup_data.columns and 'trip_count' in pickup_data.columns:
                # Trier par nombre de trajets décroissant
                pickup_data = pickup_data.sort_values(by='trip_count', ascending=False)
                st.dataframe(pickup_data, use_container_width=True)
                
                # Afficher un graphique
                st.bar_chart(pickup_data.set_index('location_id')['trip_count'])
            else:
                st.warning("Format des données d'agrégation inattendu.")
        else:
            st.info("Aucune donnée d'agrégation disponible pour les lieux de prise en charge.")
    
    with tab2:
        dropoff_data = load_latest_data(DROPOFF_AGG_PATH)
        
        if not dropoff_data.empty:
            if 'DOLocationID' in dropoff_data.columns:
                dropoff_data = dropoff_data.rename(columns={'DOLocationID': 'location_id'})
            
            if 'location_id' in dropoff_data.columns and 'trip_count' in dropoff_data.columns:
                # Trier par nombre de trajets décroissant
                dropoff_data = dropoff_data.sort_values(by='trip_count', ascending=False)
                st.dataframe(dropoff_data, use_container_width=True)
                
                # Afficher un graphique
                st.bar_chart(dropoff_data.set_index('location_id')['trip_count'])
            else:
                st.warning("Format des données d'agrégation inattendu.")
        else:
            st.info("Aucune donnée d'agrégation disponible pour les lieux de dépose.")

# Rafraîchissement automatique
if auto_refresh:
    time.sleep(REFRESH_INTERVAL)
    st.rerun()