### 0. Clone le projet
__git clone https://github.com/brandthn/esgi-4iabd2-sparkstreaming-groupe13.git__

=> sur la branche __main__ (ou autre selon préférence)

### I. Télécharger data Janvier-2025
//Pour démo simple, diminuer la volumétrie de ce fichier pour juste avoir une mise en marche du projet
//Modifier le paramètre du nom du fichier source dans le fichier de configuration situé à `src/main/resources/application.conf` pour qu'il corresponde à votre nom de fichier source. Par défaut : __sourceFile = "data/yellow_tripdata_2024-01-preview.csv"__

### II. Build les images et lancer containers de Docker:
- Commandes Docker depuis la racine :

    -  `docker-compose up --build`

### Assurez-vous que les répertoires nécessaires existent:
`mkdir -p data/streaming data/processed/raw data/processed/pickup_agg data/processed/dropoff_agg data/processed/combined_agg data/processed/checkpoints/raw data/debug` (depuis la racine du projet)
