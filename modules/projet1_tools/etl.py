from load.load import load_csv
from extract_data.extract import extract_from_api
from transform.transform import transform_data
import pandas as pd
import os
import sys

# Configuration
bd = {
    "Afrique du Sud": ("Pretoria", "ZA"),
    "Algérie": ("Alger", "DZ"),
    "Nigeria": ("Abuja", "NG"),
    "Égypte": ("Le Caire", "EG"),
    "Kenya": ("Nairobi", "KE"),
    "Ghana": ("Accra", "GH"),
    "Maroc": ("Rabat", "MA"),
    "Sénégal": ("Dakar", "SN"),
    "Cameroun": ("Yaoundé", "CM"),
    "Cote d'Ivoire": ("Abidjan", "CI"),
}

all_data = []

# Extraction
print("=== EXTRACTION ===")
for country, (city, iso_code) in bd.items():
    raw_json = extract_from_api(city, country, iso_code)
    if raw_json is not None:
        all_data.append(raw_json)
        print(f"Données extraites pour {city}, {country}")
    else:
        print(f"Aucune donnée extraite pour {city}, {country}")

# CVerifier si des données ont été extraites
if all_data:
    raw_data = pd.json_normalize(all_data)
    print(f"DataFrame créé: {len(raw_data)} lignes")
else:
    print("Aucune donnée extraite à traiter.")
    sys.exit(1)  # Exit aucune données

# Transformation
print("\n=== TRANSFORMATION ===")
try:
    transformed_data = transform_data(raw_data)
    
    if transformed_data is None or transformed_data.empty:
        print("Erreur: Transformation a produit des données vides")
        sys.exit(1)
    
    print(f"Données transformées avec succès: {len(transformed_data)} lignes")
    
except Exception as e:
    print(f"Erreur lors de la transformation des données : {e}")
    sys.exit(1)

# Chargement..
print("\n=== CHARGEMENT ===")
try:
    output_filepath = os.path.join("data", "processed", "weather_data.csv")
    os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
    
    success = load_csv(transformed_data, output_filepath)
    
    if success:
        print("Pipeline ETL terminé avec succès!")
    else:
        print("Échec du chargement des données")
        sys.exit(1)
        
except Exception as e:
    print(f"Erreur lors du chargement des données : {e}")
    sys.exit(1)