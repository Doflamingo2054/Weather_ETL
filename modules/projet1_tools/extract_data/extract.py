
import requests
import pandas as pd
from pandas import json_normalize
api_url1 = "https://api.api-ninjas.com/v1/geocoding"

api_url2 = "https://api.api-ninjas.com/v1/weather"

api_key = "L3DZU6ivHtg5eCfub8nkHg==53WVcTizrCToP6yN"

headers= {
    "X-Api-Key": api_key
}

def extract_from_api(city,country,iso_code):
    geocoding_params= {
        "city": city,
        "country": country,
    }
    try:
        response = requests.get(api_url1, headers=headers, params=geocoding_params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # filtrer par code ISO
        filtered = [d for d in data if d.get("country") == iso_code]
        
        if filtered:
            geo = filtered[0]  # premier match correct
            lat, lon = geo["latitude"], geo["longitude"]
            print(f"{city}, {country}: latitude={lat}, longitude={lon}")
            weather_params = {
                "lat": lat,
                "lon": lon
            }
            weather_response = requests.get(api_url2, headers=headers, params=weather_params, timeout=10)
            weather_response.raise_for_status()
            raw_json = weather_response.json()
            return raw_json
        else:
            print(f"Aucun résultat de géocodage pour {city}, {country} avec le code ISO {iso_code}")
            return None
    except requests.RequestException as e:
        print(f"Erreur pour {city}, {country}: {e}")
        return None



    
