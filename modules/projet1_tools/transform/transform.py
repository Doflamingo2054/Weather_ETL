
import pandas as pd
from datetime import datetime



def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the input DataFrame by cleaning and restructuring the data.

    Parameters:
    df (pd.DataFrame): Input DataFrame with raw data.

    Returns:
    pd.DataFrame: Transformed DataFrame.
    """
    try:
        # Fais une copie du DataFrame original
        df_cleaned = df.copy()

        # Convert toutes les colonnes en minuscules
        df_cleaned.columns = [col.lower() for col in df_cleaned.columns]

        # Renommer les colonnes possibles vers 'temperature'
        if 'temp' in df_cleaned.columns:
            df_cleaned.rename(columns={'temp': 'temperature'},inplace=True)

        # Ajouter la temperature en Fahrenheit si la colonne existe
        if 'temperature' in df_cleaned.columns:
            df_cleaned['temperature_f'] = df_cleaned['temperature'] * 9 / 5 + 32

            # étiquettes météorologiques basées sur la température
            df_cleaned['weather_label'] = df_cleaned['temperature'].apply(
                lambda x: 'froid' if x < 10 else ('modéré' if 10 <= x < 25 else 'chaudt')
            )
        else:
            df_cleaned['temperature_f'] = None
            df_cleaned['weather_label'] = None

        # Calcul de la durée du jour en utilisant les colonnes 'sunrise' et 'sunset' si elles existent
        if 'sunrise' in df_cleaned.columns and 'sunset' in df_cleaned.columns:
            sunrise = df_cleaned['sunrise']
            sunset = df_cleaned['sunset']
            df_cleaned['day_length'] = round((sunset - sunrise) / 3600 ,2) # durée en heures
        else:
            df_cleaned['day_length'] = None

        # Ajouter une colonne avec la date de la transformation (servira pour le tracking)
        df_cleaned['transformation_datetime'] = datetime.now()
        df_cleaned = df_cleaned.dropna(subset=['temperature', 'humidity'])
        
        #supprimer les colonnes inutiles
        cols_to_drop = ['feels_like', 'min_temp', 'max_temp', 'wind_degrees', 'sunrise', 'sunset','wind_speed','wind_degrees']
        df_cleaned = df_cleaned.drop(columns=[col for col in cols_to_drop if col in df_cleaned.columns])
        print("Transformation réussie.")

        return df_cleaned
    except Exception as e:
        print(f"Il y'a eu une erreur lors de la transformation {e}")
        return df  #Return le DataFrame original en cas d'erreur


if __name__ == "__main__":
    # Example usage
    exple = {
    "cloud_pct": [40],
    "temp": [27],
    "feels_like": [30],
    "humidity": [89],
    "min_temp": [27],
    "max_temp": [27],
    "wind_speed": [0.51],
    "wind_degrees": [300],
    "sunrise": [1762754589],
    "sunset": [1762797416]
}
    df = pd.DataFrame(exple)
    transformed_df = transform_data(df)
    print(transformed_df)