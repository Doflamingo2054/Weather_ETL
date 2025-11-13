import sqlalchemy
import pandas as pd


def load_csv(dataframe : pd.DataFrame, filepath: str) -> bool:
    """
    Load the DataFrame into a CSV file.

    Args:
        dataframe (pd.DataFrame): The DataFrame to be saved.
        filepath (str): The path where the CSV file will be saved.
    """
    try:
        business_data = dataframe.to_csv(filepath, index=False, encoding='utf-8')
        print(f"Fichier CSV sauvegardé : {filepath}")
        return True
    except Exception as e:
        print(f"Erreur lors de la sauvegarde du CSV : {e}")
        return False




