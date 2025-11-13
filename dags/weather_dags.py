import logging
from typing import Any, TYPE_CHECKING
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta, timezone
import pandas as pd
import os


log = logging.getLogger(__name__)

filepath = "data/processed/weather_data.csv"

pays = {
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


@dag(
    dag_id="weather_etl_dag",
    default_args={
        "owner": "axel",
        "depends_on_past": False,
        "start_date": datetime(2024, 1, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="@daily",
    catchup=False,
    tags=["weather", "etl"],
)
def weather_etl_dag():

    @task
    def extract_task() -> str:
        """Extraire les données de l'API pour tous les pays et écrire un CSV brut. Retourne le chemin du fichier brut."""
        # C'est dans le cas où il y'a un gros volume de données à extraire et on ne veut pas tout garder en mémoire
        from modules.projet1_tools.extract_data.extract import extract_from_api

        all_data = []
        for country, (city, iso_code) in pays.items():
            raw_json = extract_from_api(city, country, iso_code)
            if raw_json is not None:
                all_data.append(raw_json)
                log.info("Données extraites pour %s, %s", city, country)
            else:
                log.warning("Aucune donnée extraite pour %s, %s", city, country)

        raw_dir = os.path.join("data", "raw")
        os.makedirs(raw_dir, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        raw_path = os.path.join(raw_dir, f"weather_raw_{timestamp}.csv")

        if all_data:
            raw_df = pd.json_normalize(all_data)
            raw_df.to_csv(raw_path, index=False, encoding="utf-8")
            log.info("Raw CSV créé: %s lignes -> %s", len(raw_df), raw_path)
        else:
            # pour éviter les erreurs plus tard, on crée un fichier vide
            pd.DataFrame().to_csv(raw_path, index=False)
            log.warning("Aucune donnée collectée pour tous les pays. Fichier vide créé: %s", raw_path)

        return raw_path

    @task
    def transform_task(raw_path: Any) -> str:
        from modules.projet1_tools.transform.transform import transform_data

        """Read raw CSV from raw_path, transform it and write transformed CSV. Returns transformed file path."""
        if raw_path is None:
            log.warning("transform_task: raw_path est None — rien à transformer.")
            return ""

        if not os.path.exists(raw_path):
            log.warning("transform_task: raw_path n'existe pas: %s", raw_path)
            return ""

        raw_df = pd.read_csv(raw_path)
        if raw_df.empty:
            log.warning("transform_task: raw_df est vide — rien à transformer.")
            return ""

        transformed_df = transform_data(raw_df)

        processed_dir = os.path.join("data", "processed")
        os.makedirs(processed_dir, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        transformed_path = os.path.join(processed_dir, f"weather_transformed_{timestamp}.csv")
        transformed_df.to_csv(transformed_path, index=False, encoding="utf-8")
        log.info("Données transformées écrites: %s lignes -> %s", len(transformed_df), transformed_path)
        return transformed_path

    @task
    def load_task(transformed_path: Any, filepath=filepath) -> bool:
        from modules.projet1_tools.load.load import load_csv

        """Read transformed CSV from transformed_path and save final CSV via load_csv."""
        if not transformed_path:
            log.warning("load_task: transformed_path vide — rien à charger.")
            return False

        if not os.path.exists(transformed_path):
            log.error("load_task: fichier transformé introuvable: %s", transformed_path)
            raise AirflowFailException(f"Fichier transformé introuvable: {transformed_path}")

        df = pd.read_csv(transformed_path)
        if df.empty:
            log.warning("load_task: DataFrame lu depuis transformed_path est vide — rien à charger.")
            return False

        success = load_csv(df, filepath)
        if success:
            log.info("Données chargées avec succès dans %s.", filepath)
            return True
        else:
            log.error("Échec du chargement des données.")
            raise AirflowFailException("Échec du chargement des données.")

    # Définition des dépendances à l'intérieur du DAG
    raw = extract_task()
    transformed = transform_task(raw)
    load_task(transformed)


weather_dag = weather_etl_dag()


