# extract/showdown.py
import json
import requests
from datetime import datetime
from typing import Tuple
from pyspark.rdd import RDD


def fetch_json(url: str):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def extract_all_showdown(sc, logger) -> RDD:
    logger.info("Showdown: iniciando extracción desde fuentes públicas")

    current_time = datetime.utcnow().isoformat()

    # Solo extraer Pokédex
    try:
        logger.info("Descargando pokedex...")
        pokedex_json = fetch_json("https://play.pokemonshowdown.com/data/pokedex.json")
        pokedex_data = [
            {
                "record_type": "pokedex",
                "pokemon_id": k,
                "name": v.get("name"),
                "types": v.get("types"),
                "baseStats": v.get("baseStats"),
                "abilities": v.get("abilities"),
                "extraction_time": current_time,
                "source": "showdown",
                "endpoint": "data/pokedex.json",
            }
            for k, v in pokedex_json.items()
        ]
        rdd_pokedex = sc.parallelize(pokedex_data)
    except Exception as e:
        logger.error(f"Error descargando pokedex: {str(e)}")
        rdd_pokedex = sc.parallelize([])

    logger.info("Showdown: extracción de Pokédex completada con éxito")
    return rdd_pokedex
