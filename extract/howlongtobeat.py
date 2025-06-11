# extract/howlongtobeat.py
from howlongtobeatpy import HowLongToBeat
from datetime import datetime
from typing import List, Dict
import os


class HLTBExtractor:
    def __init__(self):
        self.hltb = HowLongToBeat()

    def fetch_game_times(self, title: str) -> Dict:
        results = self.hltb.search(title)
        if not results:
            return None
        r = results[0]
        return {
            "record_type": "hl_tb_time",
            "game_name": title,
            "game_image_url": r.game_image_url,
            "review_score": r.review_score,
            "main_story": r.main_story,
            "main_extra": r.main_extra,
            "completionist": r.completionist,
            "extraction_time": datetime.utcnow().isoformat(),
            "source": "howlongtobeat",
            "endpoint": "hltb_search",
        }


def extract_all_hltb(sc, logger, games_list: List[str]):
    """
    Extrae tiempos de juego para cada título de games_list.
    Devuelve un RDD de dicts con los campos listados arriba.
    """
    logger.info("HLTB: iniciando consultas de duración de juegos")

    extractor = HLTBExtractor()
    current_time = datetime.utcnow().isoformat()

    # Mapeamos cada título a su dict (o None si falla)
    records = []
    for title in games_list:
        try:
            rec = extractor.fetch_game_times(title)
            if rec:
                # Sobrescribir extraction_time para que sea uniforme
                rec["extraction_time"] = current_time
                records.append(rec)
            else:
                logger.warning(f"HLTB: sin resultados para '{title}'")
        except Exception as e:
            logger.error(f"HLTB: error al procesar '{title}': {e}")

    logger.info(f"HLTB: consultas completadas, registros válidos={len(records)}")

    # Crear y devolver RDD
    rdd = sc.parallelize(records)
    return rdd
