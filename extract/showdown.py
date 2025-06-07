# extract/showdown.py
import requests
from datetime import datetime
from typing import List, Dict

# URLs base
LADDER_URL = "https://pokemonshowdown.com/ladder"
USER_URL = "https://pokemonshowdown.com/users"

formatos_gen9 = [
    "gen9randombattle",
    "gen9challengecup1v1",
    "gen9hackmonscup",
    "gen9ou",
    "gen9ubers",
    "gen9uu",
    "gen9ru",
    "gen9nu",
    "gen9pu",
    "gen9lc",
    "gen9monotype",
    "gen91v1",
    "gen9anythinggoes",
    "gen9zu",
    "gen9bssregi",
    "gen9cap",
    "gen9randomdoublesbattle",
    "gen9doublesou",
    "gen9vgc2025regi",
    "gen9vgc2025regibo3",
    "gen9almostanyability",
    "gen9balancedhackmons",
    "gen9godlygift",
    "gen9inheritance",
    "gen9mixandmega",
    "gen9partnersincrime",
    "gen9sharedpower",
    "gen9stabmons",
    "gen9nationaldex",
    "gen9nationaldexubers",
    "gen9nationaldexuu",
    "gen9nationaldexmonotype",
    "gen9nationaldexdoubles",
]


def fetch_json(url: str) -> Dict:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def extract_all_showdown(sc, logger, formats: List[str] = None, top_n: int = 100):
    """
    Extrae de Showdown:
      1) Para cada formato en 'formats' saca su ladder (top N jugadores)
      2) Para cada jugador, descarga su perfil JSON
    Devuelve un RDD de dos tipos de registros:
      - record_type='ladder': datos de ladder
      - record_type='profile': estadísticas de perfil
    """
    logger.info("Showdown: iniciando extracción de ladders y perfiles")

    # 1) Decide qué formatos procesar
    if not formats:
        formats = formatos_gen9  # <- Usa la lista fija de Gen9 que definiste arriba
        logger.info(f"Showdown: usando {len(formats)} formatos Gen9 predefinidos")

    records = []
    extract_time = datetime.utcnow().isoformat()

    for fmt in formats:
        try:
            ladder = fetch_json(f"{LADDER_URL}/{fmt}.json")
            players = ladder.get("toplist", [])[:top_n]
            logger.info(f"{fmt}: {len(players)} jugadores en top")

            for p in players:
                records.append(
                    {
                        "record_type": "ladder",
                        "format": fmt,
                        "username": p.get("username"),
                        "rating": p.get("rating"),
                        "gxe": p.get("gxe"),
                        "pr": p.get("pr"),  # otro rating Glicko
                        "extraction_time": extract_time,
                        "source": "showdown",
                        "endpoint": f"ladder/{fmt}",
                    }
                )
                # 2) Perfil de usuario
                profile = fetch_json(f"{USER_URL}/{p.get('username')}.json")
                # El perfil contiene, por ejemplo, un dict 'lp' con histórico de ladder, etc.
                # Extraemos los campos básicos:
                rec = {
                    "record_type": "profile",
                    "username": p.get("username"),
                    "format": fmt,
                    "gxe": profile.get("gxe"),
                    "rating": profile.get("rating"),
                    "levels": profile.get("levels"),  # rating histórico
                    "extraction_time": extract_time,
                    "source": "showdown",
                    "endpoint": f"profile/{p.get('username')}",
                }
                records.append(rec)

        except Exception as e:
            logger.error(f"Showdown: error en formato {fmt}: {e}")

    logger.info(f"Showdown: registros totales extraídos = {len(records)}")

    # Crear RDD y devolver
    return sc.parallelize(records)
