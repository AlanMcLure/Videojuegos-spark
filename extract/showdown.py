import requests
import time
import urllib.parse
from datetime import datetime
from typing import List, Dict
from requests.exceptions import HTTPError

# URLs base
LADDER_URL = "https://pokemonshowdown.com/ladder"
USER_URL = "https://pokemonshowdown.com/users"

formatos_gen9 = ["gen9ou", "gen9ubers", "gen9uu", "gen9ru", "gen9nu", "gen9monotype"]


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

    if not formats:
        formats = formatos_gen9
        logger.info(f"Showdown: usando {len(formats)} formatos Gen9 predefinidos")

    extract_time = datetime.utcnow().isoformat()

    ladder_records = []
    profile_records = []

    for fmt in formats:
        try:
            ladder_url = f"{LADDER_URL}/{fmt}.json"
            ladder = fetch_json(ladder_url)
            players = ladder.get("toplist", [])[:top_n]
            logger.info(f"{fmt}: {len(players)} jugadores en top")

            for p in players:
                username = p.get("username")

                ladder_records.append(
                    {
                        "record_type": "ladder",
                        "format": fmt,
                        "username": username,
                        "rating": p.get("rating"),
                        "gxe": p.get("gxe"),
                        "pr": p.get("pr"),
                        "extraction_time": extract_time,
                        "source": "showdown",
                        "endpoint": f"ladder/{fmt}",
                    }
                )

                try:
                    encoded_username = urllib.parse.quote(username)
                    profile_url = f"{USER_URL}/{encoded_username}.json"
                    profile = fetch_json(profile_url)

                    profile_records.append(
                        {
                            "record_type": "profile",
                            "username": username,
                            "format": fmt,
                            "gxe": profile.get("gxe"),
                            "rating": profile.get("rating"),
                            "levels": profile.get("levels"),
                            "extraction_time": extract_time,
                            "source": "showdown",
                            "endpoint": f"profile/{username}",
                        }
                    )

                    time.sleep(0.3)

                except HTTPError as http_err:
                    status = http_err.response.status_code
                    if status == 403:
                        logger.warning(f"[{fmt}] 403 Forbidden para {username}")
                    elif status == 404:
                        logger.warning(f"[{fmt}] 404 Not Found: usuario {username}")
                    else:
                        logger.error(f"[{fmt}] HTTP error {status} para {username}")
                except Exception as e:
                    logger.error(f"[{fmt}] Error inesperado para {username}: {e}")

        except Exception as e:
            logger.error(f"Showdown: error en formato {fmt}: {e}")

    logger.info(
        f"Showdown: registros ladder = {len(ladder_records)}, perfiles = {len(profile_records)}"
    )
    return sc.parallelize(ladder_records), sc.parallelize(profile_records)
