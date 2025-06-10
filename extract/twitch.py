# extract/twitch.py
import os
import time
import requests
from datetime import datetime
from typing import List, Dict, Tuple
from dotenv import load_dotenv
from pyspark.rdd import RDD

# Cargar variables de entorno desde .env
load_dotenv()

CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
BASE_URL = "https://api.twitch.tv/helix"
AUTH_URL = "https://id.twitch.tv/oauth2/token"


class TwitchAPIExtractor:
    """Extractor para Twitch API usando Client Credentials."""

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None

    def authenticate(self) -> None:
        """Obtener o renovar token de acceso OAuth (Client Credentials)."""
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }
        resp = requests.post(AUTH_URL, params=params)
        resp.raise_for_status()
        self.access_token = resp.json().get("access_token")

    def _get_headers(self) -> Dict[str, str]:
        if not self.access_token:
            self.authenticate()
        return {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self.access_token}",
        }

    def get_top_games(self, limit: int = 100) -> List[Dict]:
        """Obtener lista de los juegos más populares (top)."""
        games: List[Dict] = []
        after = None
        while len(games) < limit:
            params = {"first": min(100, limit - len(games))}
            if after:
                params["after"] = after
            resp = requests.get(
                f"{BASE_URL}/games/top", headers=self._get_headers(), params=params
            )
            resp.raise_for_status()
            data = resp.json()
            batch = data.get("data", [])
            if not batch:
                break
            games.extend(batch)
            after = data.get("pagination", {}).get("cursor")
            time.sleep(0.2)
        return games[:limit]

    def get_streams_by_game(self, game_id: str, limit: int = 100) -> List[Dict]:
        """Obtener streams activos de un juego específico."""
        streams: List[Dict] = []
        after = None
        while len(streams) < limit:
            params = {"game_id": game_id, "first": min(100, limit - len(streams))}
            if after:
                params["after"] = after
            resp = requests.get(
                f"{BASE_URL}/streams", headers=self._get_headers(), params=params
            )
            resp.raise_for_status()
            data = resp.json()
            batch = data.get("data", [])
            if not batch:
                break
            streams.extend(batch)
            after = data.get("pagination", {}).get("cursor")
            time.sleep(0.2)
        return streams[:limit]


def extract_all_twitch(
    sc, logger, top_game_limit: int = 200, streams_per_game: int = 50
) -> Tuple[RDD, RDD]:
    """
    Extrae datos de Twitch:
      - Top juegos (limit top_game_limit)
      - Streams activos por cada juego (streams_per_game)
    Devuelve dos RDDs:
      - Uno con juegos
      - Otro con streams
    """
    logger.info("Twitch: iniciando extracción")
    extractor = TwitchAPIExtractor(CLIENT_ID, CLIENT_SECRET)

    top_games = extractor.get_top_games(limit=top_game_limit)
    logger.info(f"Twitch: obtenidos {len(top_games)} juegos top")

    current_time = datetime.utcnow().isoformat()
    games_records: List[Dict] = []
    stream_records: List[Dict] = []

    for g in top_games:
        games_records.append(
            {
                "record_type": "game",
                "game_id": g["id"],
                "game_name": g["name"],
                "box_art_url": g.get("box_art_url"),
                "extraction_time": current_time,
                "source": "twitch",
                "endpoint": "games/top",
            }
        )

        game_id = g["id"]
        streams = extractor.get_streams_by_game(game_id, limit=streams_per_game)
        logger.info(f"Twitch: {len(streams)} streams para juego {g['name']}")

        for s in streams:
            stream_records.append(
                {
                    "record_type": "stream",
                    "stream_id": s["id"],
                    "game_id": s["game_id"],
                    "game_name": s.get("game_name", g["name"]),
                    "user_id": s["user_id"],
                    "user_name": s["user_name"],
                    "viewer_count": s["viewer_count"],
                    "started_at": s["started_at"],
                    "language": s["language"],
                    "extraction_time": current_time,
                    "source": "twitch",
                    "endpoint": "streams",
                }
            )

    logger.info(
        f"Twitch: total juegos = {len(games_records)}, total streams = {len(stream_records)}"
    )

    return sc.parallelize(games_records), sc.parallelize(stream_records)
