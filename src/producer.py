"""Producer : interroge Open-Meteo pour 6 villes marocaines et publie les réponses brutes dans Kafka."""
import json
import logging
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [producer] %(levelname)s %(message)s")
log = logging.getLogger(__name__)
logging.getLogger("kafka").setLevel(logging.WARNING)


def create_producer(retries: int = 30, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                retries=5,
            )
            log.info("Connecté à Kafka (%s)", config.KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka indisponible (tentative %d/%d), nouvel essai dans %ds", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError("Impossible de se connecter à Kafka")


def fetch_weather() -> list[dict]:
    """Un seul appel API pour les 6 villes (Open-Meteo accepte des listes de coordonnées)."""
    names = list(config.CITIES)
    params = {
        "latitude": ",".join(str(config.CITIES[c]["latitude"]) for c in names),
        "longitude": ",".join(str(config.CITIES[c]["longitude"]) for c in names),
        "hourly": ",".join(config.HOURLY_FIELDS),
        "current": ",".join(config.HOURLY_FIELDS),
        "timezone": config.TIMEZONE,
        "past_days": 1,
        "forecast_days": 1,
    }
    resp = requests.get(config.OPEN_METEO_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict):  # une seule ville -> objet au lieu d'une liste
        data = [data]

    fetched_at = datetime.now(timezone.utc).isoformat()
    messages = []
    for city, payload in zip(names, data):
        messages.append({
            "city": city,
            "fetched_at": fetched_at,
            "source": "open-meteo",
            "latitude": payload.get("latitude"),
            "longitude": payload.get("longitude"),
            "timezone": payload.get("timezone"),
            "elevation": payload.get("elevation"),
            "current": payload.get("current"),
            "hourly_units": payload.get("hourly_units"),
            "hourly": payload.get("hourly"),
        })
    return messages


def main() -> None:
    producer = create_producer()
    while True:
        try:
            messages = fetch_weather()
            for msg in messages:
                producer.send(config.KAFKA_TOPIC, key=msg["city"], value=msg)
                current = msg.get("current") or {}
                log.info("Envoyé %-10s | %s | T=%s°C | %d heures",
                         msg["city"], current.get("time"), current.get("temperature_2m"),
                         len((msg.get("hourly") or {}).get("time", [])))
            producer.flush()
            log.info("%d messages publiés sur '%s'", len(messages), config.KAFKA_TOPIC)
        except requests.RequestException as exc:
            log.error("Erreur API Open-Meteo : %s", exc)
        except Exception:
            log.exception("Erreur inattendue")
        time.sleep(config.POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
