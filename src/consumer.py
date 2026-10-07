"""Consumer : lit les messages bruts dans Kafka, les nettoie, calcule les agrégats et stocke tout dans MongoDB."""
import json
import logging
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import config
from cleaning import clean_records, flatten_hourly
from storage import WeatherStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s [consumer] %(levelname)s %(message)s")
log = logging.getLogger(__name__)
logging.getLogger("kafka").setLevel(logging.WARNING)


def create_consumer(retries: int = 30, delay: int = 5) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                config.KAFKA_TOPIC,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                group_id=config.KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )
            log.info("Abonné au topic '%s' (groupe %s)", config.KAFKA_TOPIC, config.KAFKA_GROUP_ID)
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka indisponible (tentative %d/%d), nouvel essai dans %ds", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError("Impossible de se connecter à Kafka")


def process(store: WeatherStore, message: dict, partition: int, offset: int) -> None:
    city = message.get("city")
    if not store.save_raw(message, partition, offset):
        log.info("%-10s | message déjà stocké (fetched_at=%s), ignoré", city, message.get("fetched_at"))
        return
    clean, stats = clean_records(flatten_hourly(message))
    inserted, updated = store.upsert_clean(clean)
    daily = store.refresh_daily(city, {r["date"] for r in clean})
    log.info("%-10s | brut=%d nulls=%d invalides=%d doublons=%d -> propres=%d "
             "(nouvelles=%d, deja_en_base=%d) | jours=%d",
             city, stats["total"], stats["nulls"], stats["invalid"], stats["duplicates"],
             stats["clean"], inserted, updated, len(daily))
    for d in daily:
        log.info("   %s %s : min=%.1f max=%.1f moy=%.1f°C pluie=%.1fmm (%dh)",
                 d["city"], d["date"], d["temp_min"], d["temp_max"], d["temp_avg"],
                 d["precipitation_total"], d["hours_count"])


def main() -> None:
    store = WeatherStore()
    consumer = create_consumer()
    for msg in consumer:
        try:
            process(store, msg.value, msg.partition, msg.offset)
        except Exception:
            log.exception("Erreur de traitement (partition %s, offset %s)", msg.partition, msg.offset)


if __name__ == "__main__":
    main()
