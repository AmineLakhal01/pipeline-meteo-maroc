"""Accès MongoDB : 3 collections (brut, nettoyé, agrégats journaliers)."""
import logging
import time
from datetime import datetime, timezone

from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.errors import DuplicateKeyError, PyMongoError

import config
from cleaning import aggregate_daily

log = logging.getLogger(__name__)


class WeatherStore:
    def __init__(self, retries: int = 30, delay: int = 5):
        for attempt in range(1, retries + 1):
            try:
                self.client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
                self.client.admin.command("ping")
                break
            except PyMongoError as exc:
                log.warning("MongoDB indisponible (%d/%d) : %s", attempt, retries, exc)
                time.sleep(delay)
        else:
            raise RuntimeError("Impossible de se connecter à MongoDB")

        db = self.client[config.MONGO_DB]
        self.raw = db[config.COLL_RAW]
        self.clean = db[config.COLL_CLEAN]
        self.daily = db[config.COLL_DAILY]
        self._create_indexes()
        log.info("Connecté à MongoDB, base '%s'", config.MONGO_DB)

    def _create_indexes(self) -> None:
        # (ville, date de collecte) unique : un message Kafka relu n'est pas stocké deux fois.
        # On n'utilise pas (partition, offset) : les offsets repartent à 0 si le topic est recréé.
        self.raw.create_index([("city", ASCENDING), ("fetched_at", ASCENDING)], unique=True)
        # (ville, heure) unique : supprime les doublons entre deux appels API successifs
        self.clean.create_index([("city", ASCENDING), ("time", ASCENDING)], unique=True)
        self.clean.create_index([("city", ASCENDING), ("date", ASCENDING)])
        self.daily.create_index([("city", ASCENDING), ("date", ASCENDING)], unique=True)

    def save_raw(self, message: dict, partition: int, offset: int) -> bool:
        doc = {**message,
               "kafka": {"topic": config.KAFKA_TOPIC, "partition": partition, "offset": offset},
               "ingested_at": datetime.now(timezone.utc)}
        try:
            self.raw.insert_one(doc)
            return True
        except DuplicateKeyError:
            return False

    def upsert_clean(self, records: list[dict]) -> tuple[int, int]:
        """Retourne (nouvelles heures insérées, heures déjà connues mises à jour)."""
        if not records:
            return 0, 0
        now = datetime.now(timezone.utc)
        ops = [UpdateOne({"city": r["city"], "time": r["time"]},
                         {"$set": {**r, "updated_at": now}},
                         upsert=True)
               for r in records]
        res = self.clean.bulk_write(ops, ordered=False)
        return res.upserted_count, res.matched_count

    def refresh_daily(self, city: str, dates: set[str]) -> list[dict]:
        """Recalcule les agrégats des jours touchés à partir de TOUTES les heures propres en base."""
        records = list(self.clean.find({"city": city, "date": {"$in": sorted(dates)}}, {"_id": 0}))
        daily = aggregate_daily(records)
        now = datetime.now(timezone.utc)
        if daily:
            self.daily.bulk_write([
                UpdateOne({"city": d["city"], "date": d["date"]}, {"$set": {**d, "updated_at": now}}, upsert=True)
                for d in daily
            ], ordered=False)
        return daily
