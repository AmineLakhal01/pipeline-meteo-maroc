# Pipeline météo temps réel — 6 villes marocaines

Rabat, Casablanca, Marrakech, Fès, Tanger et Agadir : les données météo sont récupérées depuis
l'API [Open-Meteo](https://open-meteo.com/) (gratuite, sans clé), transitent par **Kafka**, sont
nettoyées et agrégées par un consumer **Python**, puis stockées dans **MongoDB**. Tout tourne dans **Docker**.

```
Open-Meteo API ──► producer.py ──► Kafka (topic weather_raw, 3 partitions, clé = ville)
                                          │
                                          ▼
                                   consumer.py ── nettoyage + agrégation ──► MongoDB (meteo_maroc)
                                                                               ├─ raw_weather
                                                                               ├─ clean_weather
                                                                               └─ daily_weather_stats
                                                                                     ▲
                                                                     mongo-express (http://localhost:8081)
```

## Prérequis

- Docker Desktop (moteur démarré)
- Git

## Démarrage

```bash
docker compose up -d --build
```

Ordre de démarrage garanti par les `healthcheck` / `depends_on` :
`kafka` + `mongodb` (healthy) → `kafka-init` (crée le topic puis s'arrête) → `producer` + `consumer` + `mongo-express`.

## Services

| Service         | Image                  | Port hôte | Rôle |
|-----------------|------------------------|-----------|------|
| `kafka`         | apache/kafka:3.8.0     | 29092     | Broker Kafka en mode KRaft (sans Zookeeper) |
| `kafka-init`    | apache/kafka:3.8.0     | –         | Crée le topic `weather_raw` (3 partitions) |
| `mongodb`       | mongo:7.0              | 27017     | Stockage (utilisateur `admin` / `admin123`) |
| `mongo-express` | mongo-express:1.0.2    | 8081      | Interface web (sans login, usage local) |
| `producer`      | build local (Python 3.12) | –      | Appelle Open-Meteo toutes les 60 s et publie dans Kafka |
| `consumer`      | build local (Python 3.12) | –      | Lit Kafka, nettoie, agrège, écrit dans MongoDB |

## Structure

```
.
├── docker-compose.yml
├── Dockerfile              # image commune producer / consumer
├── requirements.txt        # kafka-python, requests, pymongo
├── .env.example            # variables surchargeables (copier en .env)
├── scripts/check_mongo.js  # vérification des 3 collections (mongosh)
└── src/
    ├── config.py           # villes, champs, connexions (variables d'environnement)
    ├── producer.py         # API Open-Meteo -> Kafka
    ├── cleaning.py         # filtrage, nulls, bornes, doublons, agrégats (fonctions pures)
    ├── storage.py          # accès MongoDB, index, upserts
    └── consumer.py         # Kafka -> nettoyage -> MongoDB
```

## Traitement des données

**Producer** — un seul appel API pour les 6 villes (`hourly` + `current`, veille et jour courant,
fuseau `Africa/Casablanca`). Un message Kafka par ville, avec la ville comme clé.

**Consumer** — pour chaque message :
1. **Brut** : le message est stocké tel quel dans `raw_weather` avec ses métadonnées Kafka
   (index unique `city + fetched_at` : un message relu n'est jamais stocké deux fois).
2. **Filtrage des champs** : seules les variables `temperature_2m`, `relative_humidity_2m`,
   `precipitation`, `wind_speed_10m`, `weather_code` sont conservées ; les tableaux horaires sont
   transformés en un document par heure.
3. **Suppression des null** : toute heure avec un champ manquant est rejetée.
4. **Valeurs aberrantes** : rejet hors bornes (ex. température hors [-30, 60] °C, humidité hors [0, 100] %).
5. **Doublons** : dans un message (même ville + heure) puis entre messages successifs grâce à
   l'index unique `city + time` de `clean_weather` (upsert).
6. **Agrégation par ville et par jour** : recalculée à partir de toutes les heures propres en base
   pour les jours concernés, puis upsert dans `daily_weather_stats`.

## Collections MongoDB (base `meteo_maroc`)

| Collection            | 1 document = | Champs principaux |
|-----------------------|--------------|-------------------|
| `raw_weather`         | 1 message Kafka | `city`, `fetched_at`, `current`, `hourly`, `kafka{topic,partition,offset}`, `ingested_at` |
| `clean_weather`       | 1 ville × 1 heure | `city`, `time`, `date`, `hour`, `temperature_2m`, `relative_humidity_2m`, `precipitation`, `wind_speed_10m`, `weather_code` |
| `daily_weather_stats` | 1 ville × 1 jour | `temp_min`, `temp_max`, `temp_avg`, `humidity_avg`, `precipitation_total`, `wind_speed_max`, `dominant_weather_code`, `hours_count` |

## Commandes utiles

```bash
# État et logs
docker compose ps -a
docker compose logs -f producer consumer

# Messages dans Kafka
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic weather_raw --from-beginning --max-messages 3 --property print.key=true
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group weather-cleaner

# MongoDB
docker exec mongodb mongosh -u admin -p admin123 --quiet --file /scripts/check_mongo.js
docker exec -it mongodb mongosh -u admin -p admin123 meteo_maroc --authenticationDatabase admin

# Arrêt (-v supprime aussi les données)
docker compose down
docker compose down -v
```

> Sous **Git Bash**, préfixer les commandes `docker exec` contenant un chemin `/opt/...` ou `/scripts/...`
> par `MSYS_NO_PATHCONV=1` (sinon Git Bash convertit le chemin en chemin Windows). PowerShell n'a pas ce problème.

## Configuration

Les valeurs par défaut fonctionnent sans fichier `.env`. Pour les modifier : `cp .env.example .env`
puis éditer (`POLL_INTERVAL_SECONDS`, `MONGO_PASSWORD`, `KAFKA_TOPIC`…).
