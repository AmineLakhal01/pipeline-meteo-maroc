<div align="center">

# 🌦️ Pipeline de données météo en temps réel — Maroc

**ISGA — Institut Supérieur du Génie Appliqué**
 2ᵉ année — Cycle Ingénieur ISI
Année universitaire 2026 – 2026

**Module :** _à compléter (ex. Big Data / Ingénierie des données)_
**Réalisé par :** Mohamed Amine Lakhal<br>
**Encadré par :** M. Youness Tabii

</div>

---

## 1. Contexte et objectifs

Ce projet met en place une **chaîne de traitement de données en temps réel** pour suivre la météo de
six grandes villes marocaines : **Rabat, Casablanca, Marrakech, Fès, Tanger et Agadir**.

Objectifs pédagogiques :

- consommer une **API publique** (Open-Meteo, gratuite et sans clé) ;
- découpler la collecte et le traitement grâce à un **bus de messages Kafka** ;
- **nettoyer** les données (filtrage des champs, suppression des valeurs nulles et des doublons) ;
- produire des **agrégations par ville et par jour** ;
- stocker le résultat dans une base **NoSQL MongoDB** ;
- **conteneuriser** l'ensemble avec Docker Compose (un seul `docker compose up` lance tout).

## 2. Architecture

```
 ┌──────────────┐   HTTP    ┌─────────────┐  JSON   ┌──────────────────────────┐
 │  API         │ ────────► │ producer.py │ ──────► │ Kafka — topic meteo_brute │
 │  Open-Meteo  │  (60 s)   └─────────────┘         │ 3 partitions, clé = ville │
 └──────────────┘                                   └────────────┬─────────────┘
                                                                 │
                                                                 ▼
                                          ┌───────────────────────────────────────┐
                                          │ consumer.py                           │
                                          │  1. stockage brut                     │
                                          │  2. filtrage + renommage en français  │
                                          │  3. suppression null / aberrants      │
                                          │  4. dédoublonnage                     │
                                          │  5. agrégation ville × jour           │
                                          └──────────────────┬────────────────────┘
                                                             ▼
                              ┌──────────────── MongoDB (base meteo_maroc) ───────────────┐
                              │  meteo_brute      meteo_nettoyee      stats_journalieres  │
                              └───────────────────────────────▲───────────────────────────┘
                                                              │
                                              mongo-express (http://localhost:8081)
```

## 3. Technologies

| Composant      | Technologie                         | Rôle |
|----------------|-------------------------------------|------|
| Source         | API Open-Meteo                      | Données météo horaires et conditions actuelles |
| Messagerie     | Apache Kafka 3.8 (mode KRaft)       | File de messages, sans Zookeeper |
| Traitement     | Python 3.12 (`kafka-python`, `requests`, `pymongo`) | Producer et consumer |
| Stockage       | MongoDB 7.0                         | Base NoSQL orientée documents |
| Visualisation  | mongo-express 1.0                   | Interface web d'exploration de MongoDB |
| Orchestration  | Docker / Docker Compose             | Conteneurisation et ordre de démarrage |

## 4. Lancer le projet

**Prérequis :** Docker Desktop (moteur démarré) et Git.

```bash
docker compose up -d --build
```

L'ordre de démarrage est garanti par les `healthcheck` / `depends_on` :
`kafka` + `mongodb` (healthy) → `kafka-init` (crée le topic puis s'arrête) → `producer` + `consumer` + `mongo-express`.

Interface web : **http://localhost:8081** → base `meteo_maroc`.

### Lancement étape par étape (pour la démonstration)

```bash
docker compose up -d kafka mongodb        # 1. infrastructure (attendre "healthy" avec docker compose ps)
docker compose up kafka-init              # 2. création du topic meteo_brute
docker compose up -d --build producer     # 3. collecte API -> Kafka
docker compose up -d --build consumer     # 4. Kafka -> nettoyage -> MongoDB
docker compose up -d mongo-express        # 5. interface web
```

## 5. Services Docker

| Service         | Image                     | Port hôte | Rôle |
|-----------------|---------------------------|-----------|------|
| `kafka`         | apache/kafka:3.8.0        | 29092     | Broker Kafka (KRaft) |
| `kafka-init`    | apache/kafka:3.8.0        | –         | Crée le topic `meteo_brute` (3 partitions) |
| `mongodb`       | mongo:7.0                 | 27017     | Base de données (utilisateur `admin` / `admin123`) |
| `mongo-express` | mongo-express:1.0.2       | 8081      | Interface web (sans login, usage local) |
| `producer`      | build local (Python 3.12) | –         | Appelle Open-Meteo toutes les 60 s et publie dans Kafka |
| `consumer`      | build local (Python 3.12) | –         | Lit Kafka, nettoie, agrège, écrit dans MongoDB |

## 6. Structure du projet

```
.
├── docker-compose.yml       # orchestration des 6 services
├── Dockerfile               # image commune producer / consumer
├── requirements.txt         # kafka-python, requests, pymongo
├── .env.example             # variables surchargeables (copier en .env)
├── scripts/check_mongo.js   # vérification des 3 collections (mongosh)
└── src/
    ├── config.py            # villes, champs, codes météo, connexions
    ├── producer.py          # API Open-Meteo -> Kafka
    ├── cleaning.py          # filtrage, null, bornes, doublons, agrégats (fonctions pures)
    ├── storage.py           # accès MongoDB, index, upserts
    └── consumer.py          # Kafka -> nettoyage -> MongoDB
```

## 7. Traitement des données

**Producer** — un seul appel API pour les 6 villes (données horaires de la veille et du jour,
conditions actuelles, fuseau `Africa/Casablanca`). Un message Kafka par ville, la ville servant de
clé (toutes les données d'une même ville arrivent dans la même partition, donc dans l'ordre).

**Consumer** — pour chaque message :

1. **Stockage brut** dans `meteo_brute`, avec les métadonnées Kafka (topic, partition, offset).
   Index unique `ville + collecte_le` : un message relu n'est jamais stocké deux fois.
2. **Filtrage des champs** : seules 5 variables sont conservées et renommées en français
   (`temperature_2m` → `temperature_c`, `relative_humidity_2m` → `humidite_pct`,
   `precipitation` → `precipitations_mm`, `wind_speed_10m` → `vent_kmh`, `weather_code` → `code_meteo`).
   Les tableaux horaires sont transformés en un document par heure.
3. **Suppression des null** : toute heure avec un champ manquant est rejetée.
4. **Valeurs aberrantes** : rejet hors bornes (température hors [-30 ; 60] °C, humidité hors [0 ; 100] %…).
5. **Enrichissement** : le code météo WMO est traduit en clair (« Ciel dégagé », « Pluie faible »…).
6. **Dédoublonnage** : dans un message (même ville + heure), puis entre deux appels successifs
   grâce à l'index unique `ville + horodatage` de `meteo_nettoyee` (upsert).
7. **Agrégation par ville et par jour** : recalculée à partir de toutes les heures propres en base,
   puis upsert dans `stats_journalieres`.

## 8. Collections MongoDB (base `meteo_maroc`)

| Collection           | 1 document =       | Champs principaux |
|----------------------|--------------------|-------------------|
| `meteo_brute`        | 1 message Kafka    | `ville`, `collecte_le`, `conditions_actuelles`, `donnees_horaires`, `kafka{topic, partition, offset}`, `insere_le` |
| `meteo_nettoyee`     | 1 ville × 1 heure  | `ville`, `horodatage`, `date`, `heure`, `temperature_c`, `humidite_pct`, `precipitations_mm`, `vent_kmh`, `code_meteo`, `description_meteo` |
| `stats_journalieres` | 1 ville × 1 jour   | `temp_min_c`, `temp_max_c`, `temp_moy_c`, `humidite_moy_pct`, `precipitations_total_mm`, `vent_max_kmh`, `meteo_dominante`, `nb_heures` |

Exemple de document `stats_journalieres` :

```json
{
  "ville": "Marrakech",
  "date": "2026-10-06",
  "nb_heures": 24,
  "temp_min_c": 19.6,
  "temp_max_c": 34.1,
  "temp_moy_c": 26.69,
  "humidite_moy_pct": 41.7,
  "precipitations_total_mm": 0.1,
  "vent_max_kmh": 12.9,
  "code_meteo_dominant": 1,
  "meteo_dominante": "Principalement dégagé"
}
```

## 9. Commandes utiles

```bash
# État et logs
docker compose ps -a
docker compose logs -f producer consumer

# Messages dans Kafka
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic meteo_brute --from-beginning --max-messages 3 --property print.key=true
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group nettoyeur-meteo

# MongoDB
docker exec mongodb mongosh -u admin -p admin123 --quiet --file /scripts/check_mongo.js
docker exec -it mongodb mongosh -u admin -p admin123 meteo_maroc --authenticationDatabase admin

# Arrêt (-v supprime aussi les données)
docker compose down
docker compose down -v
```

> Sous **Git Bash**, préfixer les commandes `docker exec` contenant un chemin `/opt/...` ou `/scripts/...`
> par `MSYS_NO_PATHCONV=1`. PowerShell n'a pas ce problème.

## 10. Difficultés rencontrées et solutions

| Problème | Cause | Solution |
|----------|-------|----------|
| Après un redémarrage, les nouveaux messages étaient ignorés comme doublons | L'image `apache/kafka` écrit par défaut dans `/tmp/kafka-logs`, hors du volume : le topic repartait à l'offset 0 alors que MongoDB conservait les anciens offsets | `KAFKA_LOG_DIRS` pointé vers le volume + `CLUSTER_ID` fixe ; dédoublonnage du brut par `ville + collecte_le` au lieu de `partition + offset` |
| Avertissements `NotCoordinatorForGroupError` au 1ᵉʳ démarrage du consumer | Kafka crée son topic interne `__consumer_offsets` à la première utilisation d'un groupe | Comportement normal et transitoire (≈ 2 s), le client réessaie automatiquement |
| Doublons entre deux appels API | Chaque appel renvoie les mêmes 48 heures | Index unique `ville + horodatage` et écriture en upsert |
| Ordre de démarrage des conteneurs | Le producer démarrait avant que Kafka soit prêt | `healthcheck` + `depends_on: condition: service_healthy` / `service_completed_successfully` |

## 11. Perspectives

- Tableau de bord (Grafana, Streamlit ou Power BI) branché sur `stats_journalieres` ;
- alertes (canicule, fortes pluies) via un second consumer Kafka ;
- traitement distribué avec Spark Structured Streaming ou Kafka Streams ;
- ajout de villes supplémentaires et historisation longue durée.

## 12. Configuration

Les valeurs par défaut fonctionnent sans fichier `.env`. Pour les modifier : copier `.env.example`
en `.env`, puis éditer (`POLL_INTERVAL_SECONDS`, `MONGO_PASSWORD`, `KAFKA_TOPIC`…).
