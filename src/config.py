"""Configuration partagée par le producer et le consumer (lue depuis les variables d'environnement)."""
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "meteo_brute")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "nettoyeur-meteo")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@localhost:27017/?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "meteo_maroc")

# Collections MongoDB
COLL_RAW = "meteo_brute"            # messages bruts tels que reçus de Kafka
COLL_CLEAN = "meteo_nettoyee"       # mesures horaires nettoyées (1 doc par ville + heure)
COLL_DAILY = "stats_journalieres"   # agrégats par ville et par jour

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
TIMEZONE = "Africa/Casablanca"

# Variables horaires demandées à l'API -> nom du champ en français après nettoyage
HOURLY_FIELDS = {
    "temperature_2m": "temperature_c",
    "relative_humidity_2m": "humidite_pct",
    "precipitation": "precipitations_mm",
    "wind_speed_10m": "vent_kmh",
    "weather_code": "code_meteo",
}

CITIES = {
    "Rabat":      {"latitude": 34.0209, "longitude": -6.8416},
    "Casablanca": {"latitude": 33.5731, "longitude": -7.5898},
    "Marrakech":  {"latitude": 31.6295, "longitude": -7.9811},
    "Fès":        {"latitude": 34.0331, "longitude": -5.0003},
    "Tanger":     {"latitude": 35.7595, "longitude": -5.8340},
    "Agadir":     {"latitude": 30.4278, "longitude": -9.5981},
}

# Codes météo WMO renvoyés par Open-Meteo -> description en français
WMO_DESCRIPTIONS = {
    0: "Ciel dégagé", 1: "Principalement dégagé", 2: "Partiellement nuageux", 3: "Couvert",
    45: "Brouillard", 48: "Brouillard givrant",
    51: "Bruine légère", 53: "Bruine modérée", 55: "Bruine dense",
    56: "Bruine verglaçante légère", 57: "Bruine verglaçante dense",
    61: "Pluie faible", 63: "Pluie modérée", 65: "Pluie forte",
    66: "Pluie verglaçante faible", 67: "Pluie verglaçante forte",
    71: "Neige faible", 73: "Neige modérée", 75: "Neige forte", 77: "Grains de neige",
    80: "Averses faibles", 81: "Averses modérées", 82: "Averses violentes",
    85: "Averses de neige faibles", 86: "Averses de neige fortes",
    95: "Orage", 96: "Orage avec grêle faible", 99: "Orage avec grêle forte",
}
