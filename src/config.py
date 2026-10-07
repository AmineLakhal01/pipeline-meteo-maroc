"""Configuration partagée par le producer et le consumer (lue depuis les variables d'environnement)."""
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_raw")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "weather-cleaner")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@localhost:27017/?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "meteo_maroc")

# Collections MongoDB
COLL_RAW = "raw_weather"            # messages bruts tels que reçus de Kafka
COLL_CLEAN = "clean_weather"        # mesures horaires nettoyées (1 doc par ville + heure)
COLL_DAILY = "daily_weather_stats"  # agrégats par ville et par jour

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
TIMEZONE = "Africa/Casablanca"

# Variables horaires demandées à l'API et conservées après nettoyage
HOURLY_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
]

CITIES = {
    "Rabat":      {"latitude": 34.0209, "longitude": -6.8416},
    "Casablanca": {"latitude": 33.5731, "longitude": -7.5898},
    "Marrakech":  {"latitude": 31.6295, "longitude": -7.9811},
    "Fes":        {"latitude": 34.0331, "longitude": -5.0003},
    "Tanger":     {"latitude": 35.7595, "longitude": -5.8340},
    "Agadir":     {"latitude": 30.4278, "longitude": -9.5981},
}
