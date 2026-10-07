"""Nettoyage et agrégation des messages météo (fonctions pures, sans dépendance à Kafka ou MongoDB)."""
from collections import Counter, defaultdict

import config

# Bornes de plausibilité : une valeur hors bornes est considérée comme invalide
VALID_RANGES = {
    "temperature_2m": (-30, 60),
    "relative_humidity_2m": (0, 100),
    "precipitation": (0, 500),
    "wind_speed_10m": (0, 300),
    "weather_code": (0, 99),
}


def flatten_hourly(message: dict) -> list[dict]:
    """Transforme les tableaux 'hourly' d'Open-Meteo en une liste d'enregistrements (1 par heure)."""
    hourly = message.get("hourly") or {}
    times = hourly.get("time") or []
    records = []
    for i, t in enumerate(times):
        rec = {"city": message.get("city"), "time": t}
        for field in config.HOURLY_FIELDS:  # filtrage : seuls les champs utiles sont conservés
            values = hourly.get(field) or []
            rec[field] = values[i] if i < len(values) else None
        records.append(rec)
    return records


def clean_records(records: list[dict]) -> tuple[list[dict], dict]:
    """Supprime les enregistrements incomplets (null), hors bornes et les doublons (ville + heure)."""
    stats = {"total": len(records), "nulls": 0, "invalid": 0, "duplicates": 0}
    seen = {}
    for rec in records:
        if any(rec.get(k) is None for k in ("city", "time", *config.HOURLY_FIELDS)):
            stats["nulls"] += 1
            continue
        if any(not (lo <= rec[f] <= hi) for f, (lo, hi) in VALID_RANGES.items()):
            stats["invalid"] += 1
            continue
        key = (rec["city"], rec["time"])
        if key in seen:
            stats["duplicates"] += 1
        seen[key] = {
            "city": rec["city"],
            "time": rec["time"],                 # heure locale, format ISO "YYYY-MM-DDTHH:MM"
            "date": rec["time"][:10],
            "hour": int(rec["time"][11:13]),
            "temperature_2m": float(rec["temperature_2m"]),
            "relative_humidity_2m": int(rec["relative_humidity_2m"]),
            "precipitation": float(rec["precipitation"]),
            "wind_speed_10m": float(rec["wind_speed_10m"]),
            "weather_code": int(rec["weather_code"]),
        }
    clean = sorted(seen.values(), key=lambda r: (r["city"], r["time"]))
    stats["clean"] = len(clean)
    return clean, stats


def aggregate_daily(records: list[dict]) -> list[dict]:
    """Agrège les enregistrements nettoyés par ville et par jour."""
    groups = defaultdict(list)
    for rec in records:
        groups[(rec["city"], rec["date"])].append(rec)

    daily = []
    for (city, date), recs in sorted(groups.items()):
        temps = [r["temperature_2m"] for r in recs]
        hums = [r["relative_humidity_2m"] for r in recs]
        daily.append({
            "city": city,
            "date": date,
            "hours_count": len(recs),
            "temp_min": min(temps),
            "temp_max": max(temps),
            "temp_avg": round(sum(temps) / len(temps), 2),
            "humidity_avg": round(sum(hums) / len(hums), 1),
            "precipitation_total": round(sum(r["precipitation"] for r in recs), 2),
            "wind_speed_max": max(r["wind_speed_10m"] for r in recs),
            "dominant_weather_code": Counter(r["weather_code"] for r in recs).most_common(1)[0][0],
        })
    return daily
