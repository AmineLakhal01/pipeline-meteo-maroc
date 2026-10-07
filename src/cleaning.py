"""Nettoyage et agrégation des messages météo (fonctions pures, sans dépendance à Kafka ou MongoDB)."""
from collections import Counter, defaultdict

import config

# Bornes de plausibilité : une valeur hors bornes est considérée comme invalide
VALID_RANGES = {
    "temperature_c": (-30, 60),
    "humidite_pct": (0, 100),
    "precipitations_mm": (0, 500),
    "vent_kmh": (0, 300),
    "code_meteo": (0, 99),
}


def describe(code: int) -> str:
    return config.WMO_DESCRIPTIONS.get(code, f"Code météo {code}")


def flatten_hourly(message: dict) -> list[dict]:
    """Transforme les tableaux horaires d'Open-Meteo en une liste d'enregistrements (1 par heure)."""
    hourly = message.get("donnees_horaires") or {}
    times = hourly.get("time") or []
    records = []
    for i, t in enumerate(times):
        rec = {"ville": message.get("ville"), "horodatage": t}
        for api_field, fr_field in config.HOURLY_FIELDS.items():  # filtrage + renommage en français
            values = hourly.get(api_field) or []
            rec[fr_field] = values[i] if i < len(values) else None
        records.append(rec)
    return records


def clean_records(records: list[dict]) -> tuple[list[dict], dict]:
    """Supprime les enregistrements incomplets (null), hors bornes et les doublons (ville + heure)."""
    stats = {"total": len(records), "nulls": 0, "invalides": 0, "doublons": 0}
    seen = {}
    for rec in records:
        if any(rec.get(k) is None for k in ("ville", "horodatage", *VALID_RANGES)):
            stats["nulls"] += 1
            continue
        if any(not (lo <= rec[f] <= hi) for f, (lo, hi) in VALID_RANGES.items()):
            stats["invalides"] += 1
            continue
        key = (rec["ville"], rec["horodatage"])
        if key in seen:
            stats["doublons"] += 1
        code = int(rec["code_meteo"])
        seen[key] = {
            "ville": rec["ville"],
            "horodatage": rec["horodatage"],     # heure locale, format ISO "AAAA-MM-JJTHH:MM"
            "date": rec["horodatage"][:10],
            "heure": int(rec["horodatage"][11:13]),
            "temperature_c": float(rec["temperature_c"]),
            "humidite_pct": int(rec["humidite_pct"]),
            "precipitations_mm": float(rec["precipitations_mm"]),
            "vent_kmh": float(rec["vent_kmh"]),
            "code_meteo": code,
            "description_meteo": describe(code),
        }
    clean = sorted(seen.values(), key=lambda r: (r["ville"], r["horodatage"]))
    stats["propres"] = len(clean)
    return clean, stats


def aggregate_daily(records: list[dict]) -> list[dict]:
    """Agrège les enregistrements nettoyés par ville et par jour."""
    groups = defaultdict(list)
    for rec in records:
        groups[(rec["ville"], rec["date"])].append(rec)

    daily = []
    for (ville, date), recs in sorted(groups.items()):
        temps = [r["temperature_c"] for r in recs]
        hums = [r["humidite_pct"] for r in recs]
        code = Counter(r["code_meteo"] for r in recs).most_common(1)[0][0]
        daily.append({
            "ville": ville,
            "date": date,
            "nb_heures": len(recs),
            "temp_min_c": min(temps),
            "temp_max_c": max(temps),
            "temp_moy_c": round(sum(temps) / len(temps), 2),
            "humidite_moy_pct": round(sum(hums) / len(hums), 1),
            "precipitations_total_mm": round(sum(r["precipitations_mm"] for r in recs), 2),
            "vent_max_kmh": max(r["vent_kmh"] for r in recs),
            "code_meteo_dominant": code,
            "meteo_dominante": describe(code),
        })
    return daily
