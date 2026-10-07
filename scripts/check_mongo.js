// Vérification des 3 collections. Usage :
//   docker exec mongodb mongosh -u admin -p admin123 --quiet --file /scripts/check_mongo.js
const d = db.getSiblingDB("meteo_maroc");

print("== Collections et nombre de documents ==");
["raw_weather", "clean_weather", "daily_weather_stats"].forEach(c => print(c.padEnd(22), d[c].countDocuments()));

print("\n== clean_weather : doublons (city, time) ? ==");
const dup = d.clean_weather.aggregate([
  { $group: { _id: { c: "$city", t: "$time" }, n: { $sum: 1 } } },
  { $match: { n: { $gt: 1 } } },
]).toArray();
print(dup.length ? `${dup.length} doublon(s) !` : "aucun doublon");

print("\n== clean_weather : heures par ville ==");
d.clean_weather.aggregate([{ $group: { _id: "$city", heures: { $sum: 1 } } }, { $sort: { _id: 1 } }])
  .forEach(r => print(r._id.padEnd(12), r.heures));

print("\n== clean_weather : exemple ==");
printjson(d.clean_weather.findOne({ city: "Rabat" }, { _id: 0, updated_at: 0 }));

print("\n== daily_weather_stats ==");
d.daily_weather_stats.find({}, { _id: 0, city: 1, date: 1, hours_count: 1, temp_min: 1, temp_max: 1, temp_avg: 1, precipitation_total: 1 })
  .sort({ city: 1, date: 1 }).forEach(r => print(JSON.stringify(r)));

print("\n== raw_weather : champs du dernier document ==");
const last = d.raw_weather.find().sort({ ingested_at: -1 }).limit(1).toArray()[0];
if (last) print(Object.keys(last).join(", "), "\n kafka =", JSON.stringify(last.kafka));
