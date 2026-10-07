// Vérification des 3 collections. Usage :
//   docker exec mongodb mongosh -u admin -p admin123 --quiet --file /scripts/check_mongo.js
const d = db.getSiblingDB("meteo_maroc");

print("== Collections et nombre de documents ==");
["meteo_brute", "meteo_nettoyee", "stats_journalieres"].forEach(c => print(c.padEnd(20), d[c].countDocuments()));

print("\n== meteo_nettoyee : doublons (ville, horodatage) ? ==");
const dup = d.meteo_nettoyee.aggregate([
  { $group: { _id: { v: "$ville", h: "$horodatage" }, n: { $sum: 1 } } },
  { $match: { n: { $gt: 1 } } },
]).toArray();
print(dup.length ? `${dup.length} doublon(s) !` : "aucun doublon");

print("\n== meteo_nettoyee : heures par ville ==");
d.meteo_nettoyee.aggregate([{ $group: { _id: "$ville", heures: { $sum: 1 } } }, { $sort: { _id: 1 } }])
  .forEach(r => print(r._id.padEnd(12), r.heures));

print("\n== meteo_nettoyee : exemple ==");
printjson(d.meteo_nettoyee.findOne({ ville: "Rabat" }, { _id: 0, mis_a_jour_le: 0 }));

print("\n== stats_journalieres ==");
d.stats_journalieres.find({}, { _id: 0, ville: 1, date: 1, nb_heures: 1, temp_min_c: 1, temp_max_c: 1, temp_moy_c: 1, precipitations_total_mm: 1, meteo_dominante: 1 })
  .sort({ ville: 1, date: 1 }).forEach(r => print(JSON.stringify(r)));

print("\n== meteo_brute : champs du dernier document ==");
const last = d.meteo_brute.find().sort({ insere_le: -1 }).limit(1).toArray()[0];
if (last) print(Object.keys(last).join(", "), "\n kafka =", JSON.stringify(last.kafka));
