// =============================================================================
// Stripe Polyglot — Requêtes NoSQL (MongoDB 7)
// Livrable 8 de l'énoncé — AIA RNCP41993, Bloc 2
//
// Chaque requête lit une collection RÉELLEMENT alimentée par le code :
//   transaction_logs, logs, ml_features, fraud_alerts → producers/mongo_writer.py
//   ml_monitoring                                     → ml/monitor.py
// (user_interactions et customer_feedback ont un schéma et des index mais
//  aucun producteur : pas de requête ici, cf. docs/NOSQL_DATA_MODEL.md.)
//
// Vérifiées par `make queries-check` : mongosh s'arrête au premier échec.
// =============================================================================

db = db.getSiblingDB("stripe_nosql");

const DAY_MS = 24 * 60 * 60 * 1000;
const since24h = new Date(Date.now() - DAY_MS);

function show(title, cursorOrArray) {
  print(`\n== ${title} ==`);
  const rows = Array.isArray(cursorOrArray) ? cursorOrArray : cursorOrArray.toArray();
  rows.forEach((r) => printjson(r));
  print(`(${rows.length} document(s))`);
}

// -----------------------------------------------------------------------------
// 1. fraud_alerts — alertes des dernières 24 h par décision et version de modèle
// Usage : dashboard Risk, onglet "Performance ML" (comparaison rules vs xgboost).
// Index utilisé : { decision: 1, created_at: -1 } (init/mongo/01_init_collections.js).
// -----------------------------------------------------------------------------
show("1. Alertes 24 h par décision et model_version", db.fraud_alerts.aggregate([
  { $match: { created_at: { $gte: since24h } } },
  {
    $group: {
      _id: { decision: "$decision", model_version: "$model_version" },
      alerts: { $sum: 1 },
      avg_score: { $avg: "$fraud_score" },
      total_amount_cents: { $sum: "$amount" },
    },
  },
  {
    $project: {
      _id: 0,
      decision: "$_id.decision",
      model_version: "$_id.model_version",
      alerts: 1,
      avg_score: { $round: ["$avg_score", 4] },
      total_amount: { $round: [{ $divide: ["$total_amount_cents", 100] }, 2] },
    },
  },
  { $sort: { alerts: -1 } },
]));

// -----------------------------------------------------------------------------
// 2. fraud_alerts — règles les plus déclenchées (moteur à règles uniquement)
// rules_triggered est un tableau EMBARQUÉ (embedding) : $unwind le déplie.
// Vide pour xgboost-v1, qui ne produit pas de règles discrètes.
// -----------------------------------------------------------------------------
show("2. Règles déclenchées (rule-based-v1)", db.fraud_alerts.aggregate([
  { $match: { model_version: "rule-based-v1" } },
  { $unwind: "$rules_triggered" },
  { $group: { _id: "$rules_triggered", count: { $sum: 1 }, avg_score: { $avg: "$fraud_score" } } },
  { $project: { _id: 0, rule: "$_id", count: 1, avg_score: { $round: ["$avg_score", 4] } } },
  { $sort: { count: -1 } },
]));

// -----------------------------------------------------------------------------
// 3. fraud_alerts — géographie du risque
// Usage : ajuster la liste HIGH_RISK_COUNTRIES du scorer.
// -----------------------------------------------------------------------------
show("3. Alertes par pays IP (top 10)", db.fraud_alerts.aggregate([
  {
    $group: {
      _id: "$ip_country",
      alerts: { $sum: 1 },
      blocks: { $sum: { $cond: [{ $eq: ["$decision", "block"] }, 1, 0] } },
      avg_score: { $avg: "$fraud_score" },
    },
  },
  {
    $project: {
      _id: 0,
      ip_country: "$_id",
      alerts: 1,
      blocks: 1,
      block_share_pct: { $round: [{ $multiply: [{ $divide: ["$blocks", "$alerts"] }, 100] }, 1] },
      avg_score: { $round: ["$avg_score", 4] },
    },
  },
  { $sort: { alerts: -1 } },
  { $limit: 10 },
]));

// -----------------------------------------------------------------------------
// 4. transaction_logs — volume horaire des 24 dernières heures
// Usage : supervision du débit du pipeline (un trou = scorer ou writer arrêté).
// -----------------------------------------------------------------------------
show("4. Transactions journalisées par heure (24 h)", db.transaction_logs.aggregate([
  { $match: { created_at: { $gte: since24h } } },
  { $group: { _id: { $dateTrunc: { date: "$created_at", unit: "hour" } }, events: { $sum: 1 } } },
  { $project: { _id: 0, hour: "$_id", events: 1 } },
  { $sort: { hour: -1 } },
]));

// -----------------------------------------------------------------------------
// 5. transaction_logs — historique brut d'une transaction (enquête fraude)
// Référence faible txn_id → PostgreSQL transactions.txn_id. Le payload
// complet (score, vélocité, règles) est lu en une seule requête, sans JOIN.
// Index utilisé : { txn_id: 1 }.
// -----------------------------------------------------------------------------
const lastAlert = db.fraud_alerts.find({}, { txn_id: 1 }).sort({ created_at: -1 }).limit(1).toArray();
if (lastAlert.length > 0) {
  show(`5. Historique de la transaction ${lastAlert[0].txn_id}`, db.transaction_logs.find(
    { txn_id: lastAlert[0].txn_id },
    { _id: 0, txn_id: 1, event_type: 1, created_at: 1, "payload.fraud_score": 1, "payload.decision": 1,
      "payload.velocity_1h": 1, "payload.model_version": 1 },
  ));
} else {
  print("\n== 5. Historique d'une transaction == (aucune alerte pour l'instant)");
}

// -----------------------------------------------------------------------------
// 6. ml_features — clients à forte vélocité (snapshot par client, upsert)
// Un document par customer_id : c'est l'état features le plus récent,
// pas un historique. Index utilisé : { customer_id: 1 } unique.
// -----------------------------------------------------------------------------
show("6. Clients à vélocité 1 h > 10", db.ml_features.find(
  { velocity_1h: { $gt: 10 } },
  { _id: 0, customer_id: 1, velocity_1h: 1, velocity_24h: 1, last_fraud_score: 1, last_decision: 1, last_updated: 1 },
).sort({ velocity_1h: -1 }).limit(10));

// -----------------------------------------------------------------------------
// 7. ml_features — répartition des dernières décisions par modèle
// -----------------------------------------------------------------------------
show("7. Dernière décision par client, par modèle", db.ml_features.aggregate([
  { $group: { _id: { model: "$last_model_version", decision: "$last_decision" }, customers: { $sum: 1 } } },
  { $project: { _id: 0, model: "$_id.model", decision: "$_id.decision", customers: 1 } },
  { $sort: { model: 1, customers: -1 } },
]));

// -----------------------------------------------------------------------------
// 8. ml_monitoring — tendance dérive et précision servie (10 derniers contrôles)
// drift et performance sont des sous-documents EMBARQUÉS écrits par
// ml/monitor.py (Evidently + mesure servie).
// -----------------------------------------------------------------------------
show("8. Derniers contrôles ml-monitor", db.ml_monitoring.find(
  {},
  { _id: 0, checked_at: 1, model_version: 1, status: 1, "drift.drift_share": 1,
    "performance.precision": 1, "performance.recall": 1, alert_reasons: 1, retrain_triggered: 1 },
).sort({ checked_at: -1 }).limit(10));

// -----------------------------------------------------------------------------
// 9. logs — latence moyenne et p95 par service ($percentile : MongoDB >= 7.0)
// -----------------------------------------------------------------------------
show("9. Latence par service (24 h)", db.logs.aggregate([
  { $match: { type: "access", created_at: { $gte: since24h }, "context.duration_ms": { $exists: true } } },
  {
    $group: {
      _id: "$service",
      requests: { $sum: 1 },
      avg_ms: { $avg: "$context.duration_ms" },
      p95: { $percentile: { input: "$context.duration_ms", p: [0.95], method: "approximate" } },
    },
  },
  {
    $project: {
      _id: 0,
      service: "$_id",
      requests: 1,
      avg_ms: { $round: ["$avg_ms", 1] },
      p95_ms: { $round: [{ $arrayElemAt: ["$p95", 0] }, 1] },
    },
  },
]));

// -----------------------------------------------------------------------------
// 10. logs — erreurs des dernières 24 h (supervision SRE)
// -----------------------------------------------------------------------------
show("10. Erreurs 24 h par service", db.logs.aggregate([
  { $match: { level: "ERROR", created_at: { $gte: since24h } } },
  { $group: { _id: { service: "$service", message: "$message" }, count: { $sum: 1 }, last_seen: { $max: "$created_at" } } },
  { $project: { _id: 0, service: "$_id.service", message: "$_id.message", count: 1, last_seen: 1 } },
  { $sort: { count: -1 } },
  { $limit: 20 },
]));

// -----------------------------------------------------------------------------
// 11. Contrôle RGPD — index TTL effectivement en place
// -----------------------------------------------------------------------------
const ttl = [];
["transaction_logs", "user_interactions", "logs"].forEach((c) => {
  db.getCollection(c).getIndexes()
    .filter((i) => i.expireAfterSeconds !== undefined)
    .forEach((i) => ttl.push({ collection: c, index: i.name, expire_after_days: i.expireAfterSeconds / 86400 }));
});
show("11. Index TTL (purge RGPD)", ttl);
