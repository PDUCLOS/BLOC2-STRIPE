// =====================================================================
// MongoDB — Initialisation des collections, index, TTL et utilisateurs
// Exécuté automatiquement au 1er démarrage du conteneur mongo
// (les scripts .js de /docker-entrypoint-initdb.d tournent dans l'ordre alpha)
// =====================================================================

// Sélectionne la DB applicative
db = db.getSiblingDB(process.env.MONGO_DB || "stripe_nosql");

// ── transaction_logs ──────────────────────────────────────
// Log append-only de tous les événements de transaction (depuis Flink)
db.createCollection("transaction_logs");
db.transaction_logs.createIndex({ txn_id: 1 });
db.transaction_logs.createIndex(
  { created_at: 1 },
  { expireAfterSeconds: 7776000 }   // TTL 90 jours (RGPD)
);
db.transaction_logs.createIndex({ event_type: 1, created_at: -1 });

// ── user_interactions ─────────────────────────────────────
// Clics, navigations, échecs d'auth — pour analyse comportementale
db.createCollection("user_interactions");
db.user_interactions.createIndex({ customer_id: 1, timestamp: -1 });
db.user_interactions.createIndex(
  { timestamp: 1 },
  { expireAfterSeconds: 2592000 }   // TTL 30 jours
);

// ── ml_features ───────────────────────────────────────────
// Features pré-calculées pour le scoring ML (snapshot consolidé)
db.createCollection("ml_features");
db.ml_features.createIndex({ customer_id: 1 }, { unique: true });
db.ml_features.createIndex({ last_updated: 1 });

// ── customer_feedback ─────────────────────────────────────
// Disputes, contestations, notes satisfaction
db.createCollection("customer_feedback");
db.customer_feedback.createIndex({ customer_id: 1 });
db.customer_feedback.createIndex({ merchant_id: 1 });
db.customer_feedback.createIndex({ created_at: -1 });

// ── fraud_alerts ──────────────────────────────────────────
// Alertes émises par Flink (decision = review|block)
db.createCollection("fraud_alerts");
db.fraud_alerts.createIndex({ txn_id: 1 });
db.fraud_alerts.createIndex({ customer_id: 1, created_at: -1 });
db.fraud_alerts.createIndex({ decision: 1, created_at: -1 });
db.fraud_alerts.createIndex({ created_at: -1 });

print("[OK] Mongo collections et index créés pour " + (process.env.MONGO_DB || "stripe_nosql"));
