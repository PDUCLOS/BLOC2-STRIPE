# NoSQL Data Model — MongoDB

Certification AIA RNCP41993 — Bloc 2

Schéma détaillé du système NoSQL (MongoDB), stratégies de gestion des
données non structurées, relations, et indexation. Implémenté par
[`init/mongo/01_init_collections.js`](../init/mongo/01_init_collections.js)
(collections + index) et
[`init/mongo/02_app_user.js`](../init/mongo/02_app_user.js) (utilisateur
applicatif). Complète `PRESENTATION.md §3.5`.

---

## 1. Pourquoi MongoDB pour ce périmètre (et pas Postgres)

Trois catégories de données ne s'accommodent pas bien d'un schéma
relationnel rigide, d'où le choix document :

1. **Volume append-only sans structure fixe** (`transaction_logs`) : chaque
   événement a un `payload` JSON dont la forme dépend du type d'événement
   — un schéma SQL figé imposerait des colonnes NULL en masse ou des
   migrations fréquentes
2. **Durée de vie courte et automatique** (TTL) : la suppression de masse
   par âge (RGPD) est native à MongoDB (`expireAfterSeconds`), alors
   qu'en SQL il faudrait un job cron `DELETE WHERE created_at < ...`
3. **Requêtes analytiques par agrégation sur des documents imbriqués**
   (ex. `rules_triggered` en tableau) — le pipeline d'agrégation MongoDB
   traite nativement les tableaux/objets imbriqués sans jointure

---

## 2. Collections — schéma document et justification

### 2.1 — `transaction_logs`

```js
{
  txn_id: "uuid",
  event_type: "transaction.scored",
  payload: { /* transaction complète, forme libre — dénormalisée depuis Kafka */ },
  source: "mongo-writer",
  created_at: ISODate
}
```

**Relation avec le monde relationnel** : `txn_id` référence
`transactions.txn_id` (Postgres) — **pas de foreign key** (impossible
cross-database), la cohérence est garantie applicativement par le pipeline
CDC (chaque transaction Postgres produit exactement un document ici via
`mongo_writer.py`). C'est une relation de type **référence faible** :
`transaction_logs` peut survivre à la suppression de la transaction source
côté OLTP (les deux ont des cycles de vie différents — 90 jours TTL ici
contre conservation longue côté OLTP).

**Pourquoi dénormaliser le `payload` entier plutôt que republier les
champs individuellement ?** Ce document sert d'archive brute exploitable
même si le schéma Postgres évolue — un changement de colonne côté OLTP
n'oblige pas à migrer rétroactivement les logs déjà écrits.

### 2.2 — `fraud_alerts`

```js
{
  txn_id, customer_id, merchant_id,
  amount, currency, ip_country, device_type,
  fraud_score, decision,              // "review" | "block"
  rules_triggered: ["R1_high_amount", "R3_high_risk_geo"],  // tableau — 0 à N règles
  model_version, velocity_1h, velocity_24h,
  created_at
}
```

**Pourquoi un tableau pour `rules_triggered`** plutôt que des colonnes
booléennes `r1`, `r2`, `r3`... : le nombre de règles peut évoluer (ajout
d'une R6 sans migration de schéma) et une requête d'agrégation
(`$unwind` + `$group`) permet de compter facilement la fréquence de
déclenchement par règle — cas d'usage typique de l'équipe Risk (identifié
dans [`queries/mongodb_queries.js`](../queries/mongodb_queries.js) §2).

**Relation avec `transaction_logs`** : `fraud_alerts` est un
**sous-ensemble filtré et dénormalisé** de ce qui existe déjà dans
`transaction_logs.payload` — dénormalisation volontaire (duplication de
`fraud_score`, `decision`, etc.) pour que les requêtes du dashboard
(`fraud_alerts_mongo()` dans `dashboard/app.py`) n'aient pas à filtrer
dans un champ imbriqué `payload.decision` sur toute la collection
`transaction_logs`, plus volumineuse.

### 2.3 — `logs`

```js
{
  service: "mongo-writer",
  type: "access",
  level: "INFO",
  message: "transaction.scored txn_id=...",
  context: { txn_id, decision, duration_ms },
  created_at,
  ttl_expires_at   // TTL calculé explicitement, pas expireAfterSeconds sur created_at
}
```

Collection de logs opérationnels (monitoring technique), distincte de
`transaction_logs` (données métier) — séparation volontaire pour ne pas
mélanger investigation technique et audit métier dans les mêmes requêtes.

### 2.4 — `user_interactions`

```js
{ customer_id, timestamp, /* type d'interaction, contexte */ }
```

Prévue pour capturer clics/navigations/échecs d'auth (analyse
comportementale) — schéma volontairement souple, pas encore alimentée par
le pipeline actuel (pas de producer dédié), mais l'index et le TTL (30
jours) sont en place pour l'accueillir sans migration.

### 2.5 — `ml_features`

```js
{
  customer_id,
  last_amount, last_currency, last_country, last_device_type,
  velocity_1h, velocity_24h,
  last_fraud_score, last_decision, last_model_version,
  last_updated
}
```

Feature store offline pour l'entraînement ML — détaillé dans
[`ML_INTEGRATION_STRATEGY.md`](ML_INTEGRATION_STRATEGY.md). Index unique
sur `customer_id` : **une seule ligne par client**, à la différence des
autres collections qui sont append-only — celle-ci est un **instantané**
mis à jour (upsert), pas un historique. Alimentée en continu par
[`mongo_writer.py`](../producers/mongo_writer.py) (`update_one(...,
upsert=True)` à chaque transaction scorée) — vérifié en conditions réelles
(59 documents générés lors des tests de la stack complète).

### 2.6 — `customer_feedback`

```js
{ customer_id, merchant_id, /* contestation, note, contenu libre */, created_at }
```

Disputes/contestations — schéma libre car le contenu d'une contestation
n'est pas prévisible à l'avance (texte libre, pièces jointes potentielles).

---

## 3. Stratégie d'indexation

| Collection | Index | Type | Justification |
|---|---|---|---|
| `transaction_logs` | `{ txn_id: 1 }` | Simple | Recherche direct par transaction (jointure applicative avec Postgres) |
| `transaction_logs` | `{ created_at: 1 }`, `expireAfterSeconds: 7776000` | **TTL** | Purge RGPD automatique à 90 jours — pas de job de suppression à maintenir |
| `transaction_logs` | `{ event_type: 1, created_at: -1 }` | Composé | Filtrer par type d'événement puis trier par récence — pattern de requête du monitoring |
| `user_interactions` | `{ customer_id: 1, timestamp: -1 }` | Composé | "Historique d'un client trié par récence" — requête d'investigation typique |
| `user_interactions` | `{ timestamp: 1 }`, TTL 30j | TTL | Cycle de vie plus court que les transactions (données comportementales, moins critiques à conserver) |
| `ml_features` | `{ customer_id: 1 }`, `unique: true` | Unique | Garantit un seul document par client (c'est un instantané, pas un historique — cf. §2.5) |
| `fraud_alerts` | `{ txn_id: 1 }` | Simple | Recherche direct |
| `fraud_alerts` | `{ customer_id: 1, created_at: -1 }` | Composé | "Alertes d'un client donné, récentes d'abord" |
| `fraud_alerts` | `{ decision: 1, created_at: -1 }` | Composé | Filtrer par décision (`block`/`review`) — c'est la requête du dashboard `fraud_alerts_mongo()` |
| `fraud_alerts` | `{ created_at: -1 }` | Simple | Tri chronologique seul, pour les vues qui ne filtrent pas par décision |

**Principe appliqué** : chaque index composé a été choisi pour matcher un
pattern de requête réel du code (`dashboard/app.py`,
[`queries/mongodb_queries.js`](../queries/mongodb_queries.js), exécuté par `make queries-check`), pas ajouté de façon préventive — un index
inutilisé coûte en écriture (chaque insert doit le maintenir) sans jamais
apporter de bénéfice en lecture.

---

## 4. Gestion des relations (sans foreign keys)

MongoDB n'a pas de contrainte d'intégrité référentielle native. Trois
stratégies coexistent dans ce modèle :

| Stratégie | Exemple | Quand l'utiliser |
|---|---|---|
| **Référence par ID** (pas de duplication) | `fraud_alerts.customer_id` référence `customers.customer_id` (Postgres) | Quand la donnée référencée change souvent et qu'on veut toujours sa version actuelle |
| **Dénormalisation totale** (duplication) | `fraud_alerts` duplique `amount`, `ip_country`, etc. déjà présents dans `transaction_logs.payload` | Quand la lecture prime sur la fraîcheur — la transaction ne change plus une fois scorée |
| **Document imbriqué** (embedding) | `fraud_alerts.rules_triggered` (tableau embarqué, pas une collection séparée) | Quand la sous-donnée n'a de sens qu'attachée à son parent et n'est jamais interrogée seule |

Le choix embedding vs. référence suit la règle usuelle MongoDB : **données
lues ensemble → embedded ; données au cycle de vie ou volume différent →
référence**. `rules_triggered` est toujours lu avec son alerte parente
(embedding) ; `customer_id` référence une entité qui vit dans un autre
système avec son propre cycle de vie (référence).

---

## 5. Cohérence cross-système (Postgres ↔ MongoDB)

Il n'y a pas de transaction distribuée entre Postgres et MongoDB — la
cohérence est **éventuelle** (eventual consistency), garantie par le
pipeline CDC :

```
Postgres INSERT transaction
    → Debezium capte le WAL
    → Kafka stripe.public.transactions
    → scorer (flink_like_job.py) calcule fraud_score
    → Kafka stripe.payments.events
    → mongo_writer.py écrit transaction_logs + fraud_alerts
```

En cas d'échec d'écriture Mongo, le message repart en DLQ
(`stripe.etl.dead-letter`, cf. `mongo_writer.py`) plutôt que d'être perdu
silencieusement — la donnée Postgres reste la source de vérité, MongoDB
est une **vue dérivée éventuellement cohérente**, jamais l'inverse.

---

## 6. Limites assumées

- Pas de sharding configuré (pertinent seulement à un volume dépassant la
  capacité d'un replica set)
- Pas de schema validation MongoDB (`$jsonSchema`) activée sur les
  collections — les documents actuels respectent une forme implicite
  côté code, non enforcée côté base
- `user_interactions` et `customer_feedback` : schéma prêt (index, TTL)
  mais pipeline d'alimentation pas encore implémenté (aucun producer ne
  les écrit actuellement)
