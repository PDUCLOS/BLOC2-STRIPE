# Security and Compliance Plan — Stripe Polyglot

Certification AIA RNCP41993 — Bloc 2

Ce document couvre les mesures de sécurité, les stratégies de conformité et
les outils de monitoring pour l'architecture de données polyglotte
(PostgreSQL · MongoDB · Kafka · Redis · Snowflake). Il complète la section
"Conformité RGPD" de [`PRESENTATION.md`](PRESENTATION.md#6-conformité-rgpd-ce-quon-a-mis-en-place)
avec un périmètre plus large (sécurité applicative, réseau, monitoring)
et distingue explicitement l'état actuel de la démo de la cible production.

---

## 1. Cadre réglementaire

| Référentiel | Portée sur ce projet |
|---|---|
| **PCI-DSS v4.0** | Traitement de données de paiement (montants, empreintes de carte) — le PAN (numéro de carte complet) n'est **jamais** stocké, seul `payment_methods.fingerprint` (SHA-256) et `last4` le sont |
| **RGPD (UE) 2016/679** | Données personnelles clients (email, nom, pays, comportement d'achat) |

Ce projet n'est **pas** un environnement certifié PCI-DSS (c'est une démo
locale, pas un service qui encaisse réellement), mais le schéma de données
est conçu pour ne jamais introduire les données interdites par le standard
(cf. §3).

---

## 2. Classification des données

| Donnée | Classification | Où | Traitement |
|---|---|---|---|
| Email, nom client | PII | `customers`, `merchants` (Postgres) | Chiffrement en transit, anonymisation sur demande (§5) |
| Empreinte carte (`fingerprint`) | Sensible (pseudonymisée) | `payment_methods` (Postgres) | SHA-256, jamais le PAN brut |
| `last4` | Sensible (partielle, non reconstituable en PAN) | `payment_methods` | Stocké en clair — 4 derniers chiffres uniquement, autorisé par PCI-DSS |
| Montants, statuts transaction | Financier | `transactions` (Postgres), `fact_transactions` (Snowflake) | Pas de PII directe |
| Logs applicatifs | Opérationnel | `logs`, `transaction_logs` (MongoDB) | TTL 90 jours |
| `fraud_score`, `rules_triggered` | Dérivée / décisionnelle | `fraud_indicators`, `fraud_alerts` | Pas de PII directe, mais liée à `customer_id` |

**Ce qui n'est jamais collecté** : numéro de carte complet (PAN), CVV, date
d'expiration en clair non hashée — le générateur (`producers/transaction_producer.py`)
et le schéma (`init/postgres/01_ddl.sql`) ne prévoient aucun champ pour ces données.

---

## 3. Contrôle d'accès (IAM)

### 3.1 — État actuel (implémenté)

| Rôle Postgres | Créé par | Privilèges | Usage |
|---|---|---|---|
| `stripe_app` (superutilisateur du conteneur, cf. `.env`) | `docker-compose.yml` | Lecture/écriture complète | Producer, dashboard, ETL — usage applicatif direct |
| `replication_user` | [`scripts/postgres_init_roles.sh`](../scripts/postgres_init_roles.sh) + `init/postgres/01_ddl.sql` (bootstrap) | `SELECT` seul, sur toutes les tables + futures tables (`ALTER DEFAULT PRIVILEGES`) | Debezium (CDC) — accès en lecture seule, jamais d'écriture |

Le principe du moindre privilège est appliqué au moins pour `replication_user` :
il ne peut techniquement pas modifier de données, seulement les lire via le
protocole de réplication logique.

### 3.2 — Écart identifié vs. cible

`PRESENTATION.md` mentionne un rôle `analytics_reader` sans accès à
`fingerprint` — **ce rôle n'existe pas dans le DDL actuel**
([`init/postgres/01_ddl.sql`](../init/postgres/01_ddl.sql)). En l'état, tout
accès applicatif passe par `stripe_app`, qui a accès à toutes les colonnes.

**Action recommandée** :
```sql
CREATE ROLE analytics_reader WITH LOGIN PASSWORD '...';
GRANT SELECT ON merchants, customers, transactions, refunds, fraud_indicators TO analytics_reader;
REVOKE SELECT (fingerprint) ON payment_methods FROM analytics_reader;
GRANT SELECT (pm_id, customer_id, type, brand, last4, is_default) ON payment_methods TO analytics_reader;
```
À utiliser pour le dashboard Streamlit et Snowflake une fois ce rôle créé,
au lieu de `stripe_app`.

### 3.3 — MongoDB / Redis

Les identifiants `MONGO_USER`/`MONGO_PASSWORD` et `REDIS_PASSWORD` sont
injectés via `.env` (jamais commités — cf. `.gitignore`). MongoDB tourne
avec authentification (`authSource=admin`), Redis avec mot de passe
(`REDIS_CONFIG["password"]` dans `dashboard/app.py`). Pas de RBAC granulaire
par collection en l'état — un seul utilisateur applicatif pour l'ensemble
des services (acceptable pour une démo locale, à segmenter en prod).

---

## 4. Chiffrement

| Flux | État démo (actuel) | Cible production |
|---|---|---|
| Connexions Postgres | `scram-sha-256` (auth), pas de TLS | `PGSSLMODE=verify-full` + certificat |
| Kafka | Pas de TLS (réseau Docker local) | TLS + SASL (ex. AWS MSK avec TLS) |
| Redis | Mot de passe, pas de TLS | Redis avec TLS (ElastiCache in-transit encryption) |
| MongoDB | Auth SCRAM, pas de TLS | `mongodb+srv://` avec TLS |
| Données au repos | Volumes Docker non chiffrés (démo locale) | EBS/volumes chiffrés (AWS KMS), Postgres `pgcrypto` déjà installé pour du chiffrement applicatif ciblé si besoin |
| `payment_methods.fingerprint` | SHA-256 (pseudonymisation, pas du chiffrement réversible) | Inchangé — c'est le comportement voulu (non réversible) |

Le choix de ne pas activer TLS en local est assumé : c'est un compromis de
simplicité pour la démo (cf. `PRESENTATION.md §8.1 — Limites de la démo`),
pas un oubli. La variable `PGSSLMODE` est déjà prévue comme point
d'activation en production.

---

## 5. Droit à l'effacement et gestion du cycle de vie des données (RGPD)

Repris et étendu de `PRESENTATION.md §6` :

| Article RGPD | Mécanisme | Implémentation |
|---|---|---|
| Art. 17 — Droit à l'effacement | `anonymize_customer(p_customer_id UUID)` (PL/pgSQL) | Remplace email/nom/fingerprint, détache le `customer_id` des transactions existantes sans les supprimer (traçabilité comptable préservée) |
| Art. 5(1)(e) — Limitation de conservation | TTL MongoDB | `transaction_logs` : 90 jours · `user_interactions` : 30 jours (cf. `init/mongo/01_init_collections.js`) |
| Art. 4(5) — Pseudonymisation | Hash SHA-256 | `payment_methods.fingerprint`, jamais le PAN |
| Art. 25 — Privacy by design | Séparation des rôles DB | Voir §3 |
| Art. 32 — Sécurité du traitement | Chiffrement en transit (activable) | Voir §4 |
| Art. 15 — Droit d'accès (**non implémenté**) | — | Pas d'endpoint API pour qu'un client récupère l'ensemble de ses données — identifié comme axe d'amélioration dans `PRESENTATION.md §8.2` |

---

## 6. Sécurité réseau

### 6.1 — État actuel

`docker-compose.yml` expose directement tous les ports de service sur
l'hôte (`5432`, `27017`, `9092`, `6379`, `8083`, `8501`, `8081`), sans réseau
Docker dédié ni règles de restriction — cohérent avec un usage démo/dev en
local, où l'objectif est l'accessibilité pour le debug (`scripts/`, tests
manuels documentés en `PRESENTATION.md §7.2`).

### 6.2 — Cible production

- VPC dédié avec sous-réseaux privés pour Postgres/MongoDB/Redis/Kafka
  (aucun port DB exposé publiquement)
- Security groups par service (principe du moindre accès réseau : seul le
  scorer et le CDC parlent à Postgres, seul le dashboard lit en lecture seule)
- Bastion / VPN pour l'administration
- WAF devant le dashboard Streamlit si exposé publiquement

---

## 7. Monitoring et outillage

### 7.1 — Implémenté

| Outil | Usage |
|---|---|
| `docker-compose` healthchecks | Postgres, MongoDB, Kafka, Redis, Debezium — `make up` attend l'état `healthy` avant de poursuivre l'init (`demo.sh`) |
| `make smoke` | Smoke tests manuels post-déploiement (`PRESENTATION.md §7.2`) |
| `tests/test_e2e.py` | Suite de tests automatisés couvrant connectivité, schéma, CDC, TTL RGPD, pipeline de scoring |
| Dashboard Streamlit | Statut live des 3 datastores (`OK`/`DOWN`) visible en sidebar (`dashboard/app.py`) |
| Dead-letter queue (`stripe.etl.dead-letter`) | Capture les erreurs de parsing/scoring/écriture sans bloquer le pipeline ni les perdre silencieusement |

### 7.2 — Non implémenté (axe d'amélioration, cf. `PRESENTATION.md §8.2`)

- **Alerting actif** : PagerDuty/Slack sur `decision = block` avec montant élevé, ou sur DLQ non vide
- **Observabilité centralisée** : pas de Datadog/CloudWatch — les logs restent dans `docker logs` et la collection Mongo `logs`
- **Audit trail** : pas de traçabilité "qui a exécuté quelle requête" sur les rôles Postgres (pas de `pgaudit`)
- **Alerte sur volume DLQ** : un stock DLQ croissant devrait déclencher une alerte, actuellement seulement visible via `make smoke` / inspection manuelle

---

## 8. Plan de réponse à incident (esquisse)

1. **Détection** : DLQ non vide, `pg_isready` en échec, ou alerte fraude
   volumétrique anormale (`fraud_alerts` croissance rapide)
2. **Confinement** : `make down` isole l'infra ; `replication_user` en
   lecture seule limite le risque d'un compte Debezium compromis
3. **Éradication/remédiation** : rotation des secrets `.env`
   (`scripts/init_env.sh` régénère des secrets aléatoires), `ALTER ROLE ...
   WITH PASSWORD` pour `replication_user`
4. **Post-mortem** : les logs Mongo (`logs` collection, TTL 90j) et les
   `fraud_indicators.rules_triggered` fournissent la base d'investigation

Ce plan reste un canevas pour une démo de certification — une réponse à
incident réelle nécessiterait des runbooks détaillés par type d'incident,
hors périmètre de ce document.

---

## 9. Synthèse des écarts (gap analysis)

| Mesure | État |
|---|---|
| Pas de stockage du PAN/CVV | ✅ Fait |
| Pseudonymisation (fingerprint) | ✅ Fait |
| Droit à l'effacement (Art. 17) | ✅ Fait |
| TTL / limitation de conservation | ✅ Fait |
| Rôle `replication_user` restreint (lecture seule) | ✅ Fait |
| Rôle `analytics_reader` sans accès `fingerprint` | ❌ Documenté mais pas implémenté (§3.2) |
| TLS sur les connexions inter-services | ❌ Désactivé en démo, activable en prod (§4) |
| Alerting actif (PagerDuty/Slack) | ❌ Non implémenté (§7.2) |
| Audit trail des accès DB | ❌ Non implémenté (§7.2) |
| Endpoint RGPD Art. 15 (droit d'accès) | ❌ Non implémenté (§5) |
| Isolation réseau (VPC, security groups) | ❌ Non applicable en local, à faire en prod (§6.2) |
