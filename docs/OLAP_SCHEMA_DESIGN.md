# OLAP Schema Design — Snowflake

Certification AIA RNCP41993 — Bloc 2

Schéma détaillé du système OLAP (Snowflake), stratégies d'agrégation et
techniques d'optimisation des requêtes. Implémenté par
[`etl/snowflake_setup.py`](../etl/snowflake_setup.py) (DDL) et
[`etl/load_snowflake.py`](../etl/load_snowflake.py) (chargement). Complète
`PRESENTATION.md §3.7` avec le détail schéma/optimisation.

---

## 1. Pourquoi un star schema (et pas un snowflake schema)

Un **star schema** a été choisi plutôt qu'un snowflake schema (dimensions
normalisées en sous-dimensions) : les dimensions ici sont petites
(`dim_merchant` ~200 lignes, `dim_geography` 17 lignes) et à faible
cardinalité d'attributs — normaliser davantage ajouterait des jointures
sans gain de stockage significatif. Le star schema privilégie la vitesse de
requête (moins de jointures) sur l'espace disque, ce qui est le bon
compromis pour du reporting BI où la latence de requête prime.

---

## 2. Schéma — table de faits et dimensions

```
                    dim_date (date_key)
                         │
                         │
dim_geography ───► fact_transactions ◄─── dim_merchant
 (geo_key)          │  txn_key (PK)         (merchant_key)
                     │  txn_id (UNIQUE)
                     │  amount_eur
                     │  fee_amount
                     │  fraud_score, is_fraud
                     │  processing_ms
                     │
        dim_customer ◄┘└► dim_payment_method
       (customer_key)      (pm_key)
```

| Table | Type | Grain | Clé primaire |
|---|---|---|---|
| `fact_transactions` | Fait | 1 ligne = 1 transaction réussie | `txn_key` (surrogate, `AUTOINCREMENT`) |
| `dim_date` | Dimension | 1 ligne = 1 jour, 2020-2029 | `date_key` (format `YYYYMMDD`, ex. `20260315`) |
| `dim_merchant` | Dimension | 1 ligne = 1 marchand | `merchant_key` (surrogate) |
| `dim_customer` | Dimension | 1 ligne = 1 client | `customer_key` (surrogate) |
| `dim_payment_method` | Dimension | 1 ligne = 1 moyen de paiement | `pm_key` (surrogate) |
| `dim_geography` | Dimension | 1 ligne = 1 pays | `geo_key` (surrogate) |

### Pourquoi des clés de substitution (`AUTOINCREMENT`) plutôt que les UUID métier directement ?

Les UUID Postgres (`merchant_id`, `customer_id`...) sont conservés comme
attributs (`merchant_id VARCHAR(36) UNIQUE`) mais la fact table référence
des `NUMBER` (surrogate keys). Deux raisons : (1) un `NUMBER` est plus
compact et plus rapide à joindre/clusterer qu'un `VARCHAR(36)`, (2) les
surrogate keys permettent une évolution future vers un historique SCD type
2 sur les dimensions (`valid_from`/`is_current` déjà présents sur
`dim_merchant`/`dim_customer`, cf. §5) sans casser les clés étrangères de
la fact table.

---

## 3. Stratégie de clustering

```sql
CLUSTER BY (date_key, merchant_key)
```

Justification : le pattern d'usage BI dominant est "revenu/fraude par jour
et par marchand" (cf. requêtes `stripe_queries.sql §2.1 — Performance par
région et trimestre`, et le dashboard `top_merchants()`). Clusterer sur
`(date_key, merchant_key)` fait que Snowflake élague (prune) les micro-partitions
qui ne contiennent pas la plage de dates ou le marchand demandé, réduisant
le volume de données scanné sans index explicite (Snowflake n'a pas
d'index B-tree classique — le clustering en tient lieu).

---

## 4. Stratégies d'agrégation

### 4.1 — Agrégation à la charge (au lieu de tables pré-agrégées)

Le design actuel n'a **pas** de tables d'agrégats matérialisés côté
Snowflake (contrairement à Postgres qui a `mv_daily_revenue` et
`mv_merchant_stats`, cf. `init/postgres/01_ddl.sql`). Choix assumé : à
l'échelle démo (des milliers de lignes), le coût d'un `GROUP BY` à la volée
sur `fact_transactions` est négligeable grâce au clustering (§3). À plus
grande échelle (des milliards de lignes), la même logique que les vues
matérialisées Postgres serait reproduite avec des **Dynamic Tables**
Snowflake (agrégats rafraîchis automatiquement à intervalle défini).

### 4.2 — Semi-additivité de `fraud_score`

`fraud_score` (moyenne pondérée) n'est **pas additif** across dimensions —
contrairement à `amount_eur`/`fee_amount` qui se somment sans ambiguïté. Une
requête qui agrège `AVG(fraud_score)` doit toujours préciser sur quel grain
(par jour ? par marchand ? les deux ?) car la moyenne des moyennes n'est pas
la moyenne globale. `stripe_queries.sql` documente ce piège dans ses
commentaires de section OLAP.

### 4.3 — Idempotence du chargement (`MERGE INTO`)

Décrit dans `PRESENTATION.md §3.7` : dimensions en upsert, fact en
insert-only via `txn_id UNIQUE`. Ce choix permet de **rejouer** un batch
ETL en échec sans dupliquer de lignes — propriété indispensable pour un
pipeline daté (`WHERE DATE(t.created_at) = %s` dans `load_snowflake.py`)
qui peut être relancé après un incident.

---

## 5. Évolution des dimensions (Slowly Changing Dimensions)

`dim_merchant` et `dim_customer` ont des colonnes `valid_from` et
`is_current` préparées pour une historisation SCD type 2 (garder
l'historique des changements, ex. un marchand qui change de `tier`). **Non
exploitées actuellement** — `load_snowflake.py` fait de l'insert-only sur
ces dimensions (`WHERE NOT EXISTS`), pas de gestion de version. C'est un
choix délibéré pour garder le MVP simple : le schéma est prêt pour cette
évolution sans migration (`ALTER TABLE`) le jour où elle devient nécessaire.

---

## 6. Techniques d'optimisation des requêtes

| Technique | Où | Effet |
|---|---|---|
| Clustering `(date_key, merchant_key)` | `fact_transactions` | Élagage de partitions sur les requêtes temporelles/par marchand (§3) |
| Warehouse `X-SMALL` avec `AUTO_SUSPEND=60` | `etl/snowflake_setup.py` | Coûte zéro quand le warehouse est inactif plus de 60s — pertinent pour un usage BI intermittent (pas de requêtes 24/7) |
| `AUTO_RESUME=TRUE` | idem | Le warehouse redémarre automatiquement à la prochaine requête, sans intervention manuelle |
| Prédicat de date sur `dim_date.date_key` (`NUMBER`) plutôt que `TIMESTAMP` | Jointure fact↔dim_date | Comparaison d'entiers, moins coûteuse qu'une comparaison de timestamps avec fuseau horaire |
| Dénormalisation du type/marque de paiement à l'extraction (`pm.type AS pm_type`) | `load_snowflake.py` | Évite une jointure supplémentaire au moment du reporting — le prix est payé une fois à l'ETL plutôt qu'à chaque requête BI |
| `MERGE` avec sous-requête scalaire pour résoudre les surrogate keys | `load_snowflake.py` | Résout `merchant_key`/`customer_key`/`pm_key`/`geo_key` au chargement, pas à la lecture — les requêtes BI n'ont pas à faire ce lookup |

---

## 7. Limites assumées

- Pas de partitionnement physique explicite au-delà du clustering logique
  (suffisant au volume actuel — cf. `PRESENTATION.md §8.1`)
- Pas de Dynamic Tables ni de tâches (`TASK`) planifiées pour rafraîchir des
  agrégats — l'ETL est un script batch quotidien (`make snowflake-export`),
  pas un pipeline Snowflake natif
- `dim_date` pré-peuplée statiquement (2020-2029) plutôt que générée à la
  demande — trade-off simplicité vs. flexibilité, suffisant pour l'horizon
  du projet
