# Machine Learning Integration Strategy — Stripe Polyglot

Certification AIA RNCP41993 — Bloc 2

Ce document décrit l'intégration d'un modèle de machine learning au sein du
système NoSQL (MongoDB + Redis) pour le scoring de fraude : extraction des
features, entraînement, déploiement, et monitoring de performance.

---

## 1. État actuel — scoring rule-based, pas de modèle entraîné

Le pipeline calcule aujourd'hui un `fraud_score` avec un moteur à règles
statiques, implémenté à l'identique dans deux endroits :

- [`producers/flink_like_job.py`](../producers/flink_like_job.py) —
  `score_transaction()`, version démo locale
- [`flink/fraud_scoring_job.py`](../flink/fraud_scoring_job.py) —
  `FraudScoringFunction.map()`, version cluster PyFlink

5 règles additives (montant élevé, card testing, géo à risque, vélocité
1h/24h) avec des poids fixes écrits en dur, taguées `model_version:
"rule-based-v1"`. **Aucune phase d'entraînement, aucun fichier modèle
sérialisé.** Ce document décrit la stratégie pour faire évoluer ce système
vers un vrai modèle supervisé, en s'appuyant sur l'infrastructure NoSQL déjà
en place.

---

## 2. Pourquoi le NoSQL est le bon terrain pour l'intégration ML

| Composant NoSQL | Rôle dans le pipeline ML |
|---|---|
| **Redis** (sorted sets `v1h_<id>`/`v24h_<id>`, hash `feat_<id>`) | Feature store **online**, faible latence (< 5ms), déjà alimenté en temps réel par le scorer actuel |
| **MongoDB** (`transaction_logs`, `fraud_alerts`) | Source d'entraînement : historique complet des transactions + labels de décision humaine/règle (`decision`, `rules_triggered`) |
| **MongoDB `ml_features`** (collection déjà créée, cf. [`init/mongo/01_init_collections.js`](../init/mongo/01_init_collections.js), actuellement inutilisée) | Feature store **offline** consolidé, snapshot périodique des features par client pour l'entraînement batch |

Le schéma existant anticipe déjà cette évolution : `ml_features` a un index
unique sur `customer_id` et un champ `last_updated` — prêt à accueillir des
features consolidées sans migration de schéma.

---

## 3. Extraction et ingénierie des features

### 3.1 — Features déjà calculées (réutilisables telles quelles)

| Feature | Source actuelle | Calculée par |
|---|---|---|
| `velocity_1h`, `velocity_24h` | Redis ZSET (`v1h_<id>`, `v24h_<id>`) | `score_transaction()` |
| `last_amount`, `last_country` | Redis HASH (`feat_<id>`) | `score_transaction()` |
| Historique transactionnel complet | MongoDB `transaction_logs.payload` | `mongo_writer.py` |
| Label de décision (proxy de vérité terrain) | `fraud_indicators.decision` (Postgres) / `fraud_alerts.decision` (Mongo) | Pipeline de scoring actuel |

### 3.2 — Features à ajouter pour un modèle supervisé

| Feature | Calcul proposé | Où la stocker |
|---|---|---|
| Montant normalisé (z-score par marchand) | Moyenne/écart-type glissants par `merchant_id` | `ml_features` (batch) |
| Ratio montant / historique client | `amount / avg(amount client sur 30j)` | `ml_features` |
| Nouveauté du device/pays pour ce client | Device/pays jamais vu avant pour ce `customer_id` | Redis SET `devices_<id>` / `countries_<id>` (à créer) |
| Heure de la journée / jour de semaine | Dérivé de `created_at` | Calculé à la volée, pas besoin de stockage |
| Taux de fraude historique du marchand | Agrégation `fraud_indicators` par `merchant_id` sur fenêtre glissante | `ml_features` (batch) |

### 3.3 — Pipeline d'extraction batch (offline, pour l'entraînement)

```
MongoDB transaction_logs + fraud_alerts  ──┐
                                            ├─► Job batch (pandas/PySpark) ──► ml_features (Mongo)
Postgres fraud_indicators (labels)       ──┘                                  └─► export Parquet (training set)
```

Ce job réutilise le pattern déjà établi par
[`etl/load_snowflake.py`](../etl/load_snowflake.py) (extraction bornée par
date, traitement par lots) — même approche, cible différente (Mongo au lieu
de Snowflake).

---

## 4. Entraînement du modèle

| Aspect | Choix proposé | Justification |
|---|---|---|
| Algorithme | XGBoost ou LightGBM (gradient boosting) | Cité comme cible dans `PRESENTATION.md §8.1` ; performant sur données tabulaires déséquilibrées (peu de fraude vs beaucoup de légitime), interprétable via feature importance — important pour justifier une décision de blocage |
| Label | `fraud_indicators.decision` (`block`/`review`/`allow`) → binarisé `is_fraud` | Réutilise le pipeline de décision existant comme vérité terrain de départ (imparfaite mais disponible immédiatement) |
| Déséquilibre des classes | `scale_pos_weight` (XGBoost) ou sur-échantillonnage SMOTE | Le générateur de démo produit ~5% de fraude (`FRAUD_RATIO` dans `transaction_producer.py`) — déséquilibre représentatif d'un cas réel |
| Validation | Split temporel (train sur période N, test sur période N+1) | Évite la fuite d'information (leakage) via la vélocité, qui dépend de l'ordre chronologique |
| Fréquence de ré-entraînement | Hebdomadaire, ou déclenché sur dérive de performance (§6) | Les patterns de fraude évoluent — un modèle figé se dégrade |

Sortie de l'entraînement : un fichier modèle sérialisé (`.pkl`/`.onnx`)
versionné, nommé selon la même convention que `model_version` déjà présente
dans le schéma (ex. `xgboost-v1`, `xgboost-v2`).

---

## 5. Déploiement du modèle

### 5.1 — Où le modèle s'insère dans le pipeline existant

Le point d'insertion est **exactement** la fonction `score_transaction()` /
`FraudScoringFunction.map()` — le modèle remplace le calcul de règles, mais
garde la même interface (lit les features Redis, écrit `fraud_score` +
`decision` + `model_version`) :

```
Avant (rule-based) :
  Kafka event → score_transaction() [règles fixes] → fraud_score, decision

Après (ML) :
  Kafka event → extract_features() [Redis + event]
              → model.predict_proba() [modèle chargé une fois, en mémoire]
              → fraud_score, decision, model_version="xgboost-v1"
```

Le champ `model_version`, déjà présent dans le payload (`producers/flink_like_job.py`
ligne `txn["model_version"] = "rule-based-v1"`) et dans le schéma
`fraud_indicators.model_version` (Postgres), sert **sans modification de
schéma** de mécanisme de traçabilité — chaque transaction sait quel modèle
(ou quelle règle) a produit sa décision.

### 5.2 — Chargement du modèle dans le job

- Version démo (`flink_like_job.py`) : chargement du `.pkl` au démarrage du
  process Python (une fois), comme pour la connexion Redis actuelle
- Version PyFlink (`fraud_scoring_job.py`) : chargement dans `open()` de
  `FraudScoringFunction` — même emplacement que l'initialisation Redis
  actuelle (`self.redis_client`), pour la même raison : `open()` s'exécute
  une fois par tâche sur le TaskManager, pas par événement

### 5.3 — Stratégie de rollout

1. **Shadow mode** : le modèle calcule un score en parallèle des règles,
   écrit dans un champ séparé (`fraud_score_ml`), sans influencer la
   décision réelle — permet de comparer avant bascule
2. **A/B test** : bascule progressive par pourcentage de trafic, le champ
   `model_version` permet de retrouver quelle version a scoré quelle
   transaction — déjà listé comme axe d'amélioration dans
   `PRESENTATION.md §8.2` point 5, avec ce même mécanisme
3. **Bascule complète** une fois la performance validée (§6)
4. **Rollback** : conserver le moteur à règles comme filet de sécurité
   (fallback) si le modèle ne peut pas être chargé (`except Exception:
   return None` déjà présent dans `score_transaction()` pour la robustesse
   Redis — même pattern applicable au chargement modèle)

---

## 6. Monitoring de la performance

| Métrique | Calcul | Fréquence |
|---|---|---|
| Precision / Recall / F1 sur les décisions `block` | Comparaison `fraud_score` prédit vs. labels réels (quand disponibles a posteriori — ex. contestation client, chargeback) | Batch quotidien |
| Distribution du score (drift) | Histogramme `fraud_score` sur fenêtre glissante, comparé à la distribution d'entraînement (test de Kolmogorov-Smirnov) | Batch quotidien |
| Taux de décisions `block`/`review` dans le temps | Agrégation sur `fraud_alerts` (déjà visible dans le dashboard `dashboard/app.py`) | Temps réel (déjà en place) |
| Feature drift | Comparaison des distributions de features (`velocity_1h`, montant, etc.) entre training set et trafic live | Batch hebdomadaire |
| Latence d'inférence | Temps `model.predict()` par transaction | Temps réel, alerting si > seuil (budget latence < 50ms cité en `PRESENTATION.md`) |

Le dashboard Streamlit existant (`dashboard/app.py`) est le point
d'affichage naturel pour ces métriques : une nouvelle section
"Performance modèle ML" viendrait s'ajouter aux sections KPI/Charts déjà
présentes, en réutilisant le pattern `pg_query()`/`get_mongo()` existant.

---

## 7. Boucle de rétroaction (feedback loop)

```
Décision modèle (block/review/allow)
        │
        ▼
Vérification humaine (équipe Risk) ou signal client (chargeback, dispute)
        │
        ▼
Label corrigé écrit dans fraud_indicators.decision (Postgres)
ou customer_feedback (Mongo, collection déjà créée pour les contestations)
        │
        ▼
Réintégré dans le prochain cycle d'entraînement (§4)
```

Cette boucle transforme les faux positifs/négatifs identifiés
opérationnellement en données d'entraînement pour la version suivante du
modèle — condition nécessaire pour qu'un modèle ML batte durablement le
moteur à règles actuel plutôt que de simplement le reproduire.

---

## 8. Ce qui reste hors périmètre de ce document

- Choix d'infrastructure de serving (SageMaker, MLflow Model Registry, ou
  chargement direct `.pkl` comme décrit en §5.2 pour rester cohérent avec
  la simplicité du reste du pipeline démo)
- Détails d'implémentation du job d'entraînement (notebook vs. script vs.
  orchestrateur Airflow — cohérent avec l'absence actuelle d'Airflow notée
  dans `PRESENTATION.md §8.1`)
- Explicabilité réglementaire poussée (SHAP values pour justifier un refus
  client) — pertinent si ce projet évoluait vers une exigence de conformité
  type "droit à l'explication" (RGPD Art. 22)
