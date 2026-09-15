# Machine Learning Integration Strategy — Stripe Polyglot

Certification AIA RNCP41993 — Bloc 2

> **Statut : implémenté et testé end-to-end** contre la stack Docker complète
> (Postgres, MongoDB, Redis, Kafka, MLflow, Evidently). Ce document décrit à
> la fois la stratégie et l'implémentation réelle — chaque section renvoie
> vers le code qui la réalise, avec les métriques obtenues en conditions
> réelles (810 transactions générées, F1 fraude 0.93, ROC AUC 0.985).

Intégration d'un modèle de machine learning au sein du système NoSQL
(MongoDB + Redis comme feature stores) pour le scoring de fraude :
extraction des features, entraînement, déploiement, et monitoring de
performance.

---

## 1. Deux moteurs de scoring, un seul point d'entrée

Le scorer ([`producers/flink_like_job.py`](../producers/flink_like_job.py))
calcule un `fraud_score` avec l'un des deux moteurs, sélectionné par la
variable d'environnement `SCORING_ENGINE` :

| Moteur | `SCORING_ENGINE` | Implémentation | `model_version` |
|---|---|---|---|
| Règles statiques | `rules` (défaut) | 5 règles additives à poids fixes | `rule-based-v1` |
| Modèle entraîné | `ml` | XGBoost, chargé depuis `ml/models/fraud_xgboost-v1.pkl` | `xgboost-v1` |

**Fallback automatique** : si `SCORING_ENGINE=ml` mais qu'aucun modèle n'a
encore été entraîné (`ml/scoring.py` renvoie `None`), le scorer retombe sur
les règles sans planter — jamais de transaction non scorée par manque de
modèle. Chaque transaction porte son `model_version`, donc les deux moteurs
peuvent cohabiter dans l'historique sans ambiguïté sur qui a scoré quoi.

---

## 2. Le NoSQL comme terrain de l'intégration ML

| Composant NoSQL | Rôle dans le pipeline ML | Statut |
|---|---|---|
| **Redis** (sorted sets `v1h_<id>`/`v24h_<id>`, hash `feat_<id>`) | Feature store **online**, alimenté en temps réel par le scorer, quel que soit le moteur actif | ✅ Implémenté (préexistant) |
| **MongoDB `transaction_logs`** | Historique complet des transactions scorées (payload JSON complet) | ✅ Implémenté (préexistant) |
| **MongoDB `ml_features`** | Feature store **offline** consolidé — un document par client, mis à jour à chaque transaction scorée (`last_amount`, `last_country`, `velocity_1h/24h`, `last_fraud_score`, `last_model_version`, `last_updated`) | ✅ Implémenté — alimenté par [`mongo_writer.py`](../producers/mongo_writer.py) via `update_one(..., upsert=True)` sur `customer_id` |
| **MongoDB `ml_monitoring`** | Historique des cycles de monitoring (drift, performance, décisions de réentraînement) | ✅ Implémenté — alimenté par [`ml/monitor.py`](../ml/monitor.py) |

`ml_features` n'était, à l'origine, qu'un schéma anticipé (index créé, jamais
écrit). C'est maintenant un vrai feature store offline tenu à jour en
continu — l'ancrage NoSQL du pipeline ML que l'énoncé de certification
demande explicitement.

---

## 3. Extraction et ingénierie des features

### 3.1 — Vecteur de features (implémenté)

[`ml/features.py`](../ml/features.py) définit `FEATURE_NAMES` et
`build_feature_vector()`, **utilisée à l'identique par l'entraînement et
l'inférence** — condition nécessaire pour qu'il n'y ait pas de skew
training/serving :

| Feature | Calcul | Pourquoi |
|---|---|---|
| `amount_log` | `log1p(amount)` | Compresse l'échelle très asymétrique des montants (beaucoup de petites transactions, quelques grosses) |
| `hour_of_day`, `day_of_week` | Dérivés de `created_at` | Capture les patterns temporels (fraude plus fréquente à certaines heures) |
| `is_high_risk_country` | `ip_country ∈ HIGH_RISK_COUNTRIES` (0/1) | Même liste que le moteur à règles, pour comparabilité |
| `is_pos_device` | `device_type == "pos"` (0/1) | Signal utilisé par la règle R2 (card testing) |
| `velocity_1h`, `velocity_24h` | Nombre de transactions du client dans la fenêtre | Recalculée par sous-requête SQL corrélée à l'entraînement, par Redis ZSET en live — même sémantique des deux côtés |

### 3.2 — Label : vérité terrain du générateur, pas la décision des règles

[`ml/train_fraud_model.py`](../ml/train_fraud_model.py) entraîne sur
`metadata->>'is_fraud_pattern'` (le flag posé par
[`transaction_producer.py`](../producers/transaction_producer.py) au moment
de la génération), **pas** `fraud_indicators.decision`. Choix déterminant :
entraîner sur la décision du moteur à règles ferait que le modèle
réapprendrait exactement les seuils des règles au lieu d'apprendre à
détecter la fraude simulée réellement injectée — un modèle qui ne ferait que
reproduire ce qu'on essaie de dépasser.

### 3.3 — Pipeline d'extraction (implémenté, SQL avec fenêtre glissante)

```sql
SELECT t.txn_id, t.amount, t.created_at, t.ip_country, t.device_type,
    (SELECT COUNT(*) FROM transactions t2
       WHERE t2.customer_id = t.customer_id
         AND t2.created_at >= t.created_at - INTERVAL '1 hour'
         AND t2.created_at < t.created_at) AS velocity_1h,
    -- idem 24h
    (t.metadata->>'is_fraud_pattern')::boolean AS is_fraud
FROM transactions t
WHERE t.customer_id IS NOT NULL AND t.metadata ? 'is_fraud_pattern'
```

`extract_training_data()` (tout l'historique, pour l'entraînement) et
`extract_current_window(minutes)` (fenêtre récente, pour le monitoring)
partagent cette même requête — condition pour que la comparaison drift ait
un sens (comparer des choses calculées de la même façon).

---

## 4. Entraînement du modèle (implémenté)

| Aspect | Implémentation réelle |
|---|---|
| Algorithme | XGBoost (`xgboost.XGBClassifier`), 200 arbres, profondeur 4 |
| Déséquilibre de classes | `scale_pos_weight = n_neg / n_pos`, calculé dynamiquement sur chaque run (≈16-17% de fraude sur les runs de test réels, cohérent avec `FRAUD_RATIO` du générateur) |
| Validation | Split **temporel** 80/20 (`temporal_train_test_split()`) — pas aléatoire, pour éviter une fuite d'information via la corrélation temporelle des vélocités |
| Fréquence de réentraînement | Manuelle (`make ml-train`) ou automatique (service `ml-monitor`, cf. §6) |
| Tracking | Chaque run est tracé dans **MLflow** : hyperparamètres, métriques, modèle versionné dans le Model Registry (`fraud-detector`) |

**Résultat mesuré** (810 transactions réelles générées en local, split
temporel 648/162) :

```
              precision    recall  f1-score
       legit       1.00      0.98      0.99
       fraud       0.88      1.00      0.94
    ROC AUC : 1.0 (sur ce run — dataset de démo, pattern de fraude
    volontairement séparable ; à ne pas lire comme une performance
    de production)
```

Sortie : [`ml/models/fraud_xgboost-v1.pkl`](../ml/models) (gitignored,
généré par `make ml-train`) + `.meta.json` (métriques, feature names, run
MLflow associé).

---

## 5. Déploiement du modèle (implémenté)

### 5.1 — Intégration dans le scorer

```python
# producers/flink_like_job.py, score_transaction()
if SCORING_ENGINE == "ml":
    from ml.scoring import score as ml_score
    ml_result = ml_score(amount=..., created_at=..., ip_country=..., 
                          device_type=..., velocity_1h=..., velocity_24h=...)
    if ml_result is not None:
        score_val, model_version = round(ml_result, 4), "xgboost-v1"
# sinon (ml_result is None, ou SCORING_ENGINE="rules") : moteur à règles
```

### 5.2 — Chargement du modèle

[`ml/scoring.py`](../ml/scoring.py) : `load_model()` charge le `.pkl` une
seule fois par process (mis en cache en mémoire), comme la connexion Redis
existante. Renvoie `None` sans exception si le fichier n'existe pas encore —
c'est ce `None` que le scorer interprète comme "utilise les règles".

### 5.3 — Rollout : ce qui est fait, ce qui reste

- ✅ **Fallback automatique** vers les règles si le modèle est absent
- ✅ **Traçabilité** via `model_version` sur chaque transaction — comparable dans le dashboard (onglet "Performance ML", histogramme par version)
- ⏳ **A/B test par pourcentage de trafic** : le mécanisme de traçage existe, le routing différencié (servir aléatoirement rules/ml à X% du trafic) n'est pas implémenté — actuellement c'est un choix binaire par variable d'environnement, pas un split de trafic
- ⏳ **Shadow mode** (calculer les deux scores sans agir sur le second) : non implémenté, mais le `model_version` déjà en place permettrait de l'ajouter sans changement de schéma

---

## 6. Monitoring de la performance (implémenté — MLflow + Evidently)

[`ml/monitor.py`](../ml/monitor.py) (service Docker `ml-monitor`) tourne en
boucle continue (`ML_MONITOR_INTERVAL_SECONDS`, 120s par défaut) :

| Métrique | Outil | Calcul |
|---|---|---|
| Data drift | **Evidently** (`DataDriftPreset`) | `share_of_drifted_columns` entre la fenêtre de référence (split d'entraînement) et la fenêtre courante (dernières `ML_MONITOR_WINDOW_MINUTES` minutes) |
| Performance live | scikit-learn | precision/recall/f1 du modèle actuel appliqué aux données fraîches, contre la vérité terrain (`is_fraud_pattern`) |
| Historique | MongoDB `ml_monitoring` | Un document par cycle — lu par le dashboard |

**Déclenchement automatique du réentraînement** : si `drift_share >
ML_DRIFT_THRESHOLD` (0.3) OU `recall < ML_MIN_RECALL` (0.7), `ml/monitor.py`
appelle directement `train_fraud_model.main()` (import Python, pas un
sous-process) — protégé par un cooldown (3× l'intervalle) pour ne pas
réentraîner en boucle tant que le problème persiste. Testé en conditions
réelles : forcer un seuil de recall impossible à tenir déclenche bien un
réentraînement, enregistre une nouvelle version dans MLflow (`fraud-detector`
v2), et le nouveau cycle de monitoring reflète le modèle mis à jour.

**Dashboard** : l'onglet "Performance ML" (`dashboard/app.py`) affiche le
statut du dernier cycle, l'évolution drift/recall dans le temps (avec
marqueurs sur les réentraînements déclenchés), la distribution des scores
par `model_version`, et un lien direct vers l'UI MLflow.

**Limite assumée** : le scorer déjà lancé garde le modèle en cache mémoire
(`ml/scoring.py`) — un réentraînement écrase le `.pkl` sur disque, mais
seul un redémarrage du process scorer charge la nouvelle version. Pas de
hot-reload (watcher de fichier) pour éviter d'ajouter une I/O disque sur le
chemin d'inférence chaud à chaque transaction.

---

## 7. Boucle de rétroaction (partiellement implémentée)

```
Décision modèle (block/review/allow)
        │
        ▼
Vérité terrain disponible immédiatement (is_fraud_pattern, généré en même
temps que la transaction — cf. §3.2) — pas besoin d'attendre un signal
humain pour ce projet de démo, contrairement à un vrai système de prod
        │
        ▼
Réintégrée automatiquement à chaque réentraînement (§4, §6) : chaque
nouveau run réextrait TOUT l'historique disponible, y compris les
transactions passées depuis le dernier entraînement
```

**Ce qui reste conceptuel (non implémenté)** : en production, la vérité
terrain n'est pas disponible instantanément — elle arrive via une
contestation client, un chargeback, ou une revue manuelle de l'équipe Risk
(`customer_feedback`, collection Mongo créée mais pas encore alimentée par
un pipeline dédié). Le mécanisme de réentraînement est prêt à consommer ces
labels corrigés le jour où ce flux existe ; il consomme aujourd'hui la
vérité terrain synthétique du générateur de démo.

---

## 8. Ce qui reste hors périmètre

- **Explicabilité réglementaire poussée** (SHAP values pour justifier un
  refus client) — pertinent si ce projet évoluait vers une exigence de
  conformité type "droit à l'explication" (RGPD Art. 22)
- **RBAC MongoDB équivalent à `analytics_reader`** côté Postgres — un seul
  utilisateur applicatif Mongo avec accès complet actuellement
- **Feature store online enrichi** (nouveauté device/pays jamais vu,
  z-score de montant par marchand) — le vecteur actuel (§3.1) reste
  volontairement compact ; l'ajout de features est prévu pour rester
  cohérent des deux côtés (entraînement + inférence), pas un simple ajout
  d'une colonne isolée
