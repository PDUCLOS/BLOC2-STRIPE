# MLOps — Stripe Polyglot (Bloc 2)

Vue d'ensemble du cycle de vie du modèle de scoring fraude : de
l'entraînement au monitoring en production, en passant par l'intégration
continue. Ce document est le point d'entrée MLOps du projet — il renvoie
vers [`ML_INTEGRATION_STRATEGY.md`](ML_INTEGRATION_STRATEGY.md) pour le
détail de chaque étape plutôt que de le dupliquer.

## 1. Vue d'ensemble du cycle

```
                    ┌─────────────────────────────────────────────┐
                    │              CI (GitHub Actions)             │
                    │   lint  →  stack complète  →  make test  →   │
                    │           make notebook-check                │
                    └─────────────────────────────────────────────┘
                                       │ (gate avant merge sur main)
                                       ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  1. Données   │──▶│ 2. Entraîne- │──▶│ 3. Déploie-  │──▶│ 4. Monitoring │
│  (Postgres,   │   │   ment       │   │   ment       │   │  (drift +    │
│  vérité       │   │ (XGBoost +   │   │ (scorer,     │   │  perf servie)│
│  terrain      │   │  MLflow)     │   │  hot-reload) │   │              │
│  générateur)  │   │              │   │              │   └──────┬───────┘
└──────────────┘   └──────────────┘   └──────────────┘          │
        ▲                                                        │
        │              5. Réentraînement automatique              │
        └────────────── (drift > seuil OU recall < seuil) ◀──────┘
```

| Étape | Fichier(s) | Doc détaillée |
|---|---|---|
| 1. Données / vérité terrain | `seed/seed_data.py`, `producers/transaction_producer.py` | [ML_INTEGRATION_STRATEGY.md §3](ML_INTEGRATION_STRATEGY.md#3-extraction-et-ingénierie-des-features) |
| 2. Entraînement | `ml/train_fraud_model.py` | [§4](ML_INTEGRATION_STRATEGY.md#4-entraînement-du-modèle-implémenté) |
| 3. Déploiement / inférence | `ml/scoring.py`, `producers/flink_like_job.py` | [§5](ML_INTEGRATION_STRATEGY.md#5-déploiement-du-modèle-implémenté) |
| 4. Monitoring | `ml/monitor.py` (service `ml-monitor`) | [§6](ML_INTEGRATION_STRATEGY.md#6-monitoring-de-la-performance-implémenté--mlflow--evidently) |
| 5. Réentraînement auto | `ml/monitor.py` → `train_fraud_model.main()` | [§6](ML_INTEGRATION_STRATEGY.md#6-monitoring-de-la-performance-implémenté--mlflow--evidently) |
| Registre de modèles | MLflow (`fraud-detector`), UI sur `:5001` | [§4](ML_INTEGRATION_STRATEGY.md#4-entraînement-du-modèle-implémenté) |
| Audit / contrôle | `notebooks/audit_data_ml.ipynb` | §4 ci-dessous |
| CI/CD | `.github/workflows/ci.yml` | §2 ci-dessous |

---

## 2. CI/CD (GitHub Actions)

[`​.github/workflows/ci.yml`](../.github/workflows/ci.yml) — déclenché sur
push/PR vers `main`. Deux jobs :

### 2.1 — `lint` (quelques secondes, sans Docker)

- Compile tous les `.py` du repo (`python -m py_compile`)
- Valide le bon format XML des 4 diagrammes `presentation/*.drawio`
- Valide `docker-compose.yml` (`docker compose config -q`)

### 2.2 — `e2e` (~10-15 min, stack complète)

Monte **toute la stack réelle** sur le runner GitHub (pas de mocks — même
principe que `ml/monitor.py::compute_served_performance`, mesurer sur du
vrai plutôt que resimuler) :

1. `make init-env` + `make install` (`.venv`)
2. `make up` (Postgres, MongoDB, Redis, Kafka, Debezium, MLflow, ml-monitor)
3. Attente explicite des healthchecks Docker (pas un `sleep` à l'aveugle)
4. `scripts/create_topics.sh`, `postgres_init_roles.sh`, `deploy_debezium.sh`
5. `make seed`
6. Producer + scorer (`flink_like_job.py`, moteur à **règles** — pas besoin
   d'entraîner XGBoost en CI, le modèle est testé séparément par le
   notebook quand il existe) en tâche de fond, 20s de trafic
7. `make test` — les 16 assertions de `tests/test_e2e.py`
8. `make notebook-check` — exécute `notebooks/audit_data_ml.ipynb` de bout
   en bout ; échoue si un `assert` du notebook échoue (cf. §4)
9. Logs Docker dumpés automatiquement si un step échoue ; nettoyage
   (`docker compose down -v`) dans tous les cas (`if: always()`)

**Ce que ça garantit à chaque push** : le code compile, le schéma Postgres
s'initialise, le CDC Debezium fonctionne réellement (pas juste "le connecteur
existe"), le scorer traite du vrai trafic Kafka, et l'audit données/ML ne
détecte aucune régression connue (doublons CDC, features désalignées,
précision servie sous le seuil).

**Reproduire en local** : `make init && make test && make notebook-check`
— ce sont littéralement les mêmes commandes que la CI, pas une
réimplémentation parallèle.

**Volontairement hors CI** : Airflow et Flink réel (profils Docker
`airflow`/`flink`, non démarrés par défaut) — pertinents pour la démo
soutenance mais pas nécessaires pour valider chaque commit, et Snowflake
(dry-run sans compte réel, cf. `docs/ARCHITECTURE.md`) n'a pas de test
d'intégration possible sans credentials.

---

## 3. Cycle de vie du modèle — résumé

Détail complet : [ML_INTEGRATION_STRATEGY.md](ML_INTEGRATION_STRATEGY.md).

- **Deux moteurs de scoring** derrière un seul point d'entrée
  (`score_transaction()`), sélectionnés par `SCORING_ENGINE` : `rules`
  (défaut, déterministe, pas de dépendance modèle) ou `ml` (XGBoost, avec
  fallback automatique sur `rules` si le modèle n'existe pas encore).
- **Label** = vérité terrain du générateur (`is_fraud_pattern`), jamais la
  décision du moteur à règles — évite un entraînement circulaire.
- **Split temporel** (pas aléatoire) pour éviter la fuite de données via les
  features de vélocité (fenêtre glissante par client).
- **Tracking** : chaque run est loggé dans MLflow (params, métriques,
  artefact), versionné dans le Model Registry (`fraud-detector`).
- **Chargement** : `ml/scoring.py::load_model()` — cache mémoire **avec
  hot-reload** basé sur le `mtime` du `.pkl` (cf. §6 pour l'incident qui a
  motivé ce choix).

---

## 4. Notebook d'audit — `notebooks/audit_data_ml.ipynb`

Outil de contrôle exécutable, pas juste un rapport. 8 sections, chacune
terminée par des `assert` :

1. Qualité des données sources (Postgres) — intégrité référentielle,
   doublons, valeurs aberrantes, équilibre des classes
2. Cohérence inter-stores (Postgres → Kafka → MongoDB) — notamment le
   garde-fou anti-régression sur le bug de double-scoring (§5.1)
3. Distribution des features entraînement vs service (détection de skew)
4. Performance réelle **servie** (pas resimulée) — matrice de confusion,
   ROC/PR, seuils alignés sur `ml/monitor.py`
5. Audit de l'artefact modèle (features alignées, écart offline/online)
6. Détection de drift (Evidently)
7. Historique `ml_monitoring` (précision/rappel/drift dans le temps, avec
   marqueurs de réentraînement)
8. Synthèse — à relancer seule avant une démo/soutenance

**Utilisation** :
```bash
make notebook        # ouvre Jupyter Lab pour explorer interactivement
make notebook-check  # relance tout, exit != 0 si régression détectée
```

Tourne dans le kernel `stripe-polyglot` (`.venv`, cf. `requirements.txt` —
section jupyter/ipykernel/nbformat/nbconvert). Enregistré automatiquement
par `make install` puis, si besoin, `python -m ipykernel install --user
--name stripe-polyglot` (fait par la CI à chaque run, cf. `ci.yml`).

---

## 5. Incidents documentés (post-mortems)

Le monitoring et l'audit ont réellement servi à détecter et corriger des
problèmes en conditions live pendant le développement — matière concrète
pour la soutenance plutôt que des scénarios théoriques.

### 5.1 — Double-scoring via écho CDC (2026-09-15)

**Symptôme** : précision servie mesurée à **29%** (recall stable ~96%).

**Root cause** : `producers/flink_like_job.py` écrit `fraud_score` dans
Postgres après scoring (write-back). Debezium capte cet `UPDATE` et le
republie sur le **même topic** que le consumer écoute → chaque transaction
produisait 2 événements CDC, tous les deux scorés : vélocité Redis
incrémentée 2x, transaction rescorée et ré-émise vers Mongo 2x. Le modèle
lui-même (entraîné en SQL pur sur Postgres, jamais exposé à Redis) n'était
pas en cause — c'est un train/serve skew côté chaîne de service, pas côté
modèle.

**Correctif** : garde `if txn.get("fraud_score") is not None: continue`
juste après le parsing du message CDC, avant tout calcul de features.
Précision servie après correctif : **97%** (1858 transactions).

**Bug connexe découvert dans la foulée** : `ml/scoring.py::load_model()`
ne rechargeait le `.pkl` qu'une fois par process. `ml-monitor` avait
déclenché 2 réentraînements automatiques sans que le scorer déjà lancé ne
les charge jamais — l'auto-retrain tournait dans le vide. Corrigé par un
check de `mtime` à chaque appel de `load_model()`.

Détails complets, chiffres minute par minute :
[ML_INTEGRATION_STRATEGY.md §6.1 et §7.1](ML_INTEGRATION_STRATEGY.md#61--incident-documenté--auto-retrain-dans-le-vide-2026-09-15),
notebook §2 et §4.3.

### 5.2 — Corrélation apprise "vélocité faible = fraude" transformée en piège par un reset Redis (2026-09-15)

**Symptôme observé** : après une purge Redis (`FLUSHALL`, faite ce jour-là
pour nettoyer l'état contaminé par l'incident 5.1), la précision servie
s'effondre immédiatement (dès la 1ère minute, pas progressivement) et reste
basse (22-50%) malgré plusieurs réentraînements automatiques successifs —
chacun avec d'excellentes métriques **offline** (~95-96%), ce qui exclut un
problème de qualité du modèle lui-même.

**Root cause, trouvée en inspectant les faux positifs un par un**
(`notebooks/audit_data_ml.ipynb` §2 et une requête MongoDB ciblée sur
`fraud_alerts`) : **tous** les faux positifs partagent `velocity_1h=1,
velocity_24h=1` — un montant normal, un pays non à risque, aucun autre
signal fort. Or `producers/transaction_producer.py::pick_customer_pm()`
**cible délibérément des clients new/inactive** pour générer les patterns
de fraude synthétiques (`is_fraud_pattern=true`) — dans les données
d'entraînement, "vélocité quasi nulle" est donc un signal **réel et
légitime** de fraude, que le modèle apprend correctement à chaque
réentraînement (et d'autant plus fortement que l'historique
d'entraînement s'accumule).

Le piège : un `FLUSHALL` remet `velocity_1h`/`velocity_24h` à zéro pour
**tous** les clients, y compris les plus fidèles/légitimes. Pendant que
Redis reconstruit son historique, un client réellement actif depuis des
heures a temporairement la même signature (`v1h=1`) qu'un client
authentiquement nouveau — le modèle, qui a bien appris la corrélation
"vélocité faible → fraude" (car réelle dans ses données d'entraînement),
ne peut pas distinguer les deux cas et déclenche une salve de faux
positifs sur du trafic parfaitement légitime.

**Ce n'est pas un bug du modèle ni de la chaîne CDC** (l'incident 5.1 est
resté corrigé — vérifié, aucun `txn_id` dupliqué). C'est une **fragilité
opérationnelle d'une feature corrélée à un événement d'infrastructure** :
la vélocité encode à la fois "nouveauté réelle du client" (signal métier
valide) et "fraîcheur du cache" (artefact d'infra) sans que le modèle
puisse les distinguer — un reset de feature store, une migration Redis,
ou un cold-start en production reproduirait exactement ce symptôme.

**Mitigation appliquée le jour même (opérationnelle, pas de code)** :
scorer redémarré en `SCORING_ENGINE=rules` (défaut) le temps que la
vélocité Redis se reconstitue naturellement — le moteur à règles n'a pas
cette corrélation apprise (sa règle `velocity_1h > 10` ne réagit qu'à une
vélocité **haute**, jamais basse) et n'est donc pas affecté par ce mode de
défaillance. C'est aussi pourquoi la CI (§2) utilise `rules` et non `ml` :
un burst de trafic court en CI ressemble structurellement à un post-flush
(beaucoup de `velocity_1h=1` par manque de temps pour accumuler de
l'historique) — tester `ml` en CI heurterait le même piège à chaque run.

**Pistes de correction (non implémentées — décision consciente plutôt que
correctif hâtif sans recul)** :
- Distinguer les deux sens de "vélocité faible" en ajoutant une feature
  d'ancienneté réelle du client (`customers.created_at`), indépendante de
  l'état du cache Redis
- Au démarrage du scorer (ou après un `FLUSHALL`), *backfill* Redis depuis
  Postgres (mêmes sous-requêtes corrélées que `extract_training_data()`)
  plutôt que de repartir d'un cache vide
- Dans `ml/monitor.py`, ne pas déclencher de réentraînement automatique si
  une fraction anormalement élevée de la fenêtre courante a `velocity_1h`
  proche de 0 (signal d'un feature store qui vient d'être reset, pas d'une
  vraie dérive à corriger par un nouveau modèle)

---

## 6. Ce qui reste hors périmètre MLOps

Cf. [ML_INTEGRATION_STRATEGY.md §8](ML_INTEGRATION_STRATEGY.md#8-ce-qui-reste-hors-périmètre)
pour la liste complète (explicabilité SHAP, RBAC Mongo, etc.). Spécifique à
l'axe MLOps :

- **Pas de gate de promotion sur le registre de modèles** : chaque
  réentraînement écrase directement la version servie (dernière version
  MLflow = version active). Pas de comparaison automatique offline avant
  promotion, pas de rollback automatique si un nouveau modèle est pire.
- **Pas de garde-fou "warm-up"** sur le réentraînement automatique (cf. §5.2).
- **CI ne teste pas le chemin `SCORING_ENGINE=ml`** (moteur à règles
  utilisé pour rester rapide et déterministe) — le modèle ML est validé
  séparément par `make notebook-check` en local, pas par la CI actuelle.
