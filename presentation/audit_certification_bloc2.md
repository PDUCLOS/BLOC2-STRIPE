# Audit certification — Bloc 2 Stripe

*Relecture du 15 septembre 2026 : dépôt `Projet bloc 2` (commit eeca50d), slides `presentation/stripe_presentation_bloc2.pptx`, document des compétences et énoncé Stripe. Jury le 3 octobre, soit dans 18 jours.*

## Mise à jour du 15 septembre 2026 (soir) — état des actions

| Action de l'audit | Statut | Où |
|---|---|---|
| P0.1 Terraform (C4) | ✅ Écrit et validé en CI, non appliqué | `terraform/`, job CI `terraform`, `make tf-validate` |
| P0.2 Vidéo | ⏳ À réenregistrer par le candidat | Script : `presentation/script_video_bloc2.md` |
| P0.3 Slides alignées | ✅ | `stripe_presentation_bloc2.pptx` (diagrammes à jour, slide cible AWS, métriques servies) |
| P0.4 Code RNCP harmonisé | ✅ RNCP41993 partout (docs, slides, docx) | — |
| P1.5 Snowflake réel | ⏳ Compte à ouvrir ; étapes documentées | README, « Passer à un compte Snowflake payant » |
| P1.6 `fraud_indicators` alimentée | ✅ Écrite par le scorer, dans la même transaction que le score ; testée | `producers/flink_like_job.py`, `tests/test_e2e.py` |
| P1.7 Requêtes dans le dépôt et testées | ✅ | `queries/`, `make queries-check` (CI) |
| P1.8 Diagramme physique AWS | ✅ | `presentation/stripe_aws_cible.drawio` |
| P1.9 Chiffrage FinOps | ✅ ~3 100 $/mois prod, ~1 000 $ dev, 5 leviers | `docs/FINOPS.md` |
| P2.10-11 Bugs `load_snowflake.py`, `MONGO_URI` | ✅ Corrigés avant cette mise à jour | — |
| P2.12-14 `PRESENTATION.md` obsolète | ✅ Métriques, broker, onglets, moteur ML | `docs/PRESENTATION.md` |
| P2.15 Documents Word | ✅ Régénérés sur l'implémentation réelle | `stripe_architecture_bloc2.docx`, `stripe_bloc2_competences.docx`, `generators/` |
| Nouveau : `anonymize_customer()` documentée mais absente | ✅ Implémentée | `init/postgres/02_rgpd.sql` |

Les sections ci-dessous sont l'audit d'origine, conservé pour la traçabilité.

---

## Référentiel à retenir

Le titre **RNCP41993 « Architecte en intelligence artificielle »** (Jedha) est enregistré depuis le 27/02/2026 et **remplace le RNCP38777**. Son bloc 2 s'intitule « Concevoir et déployer l'infrastructure de données et de calcul pour l'IA ».
**Modalités d'évaluation du bloc 2 :** plan d'infrastructure (diagrammes), **code de déploiement hébergé sur GitHub** et **capture vidéo de l'infrastructure en production**.

Vos documents mélangent les deux codes :

- `docs/PRESENTATION.md` et `docs/ARCHITECTURE.md` indiquent RNCP 38777 ;
- les slides, le document des compétences, les autres docs et le dashboard indiquent RNCP41993.

→ Vérifiez sur votre convention Jedha le code qui s'applique à votre session, puis harmonisez partout.

---

## 1. Couverture des 8 livrables de l'énoncé

| Livrable | Statut | Où | Remarque |
|---|---|---|---|
| Diagramme d'architecture globale | ✅ | `stripe_architecture_globale.png`, slide 4 | Fidèle au code. Le PNG a un fond transparent : il devient illisible sur fond sombre. L'étiquette « ml/models/*.pkl » chevauche le bloc ml-monitor. |
| ERD OLTP | ✅ | `stripe_erd_oltp.png`, DDL | 6 tables en 3NF, index, clé d'idempotence |
| Schéma OLAP | ⚠️ | `OLAP_SCHEMA_DESIGN.md`, `snowflake_setup.py` | L'énoncé demande des **tables pré-agrégées / vues matérialisées** côté OLAP. Il n'y en a que dans PostgreSQL ; Snowflake reste un « plan » (Dynamic Tables). Le chargement ne tourne qu'en dry-run. |
| Modèle NoSQL | ✅ | `NOSQL_DATA_MODEL.md`, `init/mongo` | Embedding, referencing, TTL, index : tout est couvert |
| Architecture du pipeline | ✅ | `ARCHITECTURE.md`, `PRESENTATION.md` | Batch + streaming + CDC + DLQ + Airflow |
| Plan sécurité / conformité | ✅ | `SECURITY_COMPLIANCE_PLAN.md` | Contient une gap analysis honnête : c'est un point fort |
| Stratégie d'intégration ML | ✅ | `ML_INTEGRATION_STRATEGY.md` | XGBoost, MLflow, Evidently, réentraînement automatique |
| **Requêtes SQL et NoSQL** | ⚠️ | `stripe_queries.sql`, `stripe_queries_nosql.js` | **Absentes du dépôt GitHub** : les deux fichiers sont à la racine du dossier et datent de juin. Plusieurs requêtes ne correspondent plus au code (voir point 7). |

## 2. Couverture des 9 compétences du bloc 2

| Compétence | Statut | Preuve dans le projet | Trou éventuel |
|---|---|---|---|
| C1 Spécifications d'infrastructure | ✅ | Tableau CPU/GPU/stockage/réseau par usage (document des compétences) | — |
| C2 Architecture logique et physique | ⚠️ | Architecture logique très solide | **Pas de diagramme physique de la cible AWS** (VPC, AZ, subnets, PrivateLink) : seulement un tableau |
| C3 Arbitrage Cloud / On-prem / Serverless | ✅ | Matrice de décision | Absente des slides : à placer à l'oral sur la slide 11 |
| **C4 Infrastructure as Code (Terraform)** | ❌ | Seulement un extrait « illustratif » dans le docx | **Aucun fichier `.tf` dans le dépôt**, alors que la modalité exige du « code de déploiement sur GitHub » et que la slide 11 affirme « Terraform » |
| C5 Structurer les environnements (lake, vectoriel, conteneurs) | ⚠️ | Conteneurs réels, lake et vectoriel décrits | Rien d'implémenté pour le lake ni pour le vectoriel. Acceptable si c'est assumé. |
| C6 Résilience / haute disponibilité | ⚠️ | DLQ, idempotence, arrêt gracieux, cible multi-AZ | PoC : 1 broker, facteur de réplication 1. `PRESENTATION.md` dit à tort « 3 brokers ». |
| C7 Sécurité (IAM, chiffrement, secrets) | ✅ | Rôles PG (`replication_user`, `analytics_reader`), `.env` hors Git, pgcrypto, SHA-256 | TLS désactivé en démo et un seul utilisateur Mongo : c'est documenté, il faut juste savoir l'expliquer |
| C8 FinOps / GreenOps | ⚠️ | Principes listés | **Aucun chiffrage** et rien dans les slides |
| C9 Documenter / coordonner | ✅ | README, ARCHITECTURE, FILE_FUNCTION_INDEX, demo.sh, Makefile, tests | Le docx cite `REFERENCE_TECHNIQUE.md`, `IMPLEMENTATION.md`, des ADR, une CI/CD et des PR, **qui n'existent pas dans le dépôt** |

---

## 3. Actions avant le jury, par priorité

### 🔴 P0 — risque de non-validation

1. **Terraform (C4).** Ajoutez un dossier `terraform/` avec des modules `network`, `rds`, `msk`, `elasticache`, `security` (KMS, IAM, Secrets), un backend S3 et un `variables.tf` par environnement. Pas besoin de `apply` : un `terraform validate` et un `terraform plan` suffisent à prouver que le code est réel. À défaut, retirez « Terraform » de la slide 11 et présentez `docker-compose.yml` comme votre IaC.
2. **Vidéo (modalité d'évaluation).** `STRIPE Video.mp4` dure **1 min 13** et date du **21 juillet**, donc d'avant XGBoost, MLflow, Evidently et Airflow. Réenregistrez-la avec la stack actuelle : `./demo.sh` et `docker compose ps`, le topic CDC, le dashboard (onglets Vue d'ensemble et Performance ML), l'UI MLflow, le DAG Airflow et `make test`.
3. **Slides à aligner sur la nouvelle version** (le speech est déjà rédigé pour la version corrigée) :
   - **Slides 1 et 12** : remplacer « Juin 2026 » par la date de la session, et harmoniser le code RNCP.
   - **Slide 6** :
     - la puce « paiement et indicateur de fraude écrits atomiquement » est **fausse** : aucun code n'écrit dans `fraud_indicators`, le write-back met seulement à jour `transactions.fraud_score` (point 6) ;
     - « Multi-AZ, failover < 30 s » : préciser « (cible) ».
   - **Slide 8** :
     - les noms des dimensions ne correspondent pas au code : le code a `dim_payment_method` et `dim_geography`, la slide `dim_payment` et `dim_currency` ;
     - « partitionnement par mois » : Snowflake fait du **clustering** `(date_key, merchant_key)` ;
     - `mv_daily_revenue` est une vue **PostgreSQL**, pas une vue OLAP.
   - **Slide 9** : `ml_features` n'est pas « par transaction ». C'est un **snapshot par client**, mis à jour en upsert sur `customer_id`. Les 3 cartes oublient `transaction_logs`, `fraud_alerts` et `ml_monitoring`.
   - **Slide 10** : remplacer « 5 règles → score » et « Cible : SageMaker XGBoost » par « Règles **ou** XGBoost (fallback automatique) · MLflow · Evidently · réentraînement auto », et ajouter les métriques.
   - **Slide 11** : ajouter Airflow, MLflow et Evidently dans la colonne PoC.
   - **Slide 4** : un bandeau « PAS DANS LE PIPELINE LIVE » reste visible sur Snowflake. C'est honnête, mais le point 5 peut le supprimer.
4. **Code RNCP** : harmoniser partout (voir plus haut).

### 🟠 P1 — le jury le verra ou posera la question

5. **Snowflake en dry-run.** Un compte d'essai Snowflake gratuit se crée en 10 minutes. Ensuite, `make snowflake-setup` puis `make snowflake-export`, et une capture d'écran : vous rendez réelle la seule brique « simulée ». ⚠️ **Avant de le faire, corrigez le bug du point 10**, sinon le premier vrai chargement échouera.
6. **`fraud_indicators` n'est jamais alimentée.** Deux options :
   - ajouter un `INSERT INTO fraud_indicators` dans la même transaction que l'UPDATE du write-back (dans `flink_like_job.py`), ce qui rend la puce de la slide 6 exacte ;
   - ou corriger la slide.
7. **Requêtes (livrable 8).** Déplacez-les dans le dépôt (`queries/`), puis alignez-les sur le code et testez-les sur la stack qui tourne. Trois écarts à corriger :
   - la requête NoSQL 1.2 (« décisions par version de modèle ») et la 1.3 lisent `ml_features` comme s'il y avait un document par transaction : il faut les réécrire sur `transaction_logs` ou `fraud_alerts` ;
   - la requête 2.2 (section OLAP) interroge `mv_daily_revenue`, qui est une vue PostgreSQL ;
   - la section OLAP ne cible pas `dim_geography`.
8. **Diagramme physique de la cible AWS** (C2, C6) : VPC, 2 à 3 AZ, subnets privés, RDS Multi-AZ, MSK sur 3 AZ, ElastiCache, Atlas via PrivateLink, MWAA, SageMaker, KMS et Secrets Manager. Une slide de backup suffit.
9. **Chiffrage FinOps (C8)** : un ordre de grandeur mensuel de la cible (AWS Pricing Calculator et Snowflake), avec 2 ou 3 leviers chiffrés. Votre comparatif Snowflake vs Databricks d'août peut servir de base.

### 🟡 P2 — cohérence et bugs de code

10. **Bug réel dans `etl/load_snowflake.py` (upsert de `dim_merchant`)**, masqué par le dry-run :
    - l'INSERT vise une colonne `country_code`, mais la table créée par `snowflake_setup.py` s'appelle `country` : l'INSERT échouera dès le premier chargement réel ;
    - `name` et `email` ne sont jamais extraits ni insérés, donc ils resteront NULL.
    Corriger la colonne en `country` et ajouter `m.name` et `m.email` à l'extraction et à l'INSERT.
11. **`tests/test_e2e.py:33`** : `MONGO_URI` retombe sur `stripe_app:stripe_pass`. Or `.env` ne définit pas `MONGO_URI` et `init_env.sh` génère des mots de passe aléatoires, donc les tests Mongo échoueront. Il faut construire l'URI à partir de `MONGO_APP_USER`, `MONGO_APP_PASSWORD` et `MONGO_DB`.
12. **Métriques obsolètes dans `PRESENTATION.md` §11** : le document affiche F1 0,93 et AUC 0,985 sur 810 transactions. Le modèle actuel (`fraud_xgboost-v1.meta.json`, 15/09) donne :
    - **précision 0,81 · recall 0,96 · F1 0,88 · AUC 0,98 sur 4 923 transactions de test** ;
    - l'affirmation « 100 % des 5 % injectés » est à retirer.
13. **Taux de fraude du jeu de test : 23 %** (1 154 / 4 923), alors que le générateur en injecte 5 %. Préparez l'explication (injections des tests E2E ? patterns de vélocité ?). La précision serait plus basse avec 5 % de fraude.
14. **Autres incohérences dans `PRESENTATION.md`** :
    - « Kafka 3 brokers » (il y en a 1) ;
    - « Dashboard 5 pages » (il y a 2 onglets) ;
    - le §10 dit encore « le modèle est rule-based » ;
    - le schéma principal montre un JobManager PyFlink alors que la démo utilise le job Python.
15. **Document des compétences (`stripe_bloc2_competences.docx`)**, à mettre à jour :
    - le tableau PoC indique encore « Scripts + Makefile » (au lieu d'Airflow), « Scoring par règles » (au lieu de règles et XGBoost), et « modèle » pour la ligne OLAP ;
    - la date affichée est « Juin 2026 » ;
    - il cite des documents absents (voir C9).
16. **Petits nettoyages** :
    - ternaire identique des deux côtés dans `transaction_producer.py:30` (et dans `flink_like_job.py`) ;
    - dossier `common/` qui ne contient que `__pycache__` ;
    - `test_pg_materialized_views` vérifie que les vues existent, pas qu'elles sont remplies ;
    - `load_snowflake.py:199` : l'expression fonctionne, mais `r.get("fraud_score", 0) and …` est à simplifier ;
    - index partiel `merchants(status) WHERE status='active'` : optionnel.
17. **Deux versions du deck** : `stripe_presentation_bloc2.pptx` à la racine (17 juin) et dans `presentation/` (15 sept.). Même chose pour `IMPLEMENTATION.md`, `PRESENTATION.md` et les requêtes à la racine. Il faut remettre les bonnes versions.

---

## 4. Contre-vérification de l'audit « Tour 2 » que vous m'avez transmis

| Point | Verdict |
|---|---|
| 4 faux positifs retirés (refresh des vues, `analytics_reader`, `upsert_dimensions`, logo) | ✅ D'accord |
| `dim_merchant.name` et `email` jamais peuplés | ✅ Confirmé. **Il a manqué le plus grave** : `country_code` ≠ `country`, donc l'INSERT plante (point 10). |
| `MONGO_URI` en dur dans `test_e2e.py` | ✅ Confirmé (`MONGO_URI` absent de `.env`) |
| `load_snowflake.py:199` | ✅ Fonctionne, c'est seulement stylistique |
| « Aligner expire 87000 → 86500 entre les 2 jobs » | ❌ **Déjà fait** : les deux jobs utilisent 86500 |
| Ternaire mort, `common/` orphelin, test des vues superficiel | ✅ Confirmé |
| « Statut certification : ✅ OK » | ⚠️ **Trop optimiste.** Cet audit ne regarde que le code. Il ne confronte pas le projet aux modalités d'évaluation du bloc : Terraform absent, vidéo obsolète, slides désalignées. |

## 5. Points forts à mettre en avant

- Le PoC fonctionne réellement de bout en bout : CDC, scoring, write-back idempotent, DLQ, dashboard et tests.
- Les compromis sont documentés honnêtement (Flink sur ARM64, Snowflake en dry-run, pas de rechargement à chaud du modèle, gap analysis sécurité) : un jury d'architectes apprécie.
- La chaîne MLOps est complète : double moteur avec fallback, MLflow Registry, et Evidently qui surveille à la fois la dérive et la performance.
- Le RGPD est appliqué « par construction » : TTL, `anonymize_customer()`, empreinte SHA-256, rôle `analytics_reader`.
