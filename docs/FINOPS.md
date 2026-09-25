# FinOps & GreenOps — chiffrage de la cible AWS

Certification AIA RNCP41993 — Bloc 2 (compétence C8)

> **Nature des chiffres :** ordres de grandeur mensuels, prix publics
> **à la demande** en `eu-west-1` (Irlande), relevés en septembre 2026 pour
> les dimensionnements de [`terraform/envs/dev`](../terraform/envs/dev/main.tf)
> et [`terraform/envs/prod`](../terraform/envs/prod/main.tf). Ce ne sont pas
> des devis : à reconfirmer dans l'AWS Pricing Calculator, la page de prix
> MongoDB Atlas et celle de Snowflake avant tout engagement. Les budgets
> Terraform (`aws_budgets_budget`) reprennent ces montants avec une marge.

## 1. Coût du PoC actuel

| Poste | Coût |
|---|---|
| Stack Docker Compose sur poste personnel | 0 € (hors électricité) |
| GitHub Actions (dépôt public) | 0 € |
| Snowflake (compte d'essai, 400 $ de crédits offerts) | 0,024 crédit mesuré pour le bootstrap, deux exports de 57 000 lignes et les requêtes OLAP (warehouse X-Small, `AUTO_SUSPEND = 60`) |

La contrainte budget explique les écarts assumés du PoC : 1 broker Kafka,
pas de TLS interne, Snowflake sur un compte d'essai.

## 2. Cible PROD — environ 3 100 $/mois

Hypothèses : 730 h/mois, trafic de l'ordre de quelques centaines de
transactions par seconde en pointe, 100 Go de données OLTP, 500 Go par broker.

| Brique | Dimensionnement (Terraform) | Calcul | $/mois |
|---|---|---|---|
| RDS PostgreSQL | `db.r7g.large` Multi-AZ + 1 réplica, 100 Go gp3 | ~0,26 $/h × 3 instances + stockage/sauvegardes | ~620 |
| MSK | 3 × `kafka.m7g.large`, 3 × 500 Go | ~0,20 $/h × 3 + 1 500 Go × 0,11 $ | ~610 |
| ElastiCache Redis | 2 × `cache.r7g.large` | ~0,24 $/h × 2 | ~350 |
| MongoDB Atlas | M30, 3 nœuds, sauvegarde continue | ~0,54 $/h + backup | ~450 |
| MWAA | `mw1.small` | ~0,49 $/h + stockage | ~380 |
| ECS Fargate ARM64 | 5 vCPU / 10 Go au total (3 scorers, writer, monitor, dashboard) | 5 × 0,032 $ + 10 × 0,0036 $ par heure | ~145 |
| Réseau | 3 NAT Gateway, endpoint PrivateLink Atlas, trafic inter-AZ | 3 × 35 $ + ~25 $ + ~50 $ | ~180 |
| Observabilité | CloudWatch Logs, Container Insights, Flow Logs, alarmes | forfait estimé | ~150 |
| S3, KMS, Secrets Manager, ECR | data lake ~500 Go, 1 CMK, 3 secrets | | ~35 |
| Snowflake | Standard, warehouse X-Small, auto-suspend 60 s, ~2,25 crédits/jour | ~68 crédits × ~2,6 $ + stockage | ~180 |
| **Total** | | | **~3 100** |

Budget Terraform prod : **4 000 $/mois** (alerte à 80 % du réel, et à 100 % du prévisionnel).

**Où va l'argent :** les quatre bases managées (RDS, MSK, Redis, Atlas)
pèsent ~65 % du total. C'est le prix de la haute disponibilité multi-AZ
exigée par un système de paiement : ce poste ne se rabote pas en prod.

## 3. Cible DEV — environ 1 000 $/mois

| Brique | Dimensionnement | $/mois |
|---|---|---|
| RDS | `db.t4g.medium` mono-AZ, sans réplica | ~65 |
| MSK | 3 × `kafka.t3.small`, 3 × 500 Go | ~265 |
| ElastiCache | 1 × `cache.t4g.small` | ~25 |
| Atlas | M10 | ~60 |
| MWAA | `mw1.small` | ~380 |
| Fargate | 3 vCPU / 6 Go | ~90 |
| Réseau, observabilité, S3, Snowflake de dev | 1 NAT, logs réduits | ~120 |
| **Total** | | **~1 000** |

Budget Terraform dev : **1 200 $/mois**.

## 4. Leviers d'optimisation chiffrés

| # | Levier | Économie estimée | Contrepartie |
|---|---|---|---|
| 1 | **Remplacer MWAA par EventBridge Scheduler + tâche ECS** pour l'unique DAG quotidien (`dags/stripe_daily_etl.py`) | ~350 $/mois par environnement (−35 % en dev, −11 % en prod) | Perte de l'UI Airflow, des retries et de la lignée des tâches ; à reconsidérer dès qu'il y a plusieurs DAGs |
| 2 | **Engagements 1 an** : Reserved Instances RDS et ElastiCache, Compute Savings Plan (Fargate) | ~−30 % sur ~1 100 $, soit ~330 $/mois en prod | Engagement ferme sur la taille d'instance |
| 3 | **Arrêt hors heures ouvrées en dev** (ECS `desired_count = 0`, RDS arrêtée, 12 h/jour + week-ends) | ~−40 % sur la partie arrêtable, ~200 $/mois | Environnement indisponible la nuit ; MSK et Atlas M10 ne s'arrêtent pas |
| 4 | **Volumes MSK de dev à 100 Go** au lieu de 500 Go | ~130 $/mois | Rétention Kafka plus courte en dev |
| 5 | **Snowflake : auto-suspend 60 s + resource monitor** plafonnant les crédits | Évite une dérive (un warehouse oublié allumé coûte ~1 900 $/mois) | Premier appel après suspension un peu plus lent |
| 6 | **Cycle de vie S3** (déjà dans Terraform) : IA à 90 j, Glacier IR à 365 j | ~−70 % sur le stockage archivé | Relecture d'archive facturée et plus lente |

Leviers 1 + 2 + 3 appliqués : prod ~2 400 $/mois, dev ~450 $/mois.

## 5. GreenOps

- **Graviton (ARM64) partout où c'est possible** : RDS `r7g`/`t4g`, MSK `m7g`,
  ElastiCache `r7g`/`t4g`, Fargate ARM64. AWS annonce jusqu'à 60 % d'énergie
  en moins à performance égale par rapport aux instances x86 comparables.
  Le PoC tourne déjà sur ARM (Mac M-series), l'image applicative est donc
  construite pour ARM dès aujourd'hui.
- **Pas de ressources inactives** : arrêt nocturne en dev (levier 3),
  auto-suspend Snowflake (levier 5), `max_workers = 2` sur MWAA.
- **Données froides en classes de stockage à faible empreinte** (levier 6) et
  TTL MongoDB (90 j / 30 j) qui évitent de stocker indéfiniment des logs.
- **Choix de région** : `eu-west-1` est retenu pour la latence vers les
  marchands européens et la disponibilité de tous les services (MWAA, MSK,
  Atlas PrivateLink). `eu-north-1` (Stockholm) a une électricité moins
  carbonée ; c'est une alternative à évaluer si tous les services y sont
  disponibles.
- **Mesure** : AWS Customer Carbon Footprint Tool (rapport mensuel par
  service), à suivre au même titre que le budget.

## 6. Gouvernance

- Tags obligatoires sur toutes les ressources (`default_tags` du provider) :
  `Project`, `Environment`, `Owner`, `ManagedBy`, `DataClass`. Ils permettent
  la ventilation des coûts par environnement dans Cost Explorer.
- Budget par environnement avec alertes e-mail (`aws_budgets_budget`, cf.
  [`terraform/stack/main.tf`](../terraform/stack/main.tf)).
- Revue mensuelle : écart budget/réel, ressources non taguées, instances
  sous-utilisées (CPU < 20 % sur 30 jours → réduire d'une taille).
