# Terraform — cible AWS de Stripe Polyglot

Ce dossier décrit en code la **cible industrialisée** présentée en soutenance
(slide « Du PoC local à la cible cloud »). Le PoC local tourne sous
`docker-compose.yml` ; ici, chaque service a son équivalent managé AWS, avec le
même découpage logique.

> **État honnête :** le code est **validé** (`terraform fmt` + `terraform validate`
> en CI, job `terraform` de `.github/workflows/ci.yml`) mais **n'a jamais été
> appliqué** : aucun compte AWS/Atlas n'est rattaché au projet (contrainte budget).
> Un `terraform plan` réel nécessite des credentials, voir « Déployer » plus bas.

## Correspondance PoC → cible

| PoC local (`docker-compose.yml`) | Module | Service AWS |
|---|---|---|
| `postgres` (16, `wal_level=logical`) | [`modules/rds`](modules/rds/main.tf) | RDS PostgreSQL 16 Multi-AZ, `rds.logical_replication=1`, TLS forcé, réplica de lecture |
| `kafka` (1 broker KRaft) + `debezium` | [`modules/msk`](modules/msk/main.tf) | MSK 3 brokers / 3 AZ, RF 3, `min.insync.replicas=2`, IAM + TLS |
| `redis` (feature store vélocité) | [`modules/elasticache`](modules/elasticache/main.tf) | ElastiCache Redis 7 Multi-AZ, chiffré, AUTH token |
| `mongo` + `init/mongo/*.js` | [`modules/mongodb_atlas`](modules/mongodb_atlas/main.tf) | MongoDB Atlas 7 via PrivateLink, utilisateur `readWrite` limité à `stripe_nosql` |
| `producers/*.py`, `ml-monitor`, `dashboard` | [`modules/compute`](modules/compute/main.tf) | ECR + ECS Fargate ARM64, secrets injectés depuis Secrets Manager |
| `airflow` + `dags/` | [`modules/airflow`](modules/airflow/main.tf) | Amazon MWAA, interface privée |
| volume `mlflow-artifacts`, archive > 2 ans | [`modules/storage`](modules/storage/main.tf) | S3 SSE-KMS, versioning, cycle de vie IA → Glacier IR |
| `.env` / `scripts/init_env.sh` | [`modules/security`](modules/security/main.tf) | KMS (CMK, rotation), Secrets Manager, security groups, rôles IAM |
| réseau bridge Docker | [`modules/network`](modules/network/main.tf) | VPC 3 AZ : sous-réseaux public (NAT) / app / data sans route Internet, Flow Logs |
| — | [`stack/main.tf`](stack/main.tf) | Alarmes CloudWatch (CPU RDS, lag du scorer), SNS, budget mensuel |

Hors périmètre, assumé : **Snowflake** (provisionné par `etl/snowflake_setup.py`),
**Debezium sur MSK Connect** (plugin personnalisé à packager), **SageMaker**
(le modèle XGBoost reste servi dans le conteneur scorer, cf. `ml/scoring.py`).

## Arborescence

```
terraform/
├── bootstrap/          bucket S3 du state (appliqué une seule fois)
├── modules/            un module par brique, réutilisable
├── stack/              composition de tous les modules + observabilité/FinOps
└── envs/
    ├── dev/            petites tailles, 1 NAT, pas de Multi-AZ RDS (~1 000 $/mois)
    └── prod/           Multi-AZ partout, NAT par AZ, réplica (~3 100 $/mois)
```

Les chiffrages de budget sont détaillés dans [`docs/FINOPS.md`](../docs/FINOPS.md).

## Vérifier (sans compte AWS)

```bash
make tf-validate
```

Équivaut à `terraform fmt -check` puis `init -backend=false` + `validate` sur
`bootstrap`, `envs/dev` et `envs/prod`.

## Déployer (avec compte AWS + Atlas)

```bash
export AWS_PROFILE=stripe-polyglot
export MONGODB_ATLAS_PUBLIC_KEY=... MONGODB_ATLAS_PRIVATE_KEY=...

terraform -chdir=terraform/bootstrap init && terraform -chdir=terraform/bootstrap apply
cp terraform/envs/dev/terraform.tfvars.example terraform/envs/dev/terraform.tfvars   # renseigner
terraform -chdir=terraform/envs/dev init
terraform -chdir=terraform/envs/dev plan -out=dev.tfplan
terraform -chdir=terraform/envs/dev apply dev.tfplan
```

Promotion en prod : même commit, `image_tag` déjà validé en dev, `plan` relu
avant `apply`. Le state est séparé par environnement (`dev/` et `prod/` dans le
même bucket), verrouillé par fichier `.tflock`.

## Choix de sécurité notables

- Aucun port data ouvert à un CIDR : RDS, MSK, Redis et Atlas n'acceptent que
  le security group applicatif.
- Sous-réseaux data sans route vers Internet.
- Mot de passe maître RDS généré et stocké par RDS (`manage_master_user_password`),
  jamais visible dans le code.
- Kafka en authentification IAM : plus aucun mot de passe Kafka à gérer.
- Une CMK KMS chiffre RDS, MSK, Redis, S3, logs, ECR et secrets ; rotation annuelle.
