# Script de la vidéo — infrastructure en fonctionnement (bloc 2)

La modalité d'évaluation du bloc 2 demande une **capture vidéo de l'infrastructure
en production**. La vidéo actuelle (`STRIPE Video.mp4`, 1 min 13, juillet 2026)
date d'avant XGBoost, MLflow, Evidently, Airflow, Terraform et les requêtes : elle
est à réenregistrer avec ce script.

**Durée cible : 4 à 5 minutes.** Enregistrement d'écran macOS : `Cmd + Maj + 5`,
« Enregistrer tout l'écran », micro activé. Terminal en police 16 pt minimum.

## Préparation (hors caméra, 10 minutes avant)

```bash
cd "Projet bloc 2"
docker compose up -d                       # stack complète
bash scripts/create_topics.sh              # idempotent
bash scripts/deploy_debezium.sh            # connecteur CDC
set -a && source .env && set +a
SCORING_ENGINE=ml nohup .venv/bin/python -u producers/flink_like_job.py > /tmp/scorer.log 2>&1 &
nohup .venv/bin/python -u producers/mongo_writer.py > /tmp/mongo.log 2>&1 &
nohup .venv/bin/python -u producers/transaction_producer.py --rate 5 > /tmp/producer.log 2>&1 &
```

Attendre 3 à 5 minutes que le trafic se stabilise (Redis se remplit, les
premières minutes après un redémarrage donnent plus de faux positifs).
Ouvrir à l'avance les onglets : dashboard http://localhost:8501, MLflow
http://localhost:5001, Airflow (profil `airflow`) et le dépôt GitHub (onglet Actions).

> Si `mongo_writer.py` ne démarre pas sans message, le `.venv` sur Google Drive
> n'est pas disponible hors connexion : voir README, « Projet sur Google Drive ».

## Déroulé

| # | Durée | À l'écran | Commande ou action | À dire |
|---|---|---|---|---|
| 1 | 0:20 | Diagramme `stripe_architecture_globale.png` | — | « Architecture polyglotte : PostgreSQL, Kafka alimenté par CDC, Redis, MongoDB, scoring XGBoost, et Snowflake conçu mais en dry-run. Tout ce que vous allez voir tourne réellement. » |
| 2 | 0:20 | Terminal | `docker compose ps` | « Neuf services en conteneurs, tous en bonne santé. » |
| 3 | 0:25 | Terminal | `curl -s localhost:8083/connectors?expand=status \| python3 -m json.tool \| grep state` puis `docker exec stripe-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic stripe.public.transactions --max-messages 2` | « Debezium lit le journal de PostgreSQL et publie chaque transaction dans Kafka, sans que la base n'appelle personne. » |
| 4 | 0:30 | Terminal | `tail -3 /tmp/scorer.log` puis `docker exec stripe-postgres psql -U stripe_app -d stripe_oltp -c "SELECT decision, model_version, count(*) FROM fraud_indicators GROUP BY 1,2;"` | « Le scorer calcule le score avec XGBoost, l'écrit dans PostgreSQL et trace chaque blocage dans `fraud_indicators`, dans la même transaction. » |
| 5 | 0:40 | Dashboard | Login, onglet Vue d'ensemble puis Performance ML | « L'accès est protégé. On voit les KPIs, les transactions suspectes, puis la dérive et la précision servie mesurées par Evidently. » |
| 6 | 0:20 | MLflow | Expérience `fraud-detector`, dernier run | « Chaque entraînement, manuel ou automatique, est tracé avec ses métriques. » |
| 7 | 0:30 | Terminal | `make queries-check \| tail -25` | « Les requêtes SQL et MongoDB du livrable s'exécutent sur la stack à chaque push. Ici la précision servie et l'anonymisation RGPD, jouée puis annulée. » |
| 8 | 0:25 | Terminal | `make test \| tail -8` | « Seize contrôles de bout en bout, tous verts. » |
| 9 | 0:35 | Éditeur puis terminal | Ouvrir `terraform/stack/main.tf`, puis `make tf-validate` | « La cible AWS est écrite en Terraform : dix modules, validés en CI. Elle n'a pas été appliquée faute de compte, je le dis clairement. » |
| 10 | 0:20 | Diagramme `stripe_aws_cible.png` | — | « VPC sur trois zones, bases sans accès Internet, tout chiffré par KMS. Environ 3 100 dollars par mois en production. » |
| 11 | 0:20 | GitHub, onglet Actions | Dernier run vert | « Lint, test de bout en bout sur la stack complète et validation Terraform à chaque push. » |

## Après l'enregistrement

- Exporter en 1080p, nommer `STRIPE_Video_bloc2_2026-10.mp4`.
- Ne pas le versionner dans Git s'il dépasse 50 Mo (limite GitHub) : le déposer
  sur la plateforme Jedha et mettre le lien dans le README.
- Vérifier qu'aucun secret n'apparaît à l'écran (`.env`, mot de passe du dashboard).
