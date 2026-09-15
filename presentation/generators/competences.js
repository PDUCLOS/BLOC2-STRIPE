// Génère presentation/stripe_bloc2_competences.docx — couverture des 9 compétences
// du bloc 2, chaque affirmation renvoyant à une preuve dans le dépôt.
const fs = require("fs");
const { Packer } = require("docx");
const L = require("./lib");
const { h1, h2, p, bullets, code, note, table, image, cover, makeDoc, toc } = L;

const PRES = process.argv[2];
const OUT = process.argv[3];
const img = (f) => `${PRES}/${f}`;

const children = [
  ...cover(
    "Infrastructure de Données et de Calcul pour l’IA",
    "Couverture des compétences du Bloc 2",
    "Spécifications · Architecture · Cloud · IaC · Environnements · Résilience · Sécurité · FinOps · Coordination",
    [
      ["Candidat", "Patrice Duclos"],
      ["Certification", "Architecte en intelligence artificielle — RNCP41993 — Bloc 2"],
      ["Intitulé du bloc", "Concevoir et déployer l’infrastructure de données et de calcul pour l’IA"],
      ["Contexte", "Stripe — FinTech — Business Case"],
      ["Livrables", "PoC fonctionnel local, code de déploiement sur GitHub (Docker Compose + Terraform), diagrammes, vidéo"],
      ["Date", "Octobre 2026"],
    ],
  ),

  ...toc(),

  h1("Préambule — trois niveaux de preuve"),
  p("Ce document met en regard les neuf compétences du bloc 2 et le projet Stripe. Pour chaque compétence, il indique la **preuve** dans le dépôt `github.com/PDUCLOS/BLOC2-STRIPE`, et le niveau de cette preuve :"),
  ...table(["Niveau", "Signification", "Exemples"], [
    ["Exécuté", "Tourne sur la stack Docker et est vérifié automatiquement en CI", "Pipeline CDC, scoring XGBoost, write-back, MongoDB, Airflow, MLflow, Evidently, requêtes"],
    ["Codé et validé", "Présent dans le dépôt, vérifié par un outil, jamais déployé", "Terraform de la cible AWS"],
    ["Conçu", "Décrit et justifié, sans code exécutable", "Bases vectorielles, reprise après sinistre inter-régions"],
  ], [16, 44, 40]),
  p("Le PoC a été réalisé sur un poste personnel pour une raison de budget. La cible industrialisée reprend la même architecture logique : seuls les composants changent."),

  // 1
  h1("1. Synthèse : du PoC local à la cible industrialisée"),
  ...table(["Couche", "PoC local (Docker Compose) — exécuté", "Cible AWS — codée dans terraform/"], [
    ["OLTP", "PostgreSQL 16", "RDS PostgreSQL 16 Multi-AZ + réplica de lecture"],
    ["CDC / streaming", "Debezium 2.6 + Kafka KRaft (1 broker)", "MSK 3 brokers sur 3 AZ (Debezium sur MSK Connect : conçu)"],
    ["Traitement du flux", "Job Python « Flink-like » (PyFlink équivalent dans `flink/`)", "ECS Fargate ; Managed Flink quand le débit le justifie"],
    ["Feature store online", "Redis 7", "ElastiCache Redis 7 Multi-AZ"],
    ["NoSQL", "MongoDB 7", "MongoDB Atlas via PrivateLink"],
    ["OLAP", "Schéma Snowflake prêt, chargement en dry-run", "Snowflake (hors Terraform, compte payant à ouvrir)"],
    ["Orchestration batch", "Airflow 2.9, DAG quotidien", "Amazon MWAA"],
    ["ML", "XGBoost + repli sur règles, MLflow, Evidently, réentraînement automatique", "Même image sur ECS ; artefacts sur S3"],
    ["Restitution", "Dashboard Streamlit protégé par login", "Service ECS privé"],
    ["Provisioning", "`docker-compose.yml`, Makefile", "Terraform : 10 modules, environnements dev et prod"],
    ["CI/CD", "GitHub Actions : lint, e2e sur stack complète, terraform validate", "Idem + plan/apply via fédération OIDC (conçu)"],
  ], [18, 41, 41]),

  // 2
  h1("2. Définir les spécifications techniques d’infrastructure"),
  note("**Compétence C1** — définir les spécifications techniques (calcul, stockage, réseau) adaptées aux usages.", "info"),
  p("Les charges sont très différentes : l’OLTP est sensible à la latence et à la concurrence, Kafka au débit disque et réseau, le feature store à la mémoire, l’inférence au CPU. Le dimensionnement de production ci-dessous est celui codé dans `terraform/envs/prod/main.tf`."),
  ...table(["Usage", "Calcul", "Stockage", "Réseau"], [
    ["OLTP (RDS)", "`db.r7g.large` (Graviton, mémoire-optimisé), Multi-AZ + 1 réplica", "gp3 100 Go, autoscaling ×5, sauvegardes 35 j", "Sous-réseaux data privés, TLS forcé"],
    ["Streaming (MSK)", "3 × `kafka.m7g.large`", "500 Go EBS par broker, rétention 7 j", "3 AZ, IAM + TLS sur 9098"],
    ["Feature store (ElastiCache)", "2 × `cache.r7g.large`", "En mémoire, éviction `volatile-lru` (clés à TTL)", "Latence < 1 ms intra-AZ"],
    ["NoSQL (Atlas)", "M30, replica set 3 nœuds", "Autoscaling disque, sauvegarde continue", "PrivateLink"],
    ["Scoring et services (Fargate)", "3 scorers 1 vCPU / 2 Go ARM64 ; writer, monitor, dashboard", "Sans état", "Sous-réseaux app privés"],
    ["Entraînement ML", "CPU suffisant : XGBoost sur ~100 000 lignes en quelques secondes", "S3 (artefacts)", "—"],
    ["OLAP (Snowflake)", "Warehouse X-Small, auto-suspend 60 s", "Colonnaire + S3 Parquet pour l’archive", "Network policy ou PrivateLink"],
  ], [20, 30, 28, 22]),
  p("**Pourquoi pas de GPU** : le modèle retenu (gradient boosting sur 7 features tabulaires) s’entraîne et s’infère efficacement sur CPU. Un GPU ne serait justifié que pour un modèle séquentiel (LSTM, transformeur sur l’historique client), identifié comme évolution."),
  p("**PoC local** : 8 conteneurs par défaut (plus les profils `flink` et `airflow`) sur un Mac ARM64, dimensionné pour 5 à 10 transactions par seconde, 200 marchands et 5 000 clients (`seed/seed_data.py`)."),

  // 3
  h1("3. Concevoir une architecture Data & IA évolutive et industrialisable"),
  note("**Compétence C2** — concevoir l’architecture logique et physique.", "info"),
  h2("Architecture logique"),
  p("Architecture polyglotte : PostgreSQL pour l’ACID, MongoDB pour les documents évolutifs, Redis pour les features en milliseconde, un schéma en étoile pour l’analytique. Kafka, alimenté par CDC Debezium, relie les systèmes sans que la base transactionnelle n’appelle quiconque."),
  ...image(img("stripe_architecture_globale.png"), "Architecture logique implémentée (presentation/stripe_architecture_globale.drawio)"),
  h2("Architecture physique (cible)"),
  p("VPC sur trois zones de disponibilité, trois niveaux de sous-réseaux (public pour les NAT, app pour les services, data sans route Internet), services régionaux chiffrés par une clé KMS unique, MongoDB Atlas joint en PrivateLink."),
  ...image(img("stripe_aws_cible.png"), "Architecture physique de la cible AWS, fidèle à terraform/envs/prod"),
  h2("Évolutivité et industrialisation"),
  bullets([
    "**Découplage** : chaque consommateur Kafka évolue indépendamment ; ajouter un consommateur (moteur de recherche, webhooks) ne touche pas PostgreSQL.",
    "**Montée en charge horizontale** : autant de scorers que de partitions ; `desired_count` Fargate paramétré par environnement.",
    "**Même image partout** : le code applicatif lit les mêmes variables d’environnement en local et en cloud (`PG_HOST`, `KAFKA_BROKERS`…).",
    "**Trace des décisions** : `model_version` sur chaque score permet de faire cohabiter et comparer plusieurs modèles.",
  ]),

  // 4
  h1("4. Arbitrer Cloud / On-Premise / Hybride et IaaS / PaaS / Serverless"),
  note("**Compétence C3** — arbitrer les modèles de déploiement et de service.", "info"),
  p("Choix retenu : **cloud public, PaaS dominant, conteneurs serverless (Fargate)**. Les critères : conformité (PCI-DSS, RGPD), charge d’exploitation, élasticité, coût total, réversibilité."),
  ...table(["Option", "Avantages", "Limites", "Décision pour Stripe"], [
    ["On-premise", "Contrôle total, pas de dépendance fournisseur", "Investissement initial lourd, montée en charge lente, astreinte matérielle", "Écarté"],
    ["Cloud public", "Élasticité, services managés certifiés PCI-DSS", "Dépendance fournisseur, coûts à piloter", "Retenu comme socle"],
    ["Hybride", "Résidence de données, migration progressive", "Complexité réseau et opérationnelle", "Non retenu : la région UE suffit au RGPD"],
    ["IaaS (EC2)", "Liberté de configuration", "Patching, haute dispo à construire soi-même", "Aucun composant"],
    ["PaaS", "Haute disponibilité et sauvegardes intégrées", "Moins de réglages fins", "RDS, MSK, ElastiCache, Atlas, MWAA"],
    ["Serverless", "Aucun serveur à gérer, facturation à l’usage", "Démarrage à froid, limites de durée", "Fargate pour les services ; EventBridge envisagé à la place de MWAA (levier FinOps)"],
  ], [14, 28, 30, 28]),
  p("**Pourquoi Fargate plutôt que Lambda ou EKS** : le scorer est un consommateur Kafka qui tourne en continu, ce qui écarte Lambda (durée limitée, reconnexions). EKS apporterait une charge d’exploitation (plan de contrôle, mises à jour) injustifiée pour quatre services."),
  p("**Pourquoi Atlas plutôt que DocumentDB** : compatibilité MongoDB 7 complète (index TTL, `$percentile` utilisé dans les requêtes) et PrivateLink natif."),

  // 5
  h1("5. Industrialiser via l’Infrastructure as Code (Terraform)"),
  note("**Compétence C4** — industrialiser via l’Infrastructure as Code. **Preuve : dossier `terraform/`, validé à chaque push par le job CI `terraform`.**", "ok"),
  h2("Organisation du code"),
  ...table(["Élément", "Contenu"], [
    ["`terraform/bootstrap`", "Bucket S3 du state : versionné, chiffré, public bloqué, `prevent_destroy`"],
    ["`terraform/modules/*`", "10 modules : network, security, rds, msk, elasticache, mongodb_atlas, storage, compute, airflow (+ versions des providers par module)"],
    ["`terraform/stack`", "Composition des modules, alarmes CloudWatch, SNS, budget"],
    ["`terraform/envs/dev`, `envs/prod`", "Dimensionnement, backend S3 à clé distincte, `default_tags` de coût"],
    ["Verrouillage du state", "Fichier `.tflock` natif (Terraform ≥ 1.10), sans table DynamoDB"],
    ["Secrets", "Aucun dans le code : clés Atlas par variables d’environnement, mot de passe RDS géré par RDS, valeurs générées par `random_password` stockées dans Secrets Manager"],
  ], [30, 70]),
  h2("Extrait réel — module rds"),
  ...code(`resource "aws_db_parameter_group" "this" {
  name   = "\${var.name}-pg16"
  family = "postgres16"

  # CDC Debezium (plugin pgoutput)
  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }
  # Refuse toute connexion non TLS (PCI-DSS req. 4)
  parameter {
    name  = "rds.force_ssl"
    value = "1"
  }
}

resource "aws_db_instance" "primary" {
  engine                        = "postgres"
  instance_class                = var.instance_class
  manage_master_user_password   = true          # secret géré et tourné par RDS
  master_user_secret_kms_key_id = var.kms_key_arn
  storage_encrypted             = true
  multi_az                      = var.multi_az
  publicly_accessible           = false
  deletion_protection           = var.deletion_protection
}`),
  h2("Validation et limites"),
  bullets([
    "`make tf-validate` et le job CI exécutent `terraform fmt -check`, puis `init -backend=false` et `validate` sur bootstrap, dev et prod.",
    "**Non appliqué** : un `plan` réel demande un compte AWS et Atlas. Étape suivante : fédération OIDC GitHub → AWS, `plan` commenté sur chaque pull request, `apply` après approbation.",
    "Le `docker-compose.yml` reste l’IaC du PoC : déclaratif, reproductible, et relancé à l’identique en CI.",
  ]),

  // 6
  h1("6. Structurer les environnements (Data Lake, bases vectorielles, conteneurs)"),
  note("**Compétence C5** — structurer les environnements de données et d’exécution.", "info"),
  h2("Data Lake"),
  bullets([
    "**Codé** : bucket data lake S3 (`terraform/modules/storage`) chiffré SSE-KMS, versionné, TLS obligatoire, cycle de vie : préfixe `archive/` en Standard-IA à 90 jours puis Glacier Instant Retrieval à 365 jours.",
    "**Conçu** : zones `raw/` (bronze), `cleansed/` (silver), `curated/` (gold) en Parquet et tables Iceberg cataloguées par AWS Glue ; archivage des faits de plus de deux ans.",
  ]),
  h2("Bases vectorielles"),
  p("**Conçu, non implémenté.** Cas d’usage identifiés : similarité entre transactions (fraude par analogie), recherche sémantique sur les contestations clients (`customer_feedback`), RAG sur la base de connaissances support. Choix envisagé : **pgvector** sur RDS (index HNSW), pour éviter une base de plus tant que les volumes restent modérés ; OpenSearch k-NN au-delà de quelques dizaines de millions de vecteurs."),
  h2("Conteneurs et environnements"),
  bullets([
    "**Exécuté** : 8 services Docker Compose avec healthchecks, plus les profils optionnels `flink` et `airflow` ; images dédiées pour le dashboard, `ml-monitor` et PyFlink.",
    "**Codé** : ECR (tags immuables, scan CVE à chaque push), ECS Fargate ARM64, un groupe de logs par service.",
    "**Environnements** : dev et prod séparés par VPC (10.20.0.0/16 et 10.30.0.0/16), state Terraform et tags ; `.env` local jamais versionné.",
  ]),

  // 7
  h1("7. Garantir la résilience et la haute disponibilité"),
  note("**Compétence C6** — résilience, scalabilité, reprise.", "info"),
  ...table(["Composant", "PoC — exécuté", "Cible — codée"], [
    ["PostgreSQL", "Clé d’idempotence ; write-back idempotent et atomique", "Multi-AZ synchrone, réplica, sauvegardes 35 j, protection contre la suppression"],
    ["Kafka", "1 broker, facteur de réplication 1 (écart assumé)", "3 brokers / 3 AZ, RF 3, `min.insync.replicas=2`, `unclean.leader.election=false`"],
    ["Scorer", "DLQ, reconnexion limitée, arrêt propre, repli sur règles, reconstruction de la vélocité après perte de Redis", "Plusieurs tâches Fargate, déploiement progressif avec retour arrière automatique"],
    ["Redis", "Clés à TTL, reconstructibles depuis PostgreSQL", "Primaire + réplica, bascule automatique"],
    ["MongoDB", "Écritures en échec envoyées en DLQ", "Replica set Atlas 3 nœuds, restauration à un instant donné"],
    ["Supervision", "`ml-monitor` : dérive et précision servie", "Alarmes CloudWatch (CPU RDS, retard du scorer) vers SNS"],
  ], [16, 42, 42]),
  h2("Incidents réels traités"),
  bullets([
    "**Double scoring par écho CDC** : la mise à jour du score repassait dans Kafka et était re-scorée ; corrigé par la condition `fraud_score IS NULL` (précision servie passée de 29 % à 97 %).",
    "**Reset de Redis** : tous les clients paraissaient nouveaux, d’où une salve de faux positifs ; corrigé par la reconstruction de la vélocité depuis PostgreSQL.",
    "**Réentraînement automatique** : précision offline tombée à 0,77 alors que la précision servie restait à 0,98 ; conclusion : juger un modèle sur les deux mesures. Post-mortems dans `docs/MLOPS.md`.",
  ]),
  p("**Conçu** : reprise après sinistre inter-régions (réplica RDS dans une seconde région, MirrorMaker 2 pour Kafka), objectifs RPO de 5 minutes et RTO d’une heure."),

  // 8
  h1("8. Mettre en place la sécurité"),
  note("**Compétence C7** — IAM, chiffrement, gestion des secrets.", "info"),
  h2("IAM et contrôle d’accès"),
  bullets([
    "**Exécuté** : `replication_user` (lecture seule, pour Debezium), `analytics_reader` (lecture sans la colonne `fingerprint`, accordée colonne par colonne), utilisateur MongoDB `stripe_app` limité à `readWrite` sur `stripe_nosql`, dashboard protégé par login (hash SHA-256, blocage après échecs).",
    "**Codé** : rôle d’exécution ECS (images, logs, secrets) distinct du rôle de tâche (topics `stripe.*`, bucket data lake, clé KMS) ; security group par base n’acceptant que le groupe applicatif.",
  ]),
  h2("Chiffrement"),
  bullets([
    "**Codé** : une CMK KMS à rotation annuelle pour RDS, MSK, ElastiCache, S3, CloudWatch, ECR et Secrets Manager ; TLS forcé sur RDS, MSK, Redis et S3.",
    "**Exécuté** : aucun numéro de carte stocké, empreinte pseudonymisée ; `pgcrypto` activé.",
    "**Écart assumé du PoC** : TLS désactivé entre conteneurs locaux, documenté dans `docs/SECURITY_COMPLIANCE_PLAN.md`.",
  ]),
  h2("Gestion des secrets"),
  bullets([
    "**Exécuté** : `scripts/init_env.sh` génère des mots de passe aléatoires dans `.env`, exclu de Git ; `.env.example` sans valeur.",
    "**Codé** : Secrets Manager, injection des secrets dans les tâches ECS au démarrage (jamais dans la définition de tâche), mot de passe maître RDS géré et tourné par RDS, Kafka en IAM.",
  ]),
  h2("Audit et conformité"),
  bullets([
    "RGPD : `anonymize_customer()` (`init/postgres/02_rgpd.sql`), TTL MongoDB de 90 et 30 jours, hébergement en `eu-west-1`.",
    "Traçabilité : `fraud_indicators` conserve chaque décision de blocage avec la version du modèle ; VPC Flow Logs 365 jours en cible.",
  ]),

  // 9
  h1("9. Piloter l’efficience (FinOps / GreenOps)"),
  note("**Compétence C8** — piloter les coûts et l’empreinte. **Preuve : `docs/FINOPS.md` et budgets dans `terraform/stack/main.tf`.**", "ok"),
  h2("Chiffrage mensuel (prix publics à la demande, eu-west-1)"),
  ...table(["Poste", "Prod", "Dev"], [
    ["RDS PostgreSQL", "~620 $", "~65 $"],
    ["MSK", "~610 $", "~265 $"],
    ["ElastiCache", "~350 $", "~25 $"],
    ["MongoDB Atlas", "~450 $", "~60 $"],
    ["MWAA", "~380 $", "~380 $"],
    ["ECS Fargate", "~145 $", "~90 $"],
    ["Réseau (NAT, PrivateLink, inter-AZ)", "~180 $", "inclus ci-dessous"],
    ["Observabilité, S3, KMS, secrets, Snowflake", "~365 $", "~120 $"],
    ["**Total**", "**~3 100 $**", "**~1 000 $**"],
    ["Budget Terraform (alertes 80 % réel, 100 % prévisionnel)", "4 000 $", "1 200 $"],
  ], [56, 22, 22]),
  h2("Leviers chiffrés"),
  ...table(["Levier", "Économie", "Contrepartie"], [
    ["Remplacer MWAA par EventBridge + tâche ECS (un seul DAG)", "~350 $/mois par environnement", "Perte de l’interface et des reprises Airflow"],
    ["Engagements 1 an (RDS, ElastiCache, Savings Plan Fargate)", "~330 $/mois en prod", "Engagement ferme"],
    ["Arrêt du dev la nuit et le week-end", "~200 $/mois", "Environnement indisponible hors heures"],
    ["Volumes MSK de dev à 100 Go", "~130 $/mois", "Rétention plus courte"],
    ["Snowflake : auto-suspend 60 s + resource monitor", "Évite une dérive (~1 900 $/mois pour un warehouse oublié)", "Premier appel plus lent"],
  ], [44, 30, 26]),
  p("Avec les trois premiers leviers : environ 2 400 $/mois en prod et 450 $ en dev."),
  h2("GreenOps"),
  bullets([
    "Graviton (ARM64) pour RDS, MSK, ElastiCache et Fargate : moins d’énergie à performance égale, et même architecture que le poste de développement.",
    "Pas de ressource inactive : arrêt nocturne en dev, auto-suspend Snowflake.",
    "Données froides en classes de stockage à faible empreinte ; TTL MongoDB plutôt que conservation indéfinie.",
    "Région : `eu-west-1` pour la latence et la disponibilité des services ; `eu-north-1`, moins carbonée, à évaluer. Mesure via AWS Customer Carbon Footprint Tool.",
    "Le PoC sur une seule machine est lui-même une démarche sobre : prouver l’architecture avant d’engager de l’infrastructure.",
  ]),

  // 10
  h1("10. Documenter et coordonner les équipes techniques"),
  note("**Compétence C9** — documenter et coordonner.", "info"),
  h2("Documentation produite (toute dans le dépôt)"),
  ...table(["Document", "Public", "Contenu"], [
    ["`README.md`", "Tout nouvel arrivant", "Démarrage en une commande, démo, commandes utiles, Terraform, requêtes, Snowflake payant"],
    ["`docs/ARCHITECTURE.md`", "Développeurs", "Documentation technique complète, fichier par fichier"],
    ["`docs/FILE_FUNCTION_INDEX.md`", "Développeurs", "Chaque fichier, ses fonctions et la source des données qu’il lit"],
    ["`docs/PRESENTATION.md`", "Jury, décideurs", "Justification des choix, métriques"],
    ["`docs/SECURITY_COMPLIANCE_PLAN.md`", "Sécurité, DPO", "PCI-DSS, RGPD, écarts assumés, traduction en Terraform"],
    ["`docs/ML_INTEGRATION_STRATEGY.md`, `docs/MLOPS.md`", "Data scientists, MLOps", "Stratégie, cycle de vie, CI, post-mortems"],
    ["`docs/FINOPS.md`", "Direction, FinOps", "Chiffrage et leviers"],
    ["`docs/OLAP_SCHEMA_DESIGN.md`, `docs/NOSQL_DATA_MODEL.md`", "Data engineers", "Modèles de données justifiés"],
    ["`terraform/README.md`", "Ops, SRE", "Correspondance PoC → cible, validation, déploiement"],
    ["Diagrammes `presentation/*.drawio`", "Tous", "Architecture, ERD, MongoDB, structure du code, cible AWS"],
    ["`notebooks/audit_data_ml.ipynb`", "Data, qualité", "Audit exécutable des données et du modèle"],
  ], [34, 20, 46]),
  h2("Coordination"),
  bullets([
    "**Automatisation partagée** : le Makefile est l’unique point d’entrée (`make init`, `make test`, `make queries-check`, `make tf-validate`) ; la CI exécute exactement les mêmes commandes.",
    "**Garde-fous** : tests de bout en bout, requêtes et Terraform vérifiés à chaque push ; une régression bloque la branche.",
    "**Rôles et accès** : data engineer (pipeline, `replication_user`), analyste (`analytics_reader`), ML engineer (MLflow, `ml-monitor`), SRE (Terraform, alarmes), DPO (plan RGPD).",
    "**Limite honnête** : projet mené seul ; le fonctionnement en équipe (revue de pull request obligatoire, protection de branche) est décrit mais n’a pas été pratiqué à plusieurs.",
  ]),

  // 11
  h1("11. Conclusion"),
  ...table(["Compétence", "Niveau de preuve", "Preuve principale"], [
    ["C1 Spécifications", "Codé et validé", "`terraform/envs/prod`, section 2"],
    ["C2 Architecture logique et physique", "Exécuté (logique) / codé (physique)", "Diagrammes, stack Docker, `terraform/`"],
    ["C3 Arbitrage Cloud / PaaS / serverless", "Conçu et justifié", "Section 4"],
    ["C4 Infrastructure as Code", "Codé et validé en CI", "`terraform/`, job CI `terraform`"],
    ["C5 Environnements", "Exécuté (conteneurs) / codé (data lake) / conçu (vectoriel)", "`docker-compose.yml`, `modules/storage`"],
    ["C6 Résilience", "Exécuté (PoC) / codé (cible)", "DLQ, idempotence, post-mortems, modules Multi-AZ"],
    ["C7 Sécurité", "Exécuté (rôles, secrets, login) / codé (KMS, IAM)", "`init/postgres`, `modules/security`"],
    ["C8 FinOps / GreenOps", "Chiffré et codé (budgets)", "`docs/FINOPS.md`"],
    ["C9 Documentation et coordination", "Exécuté", "`docs/`, Makefile, CI"],
  ], [30, 34, 36]),
  p("Le PoC prouve, à coût quasi nul, que la chaîne complète fonctionne : transaction, capture de changement, scoring surveillé, persistance et restitution. Le passage en production ne change pas la logique : il remplace des conteneurs par des services managés, décrits en Terraform et chiffrés en FinOps. Les écarts restants sont identifiés : Terraform non appliqué, Snowflake non connecté, bases vectorielles à construire."),
];

const doc = makeDoc({ headerLeft: "Stripe — Bloc 2 · Infrastructure Data & IA", footerText: "Patrice Duclos — AIA RNCP41993", children });
Packer.toBuffer(doc).then((b) => { fs.writeFileSync(OUT, b); console.log("written", OUT); });
