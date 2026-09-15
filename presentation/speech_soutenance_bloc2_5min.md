# Speech de soutenance — Bloc 2 Stripe (5 minutes)

**Rythme visé : environ 130 mots par minute.** Le texte compte environ 600 mots, soit 4 min 30 à débit calme : il reste 30 secondes pour respirer et changer de slide.
Les minutages sont cumulés. Les passages entre crochets sont des indications pour vous : ne les lisez pas à voix haute.

---

### Slide 1 — Titre · 0:00 → 0:10

Bonjour. Je suis Patrice Duclos. Je vous présente mon projet du bloc 2 : concevoir l'infrastructure de données de Stripe, une fintech qui traite des millions de paiements par jour.

### Slide 2 — Contexte · 0:10 → 0:40

Stripe a trois besoins contradictoires.
Un : des paiements ACID en moins de 50 millisecondes.
Deux : analyser tout l'historique sans ralentir la production.
Trois : respecter PCI-DSS et le RGPD.
Une seule base ne peut pas tout faire. J'ai donc choisi une **architecture polyglotte**.

### Slide 3 — Le bon outil pour chaque besoin · 0:40 → 1:05

Chaque besoin a son outil.
**PostgreSQL** pour les transactions : c'est la source de vérité.
**Snowflake** pour l'analytique.
**MongoDB** pour le semi-structuré : logs, sessions, features ML.
Et au milieu, **Kafka** relie tout, avec **Redis** comme mémoire rapide pour le scoring.

### Slide 4 — Diagramme d'architecture · 1:05 → 1:40

Voici l'architecture qui tourne réellement : conteneurisée avec Docker Compose, lancée en une commande, versionnée sur GitHub.
À gauche, l'OLTP et le batch. Au centre, le streaming. À droite, le NoSQL et le machine learning.
Un point de transparence : sans compte Snowflake, le chargement OLAP tourne en simulation. Le schéma et le code sont prêts.
*[Si vous créez un compte d'essai Snowflake d'ici le jury, dites plutôt : « Le chargement Snowflake est branché sur un compte d'essai. »]*

### Slide 5 — Le parcours de la donnée · 1:40 → 2:15

La donnée suit deux canaux.
**En temps réel** : la transaction est écrite dans PostgreSQL. Debezium lit le journal WAL et publie le changement dans Kafka. Le job de scoring lit la vélocité du client dans Redis, calcule un score, puis écrit la décision dans PostgreSQL et MongoDB. Aucun trigger, aucun polling : la production n'est pas ralentie.
**En batch** : chaque nuit, un DAG Airflow charge les données de la veille dans le modèle en étoile. Le chargement est idempotent : on peut le rejouer sans doublons.

### Slides 6 et 7 — Modèle OLTP et ERD · 2:15 → 2:45

L'OLTP est en troisième forme normale, avec six tables.
*[Passez à l'ERD.]*
Trois choix clés. Une clé d'idempotence empêche de débiter deux fois un paiement. Les index servent la requête la plus fréquente : les dernières transactions d'un marchand. Et, pour PCI-DSS, aucun numéro de carte n'est stocké, seulement une empreinte SHA-256.

### Slide 8 — Modèle OLAP · 2:45 → 3:10

Pour l'analytique, un schéma en étoile : une table de faits et cinq dimensions, dont la date, le marchand et la géographie. Elle est clusterisée par date et par marchand.
Les agrégats fréquents sont pré-calculés en vues matérialisées, rafraîchies par Airflow. À l'échelle de Stripe, ce seraient des Dynamic Tables Snowflake.

### Slide 9 — Modèle NoSQL · 3:10 → 3:30

MongoDB reçoit ce qui n'a pas de schéma fixe : logs, alertes, clickstream, features ML.
J'**embarque** ce qui est lu ensemble et je **référence** les données PostgreSQL par UUID.
Des index TTL purgent les logs après 90 jours : le RGPD est appliqué par la base elle-même.

### Slide 10 — Détection de fraude en temps réel · 3:30 → 4:10

Le cœur du projet : la fraude en temps réel, avec un objectif de décision sous 100 millisecondes.
Deux moteurs : des règles, et un modèle **XGBoost**. Si le modèle manque, on revient automatiquement aux règles.
Au-delà de 0,6, revue. Au-delà de 0,85, blocage.
En production locale, sur le trafic réellement scoré, le modèle bloque **96 % des fraudes** avec **93 % de précision**. Chaque décision de blocage laisse une trace dans la table `fraud_indicators`, écrite dans la même transaction que le score.
Chaque entraînement est tracé dans **MLflow**. **Evidently** surveille la dérive et la performance, et relance un entraînement si la qualité baisse.

### Slide 11 — Du PoC local à la cible cloud · 4:10 → 4:45

Pour tenir le budget, j'ai construit un PoC local, mais complet.
La cible cloud garde la même logique ; seuls les composants changent : RDS Multi-AZ, MSK, ElastiCache, Atlas en PrivateLink, ECS Fargate et MWAA.
Elle est écrite en **Terraform** dans le dépôt : dix modules, validés à chaque push par la CI, mais jamais appliqués faute de compte AWS. Sécurité par KMS, IAM et Secrets Manager ; FinOps chiffré à environ 3 100 dollars par mois en prod, avec un budget et des alertes dans le code.

### Slide 12 — En résumé · 4:45 → 5:00

En résumé : chaque système est à sa place, le CDC découple tout sans ralentir les paiements, la fraude est scorée en temps réel par un modèle surveillé, et ce PoC est prêt pour le cloud.
Merci. Je suis prêt pour la démonstration et vos questions.

---

## Questions probables du jury — réponses courtes

| Question | Réponse en une ou deux phrases |
|---|---|
| **Pourquoi un job « Flink-like » en Python et pas Flink ?** | L'image PyFlink ne se construit pas sur Mac ARM64 (numpy 1.21, en-têtes JDK, `ClassCastException`). Le job Python reproduit la même logique DataStream : source Kafka, enrichissement Redis, scoring, sink Kafka. La version PyFlink est dans `flink/fraud_scoring_job.py`, prête pour Managed Flink. |
| **Le write-back dans PostgreSQL ne crée-t-il pas une boucle CDC ?** | Si : l'UPDATE revient dans Kafka. Mais la condition `WHERE fraud_score IS NULL` rend l'écriture idempotente, donc le second passage met à jour 0 ligne. |
| **Que devient un message invalide ?** | Il part dans la dead-letter queue `stripe.etl.dead-letter`, avec le topic, l'offset et l'erreur. On ne perd pas le message et le pipeline continue. |
| **Comment gérez-vous le droit à l'effacement ?** | La fonction PL/pgSQL `anonymize_customer()` anonymise le client sans casser les clés étrangères. Côté Mongo, les TTL purgent les logs à 90 jours et les interactions à 30 jours. |
| **Pourquoi un schéma en étoile plutôt qu'en flocon ?** | Il demande moins de jointures, les requêtes BI sont plus simples, et le stockage colonne de Snowflake rend la dénormalisation peu coûteuse. |
| **Votre rappel est de 96 %, mais avec quelle proportion de fraude ?** | Environ 25 % dans le trafic scoré, alors que le générateur tire 5 % des transactions en fraude : chaque fraude déclenche une rafale de transactions sur le même client. Avec un taux réel de 0,1 à 0,5 %, la précision baisserait à rappel égal ; c'est pourquoi je mesure la précision servie en continu. |
| **Quelle est la haute disponibilité en local ?** | Aucune : le PoC tourne avec un seul broker et un facteur de réplication de 1, par choix. La cible utilise MSK avec un facteur de réplication de 3 et `min.insync.replicas=2`, RDS Multi-AZ avec un failover de moins de 30 s, et un replica set Atlas. |
| **Combien coûterait la cible ?** | Environ 3 100 $/mois en prod et 1 000 $ en dev, en prix à la demande (`docs/FINOPS.md`). Les quatre bases managées pèsent 65 %. Remplacer MWAA par EventBridge et prendre des engagements 1 an ramène la prod vers 2 400 $. |
| **Que se passe-t-il après un réentraînement ?** | Le modèle est enregistré dans MLflow et le scorer le recharge dès que le fichier change (contrôle de date de modification dans `ml/scoring.py`). Le 15/09, un réentraînement automatique a fait chuter la précision offline à 0,77 alors que la précision servie restait à 0,98 : je juge donc un modèle sur les deux mesures, pas sur l'offline seule. |
| **Votre Terraform a-t-il été testé ?** | Validé par `terraform validate` en CI sur dev, prod et le bootstrap du state, mais jamais appliqué : il faudrait un compte AWS et Atlas. Je l'assume comme une limite du PoC. |
| **Où sont vos requêtes ?** | Dans `queries/` : SQL PostgreSQL et MongoDB exécutées sur la stack par `make queries-check` à chaque push ; les requêtes Snowflake sont alignées sur le schéma mais non exécutées. |
