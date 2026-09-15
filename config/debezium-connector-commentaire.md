# Explication du connecteur Debezium

Le fichier `debezium-connector.json` est un JSON de configuration pur. On ne peut pas y mettre des commentaires directement sans le rendre invalide pour Debezium / Kafka Connect. Cette fiche sert donc de version commentée, ligne par ligne.

## Rôle du fichier

Ce fichier décrit le connecteur Debezium PostgreSQL qui lit les changements de la base `stripe_oltp` et les publie dans Kafka. Il sert à transformer les `INSERT`, `UPDATE` et `DELETE` de PostgreSQL en événements de changement de données, pour alimenter le reste du pipeline temps réel.

## Les gros sujets à comprendre

Cette configuration se lit plus facilement si on la découpe en 5 blocs :

- La connexion à PostgreSQL, avec l'hôte, le port, l'utilisateur et la base source.
- Le mécanisme CDC Debezium, qui suit les changements via un slot de réplication et une publication.
- Le filtrage des tables, pour ne capter que les données métier utiles au projet.
- La transformation `unwrap`, qui simplifie le format Debezium avant l'envoi vers Kafka.
- Les paramètres de sortie Kafka, qui rendent les messages plus simples à consommer côté scripts Python.

En pratique, le connecteur fait donc 3 choses : il lit PostgreSQL, il transforme les événements, puis il les publie dans Kafka.

## Lecture ligne par ligne

| Ligne | Contenu | Rôle |
|---|---|---|
| 1 | `{` | Ouvre l'objet JSON principal. |
| 2 | `"name": "stripe-postgres-cdc-v2",` | Donne le nom du connecteur Debezium dans Kafka Connect (le script `deploy_debezium.sh` impose ce même nom). |
| 3 | `"config": {` | Ouvre l'objet contenant tous les paramètres du connecteur. |
| 4 | `"connector.class": "io.debezium.connector.postgresql.PostgresConnector",` | Indique qu'on utilise le connecteur Debezium pour PostgreSQL. |
| 5 | `"database.hostname": "postgres",` | Définit le nom d'hôte du serveur PostgreSQL à surveiller. |
| 6 | `"database.port": "5432",` | Définit le port PostgreSQL. |
| 7 | `"database.user": "replication_user",` | Indique l'utilisateur utilisé pour la réplication logique. |
| 8 | `"database.password": "REPLACE_ME",` | Mot de passe du compte de réplication. Cette valeur doit être remplacée avant usage. |
| 9 | `"database.dbname": "stripe_oltp",` | Nom de la base source à surveiller. |
| 10 | `"database.server.name": "stripe",` | Préfixe logique utilisé par Debezium pour nommer les sujets Kafka générés. |
| 11 | `"plugin.name": "pgoutput",` | Choisit le plugin de réplication PostgreSQL `pgoutput`. |
| 12 | `"publication.name": "stripe_publication",` | Nom de la publication PostgreSQL lue par Debezium. |
| 13 | `"slot.name": "stripe_debezium_slot",` | Nom du slot de réplication logique utilisé pour suivre les changements. |
| 14 | `"table.include.list": "public.transactions,public.refunds,public.fraud_indicators",` | Limite la capture aux tables listées, pour ne pas capter toute la base. |
| 15 | `"transforms": "unwrap",` | Active une transformation Kafka Connect nommée `unwrap`. |
| 16 | `"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",` | Remplace le format Debezium complet par l'état de ligne final plus simple à consommer. |
| 17 | `"transforms.unwrap.drop.tombstones": "false",` | Conserve les tombstones Kafka, utiles pour certaines suppressions et nettoyages. |
| 18 | `"transforms.unwrap.delete.handling.mode": "rewrite",` | Réécrit les suppressions dans un format exploitable au lieu de les supprimer brutalement. |
| 19 | `"key.converter": "org.apache.kafka.connect.json.JsonConverter",` | Définit le convertisseur JSON pour la clé des messages Kafka Connect. |
| 20 | `"key.converter.schemas.enable": "false",` | Désactive le schéma dans la clé pour simplifier le payload. |
| 21 | `"value.converter": "org.apache.kafka.connect.json.JsonConverter",` | Définit le convertisseur JSON pour la valeur des messages. |
| 22 | `"value.converter.schemas.enable": "false",` | Désactive le schéma dans la valeur pour obtenir des messages plus simples. |
| 23 | `"topic.prefix": "stripe",` | Préfixe des topics Kafka créés par Debezium. |
| 24 | `"snapshot.mode": "initial",` | Demande une capture initiale des données existantes au démarrage du connecteur. |
| 25 | `"decimal.handling.mode": "double",` | Convertit les décimaux en `double` pour faciliter la consommation côté applications. |
| 26 | `"time.precision.mode": "connect"` | Conserve une précision de temps compatible Kafka Connect. |
| 27 | `}` | Ferme l'objet `config`. |
| 28 | `}` | Ferme l'objet JSON principal. |

## Résumé rapide

- Ce fichier démarre un CDC PostgreSQL vers Kafka.
- Il cible uniquement les tables métier utiles au projet.
- Il aplatit les événements Debezium pour les rendre plus simples à consommer.
- Il démarre par un instantané initial puis suit les changements en continu.

## Comment le lire rapidement

Si tu veux aller à l'essentiel, retiens ce chemin de lecture :

1. Les lignes 4 à 13 définissent la source PostgreSQL.
2. Les lignes 14 à 18 définissent ce qu'on capture et comment on réécrit les événements.
3. Les lignes 19 à 26 définissent le format des messages envoyés à Kafka.
4. Les lignes 24 à 26 disent si on prend un instantané de départ et comment on encode les types.

Si tu veux, je peux aussi te faire une version directement intégrée dans un fichier `.jsonc` commenté, ou t'expliquer chaque option en mode très simple.