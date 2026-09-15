#!/usr/bin/env python3
"""Génère presentation/stripe_aws_cible.drawio — architecture physique de la cible
AWS, fidèle au code terraform/ (envs/prod)."""
import sys
from xml.sax.saxutils import escape

OUT = sys.argv[1]
cells = []
_id = [10]


def nid():
    _id[0] += 1
    return f"c{_id[0]}"


def box(x, y, w, h, label, fill="#ffffff", stroke="#666666", font=11, bold=False,
        dashed=False, align="center", valign="middle", rounded=True, fcolor="#1a1f36", parent="1"):
    i = nid()
    style = (f"rounded={1 if rounded else 0};whiteSpace=wrap;html=1;fillColor={fill};strokeColor={stroke};"
             f"fontSize={font};fontColor={fcolor};align={align};verticalAlign={valign};arcSize=6;"
             f"{'fontStyle=1;' if bold else ''}{'dashed=1;' if dashed else ''}spacingLeft=6;spacingRight=6;")
    cells.append(f'<mxCell id="{i}" value="{escape(label.replace(chr(10), '<br>'), {chr(34): "&quot;"})}" style="{style}" vertex="1" parent="{parent}">'
                 f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/></mxCell>')
    return i


def edge(src, dst, label="", color="#555555", dashed=False, exit_=None, entry=None):
    i = nid()
    style = f"edgeStyle=orthogonalEdgeStyle;rounded=1;html=1;strokeColor={color};fontSize=10;fontColor={color};endArrow=block;endFill=1;labelBackgroundColor=#ffffff;"
    if dashed:
        style += "dashed=1;"
    if exit_:
        style += f"exitX={exit_[0]};exitY={exit_[1]};exitDx=0;exitDy=0;"
    if entry:
        style += f"entryX={entry[0]};entryY={entry[1]};entryDx=0;entryDy=0;"
    cells.append(f'<mxCell id="{i}" value="{escape(label)}" style="{style}" edge="1" parent="1" source="{src}" target="{dst}">'
                 f'<mxGeometry relative="1" as="geometry"/></mxCell>')


W = 1640
# Titre
box(20, 10, W - 40, 40, "Stripe Polyglot — Architecture physique de la cible AWS (terraform/envs/prod)",
    fill="#ffffff", stroke="#ffffff", font=18, bold=True)
box(20, 50, W - 40, 26,
    "Code validé en CI (terraform validate), jamais appliqué : aucun compte AWS. Chaque boîte indique le module Terraform qui la crée.",
    fill="#ffffff", stroke="#ffffff", font=11, fcolor="#555555")

# Hors AWS (gauche)
users = box(20, 165, 170, 44, "Marchands / API de paiement", fill="#f5f5f5", stroke="#999999", bold=True)
gha = box(20, 560, 170, 90, "GitHub Actions (hors AWS)\nlint · e2e · terraform validate\n→ pousse l'image dans ECR", fill="#f5f5f5", stroke="#999999")
snow = box(20, 760, 170, 90, "Snowflake (OLAP)\nhors Terraform, chargé par\nle DAG MWAA (dry-run PoC)", fill="#fff8e1", stroke="#c9a227", dashed=True)

# Région AWS
region = box(220, 100, 1150, 900, "", fill="#fbfbff", stroke="#232f3e", rounded=False)
box(230, 104, 600, 24, "AWS — région eu-west-1 (Irlande, données UE)", fill="#fbfbff", stroke="#fbfbff",
    font=13, bold=True, align="left")

vpc = box(240, 135, 900, 850, "", fill="#f3f7fb", stroke="#3b7dd8", rounded=False)
box(250, 139, 700, 22, "VPC 10.30.0.0/16 — modules/network (Flow Logs 365 j, endpoint S3)", fill="#f3f7fb",
    stroke="#f3f7fb", font=12, bold=True, align="left", fcolor="#2a5ea8")

igw = box(560, 170, 260, 34, "Internet Gateway", fill="#ffffff", stroke="#3b7dd8")

azs = ["eu-west-1a", "eu-west-1b", "eu-west-1c"]
colx = [255, 550, 845]
colw = 280
tiers = [
    ("Sous-réseau public /20", "#e8f5e9", "#43a047", 230, 95),
    ("Sous-réseau app /20 (privé)", "#e3f2fd", "#1e88e5", 345, 215),
    ("Sous-réseau data /20 (privé, sans route Internet)", "#fce4ec", "#d81b60", 580, 390),
]
nat = []
app_nodes = []
data_nodes = {}
for k, az in enumerate(azs):
    x = colx[k]
    box(x, 212, colw, 18, f"AZ {az}", fill="#f3f7fb", stroke="#f3f7fb", font=11, bold=True, fcolor="#2a5ea8")
    for (tname, fill, stroke, ty, th) in tiers:
        box(x, ty, colw, th, "", fill=fill, stroke=stroke, dashed=True, rounded=False)
        box(x + 4, ty + 2, colw - 8, 18, tname, fill=fill, stroke=fill, font=10, fcolor=stroke, align="left")
    nat.append(box(x + 60, 262, 160, 46, "NAT Gateway\n(1 par AZ en prod)", fill="#ffffff", stroke="#43a047", font=10))
    # App tier
    scorer = box(x + 15, 370, 120, 52, "ECS · scorer\n(flink_like_job)", fill="#ffffff", stroke="#1e88e5", font=10)
    app_nodes.append(scorer)
    if k == 0:
        box(x + 145, 370, 120, 52, "ECS · mongo-writer", fill="#ffffff", stroke="#1e88e5", font=10)
        box(x + 15, 432, 120, 52, "ECS · dashboard", fill="#ffffff", stroke="#1e88e5", font=10)
        box(x + 145, 432, 120, 52, "ECS · ml-monitor", fill="#ffffff", stroke="#1e88e5", font=10)
    if k < 2:
        box(x + 15, 494, 250, 44, "MWAA worker (Airflow, 2 sous-réseaux)", fill="#ffffff", stroke="#1e88e5", font=10)
    # Data tier
    rds_label = ["RDS PostgreSQL 16\nprimaire", "RDS standby\nsynchrone (Multi-AZ)", "RDS réplica\nde lecture"][k]
    data_nodes.setdefault("rds", []).append(
        box(x + 15, 606, 250, 56, rds_label, fill="#ffffff", stroke="#d81b60", font=10, bold=(k == 0)))
    data_nodes.setdefault("msk", []).append(
        box(x + 15, 672, 250, 46, f"MSK broker {k + 1} · kafka.m7g.large", fill="#ffffff", stroke="#d81b60", font=10))
    if k < 2:
        data_nodes.setdefault("redis", []).append(
            box(x + 15, 728, 250, 46, ["ElastiCache Redis primaire", "ElastiCache Redis réplica"][k],
                fill="#ffffff", stroke="#d81b60", font=10))
    data_nodes.setdefault("pl", []).append(
        box(x + 15, 784, 250, 46, "ENI PrivateLink → MongoDB Atlas", fill="#ffffff", stroke="#d81b60", font=10))
    box(x + 15, 840, 250, 120,
        ["SG rds : 5432\nSG msk : 9098 (IAM + TLS)\nSG redis : 6379 (TLS + AUTH)\nSG mongo : PrivateLink\n→ uniquement depuis le SG app",
         "RDS : rds.force_ssl=1,\nlogical_replication=1 (CDC)\nMSK : RF 3, min.insync=2,\nauto.create.topics=false",
         "Aucune route 0.0.0.0/0\ndans ces sous-réseaux\n(PCI-DSS 1.3)"][k],
        fill="#fff5f8", stroke="#f8bbd0", font=9, fcolor="#7a2847", align="left")

edge(users, igw, "HTTPS", color="#555555")
edge(igw, nat[1], "", color="#43a047", exit_=(0.5, 1), entry=(0.5, 0))

# Services régionaux hors VPC (droite de la région)
rx = 1160
box(rx, 135, 200, 24, "Services régionaux", fill="#fbfbff", stroke="#fbfbff", font=12, bold=True)
kms = box(rx, 165, 200, 60, "KMS — CMK unique\nrotation annuelle\nmodules/security", fill="#ffffff", stroke="#7b1fa2", font=10)
sec = box(rx, 235, 200, 60, "Secrets Manager\nRedis AUTH, Mongo, dashboard\n(mot de passe RDS géré par RDS)", fill="#ffffff", stroke="#7b1fa2", font=10)
ecr = box(rx, 305, 200, 50, "ECR — image app\nscan CVE à chaque push", fill="#ffffff", stroke="#1e88e5", font=10)
s3 = box(rx, 365, 200, 70, "S3 data lake + bucket DAGs\nSSE-KMS, TLS only,\nIA 90 j → Glacier IR 365 j", fill="#ffffff", stroke="#43a047", font=10)
cw = box(rx, 445, 200, 80, "CloudWatch Logs + alarmes\n(CPU RDS, lag scorer)\n→ SNS e-mail", fill="#ffffff", stroke="#f57c00", font=10)
bud = box(rx, 535, 200, 60, "AWS Budgets\n4 000 $/mois (alerte 80 %)", fill="#ffffff", stroke="#f57c00", font=10)
box(rx, 605, 200, 90, "IAM\nrôle d'exécution ECS\nrôle de tâche : topics stripe.*,\ndata lake, CMK", fill="#ffffff", stroke="#7b1fa2", font=10)
box(rx, 705, 200, 70, "Endpoint S3 (gateway)\nle trafic S3 ne passe\nni par NAT ni par Internet", fill="#ffffff", stroke="#3b7dd8", font=10)

# Atlas hors AWS (droite)
atlas = box(1400, 742, 220, 130, "MongoDB Atlas 7\nreplica set 3 nœuds / 3 AZ\nM30, sauvegarde continue\nmodules/mongodb_atlas", fill="#e8f5e9", stroke="#2e7d32", bold=False)
box(1400, 882, 220, 60, "Endpoint service PrivateLink\n(côté Atlas)", fill="#ffffff", stroke="#2e7d32", font=10)
tfstate = box(1400, 120, 220, 70, "Bucket S3 du state Terraform\nversionné, chiffré, .tflock\nterraform/bootstrap", fill="#ffffff", stroke="#999999", font=10)

edge(data_nodes["pl"][2], atlas, "PrivateLink (réseau AWS)", color="#2e7d32", exit_=(1, 0.5), entry=(0, 0.5))

# Légende
box(20, 1010, W - 40, 40,
    "Correspondance PoC → cible : postgres → RDS · kafka+debezium → MSK (Debezium sur MSK Connect, hors périmètre) · redis → ElastiCache · "
    "mongo → Atlas · producers/*, ml-monitor, dashboard → ECS Fargate ARM64 · airflow → MWAA · .env → Secrets Manager + KMS",
    fill="#ffffff", stroke="#cccccc", font=10, align="left")

xml = ('<mxfile host="drawio"><diagram name="Cible AWS" id="aws"><mxGraphModel dx="1600" dy="1000" grid="0" gridSize="10" '
       'guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="0" pageScale="1" pageWidth="1700" pageHeight="1100" '
       'background="#ffffff" math="0" shadow="0"><root><mxCell id="0"/><mxCell id="1" parent="0"/>'
       + "".join(cells) + "</root></mxGraphModel></diagram></mxfile>")
open(OUT, "w", encoding="utf-8").write(xml)
print("written", OUT, len(cells), "cells")
