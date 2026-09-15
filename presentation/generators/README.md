# Générateurs des livrables de présentation

Les documents Word et le diagramme de la cible AWS sont générés par code, pour
pouvoir les régénérer à chaque évolution du projet sans retouche manuelle.

| Script | Produit | Commande (depuis `presentation/`) |
|---|---|---|
| `architecture.js` + `lib.js` | `stripe_architecture_bloc2.docx` | `node generators/architecture.js . stripe_architecture_bloc2.docx` |
| `competences.js` + `lib.js` | `stripe_bloc2_competences.docx` | `node generators/competences.js . stripe_bloc2_competences.docx` |
| `gen_aws_drawio.py` | `stripe_aws_cible.drawio` | `python3 generators/gen_aws_drawio.py stripe_aws_cible.drawio` puis export PNG avec draw.io |

Prérequis : `npm install docx@9` (dans ce dossier ou globalement). Les chiffres
cités (métriques ML, coûts) proviennent de `ml/models/fraud_xgboost-v1.meta.json`,
de `queries/postgres_oltp.sql` §3 exécuté le 15/09/2026 et de `docs/FINOPS.md`.
