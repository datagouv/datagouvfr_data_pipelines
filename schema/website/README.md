# Documentation

## schema_website_publication_(pre)prod

| Information | Valeur |
| -------- | -------- |
| Fichiers sources     | `DAG_schema_website_publication_(pre)prod.py`     |
| Description | Ce traitement permet de générer l'ensemble de la documentation liée aux schémas sur le site schema.data.gouv.fr. Ce DAG scrute les différents repo Git des schémas référencés, récupère l'ensemble des releases et génère la documentation markdown associée. |
| Fréquence de mise à jour | Quotidienne |
| Données sources | Repositories github |
| Données de sorties | schema.(preprod.)data.gouv.fr |
| Channel Mattermost d'information | Aucun |
