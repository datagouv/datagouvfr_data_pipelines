# Documentation

## data_processing_dvf

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG.py`     |
| Description | Ce traitement permet de récupérer les données DVF et les traite afin de générer des statistiques à différents niveaux géographiques. Le choix a été fait de calculer les indicateurs suivants : nombre de mutations, moyenne des prix au m² et médiane des prix au m² pour chaque type de bien sélectionné (parmi : maisons, appartements, appartements + maisons, locaux). Seules les ventes concernant uniquement un seul bien sont prises en compte (par exemple, une vente incluant 1 appartement et un local commercial n'est pas compatbilisée, de même qu'une vente de 3 appartements). Le prix au m² est calculé en divisant la valeur foncière par la surface réelle du bâti.  |
| Fréquence de mise à jour | Manuelle |
| Données sources | [JDD DVF](https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/) |
| Données de sorties | Postgres |
| Channel Mattermost d'information | ~startup-datagouv-dataeng |
