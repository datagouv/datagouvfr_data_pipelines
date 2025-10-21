# Documentation

## data_processing_carburants

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG.py`     |
| Description | Ce traitement permet de récupérer les données des prix des caburants publiés pas le MEF et les traite (nettoyage, convertisseur utf-8, convertisseur geojson) afin de générer des fichiers de synthèses propres sur les prix des caburants. Ce process permet d'alimenter le [tableau de bord des prix des carburants](https://explore.data.gouv.fr/prix-caburants)  |
| Fréquence de mise à jour | Toutes les 30min |
| Données sources | [JDD Flux instantané](https://www.data.gouv.fr/datasets/prix-des-carburants-en-france-flux-instantane/) <br /> [JDD Flux quotidien](https://www.data.gouv.fr/datasets/prix-des-carburants-en-france-flux-quotidien/) |
| Données de sorties | A modifier : Stocké sur [un repo github](https://github.com/etalab/prix-carburants-data) |
| Channel Mattermost d'information | Non |
