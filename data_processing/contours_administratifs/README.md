# Documentation

## data_processing_update_contours_administratifs_ressources_list_datagouv

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `dag.py`     |
| Description | Ce traitement permet de mettre à jour la liste des ressources associées au dataset [Contours administratifs](https://www.data.gouv.fr/datasets/contours-administratifs), utilisé pour fournir les données consolidées pour l'API Découpage Administratif|
| Fréquence de mise à jour | Annuelle|
| Données sources| Liste des fichiers S3 sur le bucket S3 OVH `contours-administratifs` hébergé sur la région RBX |
| Données de sorties | [Liste des ressources du jeu de données sur data.gouv](https://www.data.gouv.fr/datasets/contours-administratifs) |
| Channel Tchap d'information | Non |
