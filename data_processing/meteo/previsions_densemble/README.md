# Documentation

## data_processing_meteo_previsions_densemble

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `dag.py`     |
| Description | Ce traitement permet de récupérer et d'exposer les données de prévisions d'ensemble de Météo France |
| Fréquence de mise à jour | Infra-quoditienne |
| Données sources | SFTP |
| Données de sorties | data.gouv.fr |
| Channel Tchap d'information | bot-datagouv-dataeng |

## Vocabulaire

- **Pack** : famille de modèle de prévision d'ensemble Météo-France. Par exemple
  `arome` (PEAROME, ensemble pour le domaine AROME) ou `arpege` (PEARP, ensemble
  pour le domaine ARPEGE).
- **Grid** : grille géographique, soit le domaine (zone couverte) combiné au
  maillage (résolution). Par exemple `ncaled0025` (Nouvelle-Calédonie à 0,025°),
  `eurat01` (Europe à 0,1°) ou `glob025` (global à 0,25°).
- **Échéance** : délai de prévision (horizon de temps), exprimé en heures, de
  `00:00` à `48:00`. Chaque échéance d'un même modèle correspond à une ressource
  distincte sur data.gouv.fr.
- **Run** : l'ensemble des échéances (`00:00`…`48:00`) calculées ensemble par
  Météo-France pour une date donnée. Tous les fichiers d'un même run partagent
  la même date/heure de calcul, sont stockés dans un seul dossier S3 et exposés
  sur data.gouv.fr comme autant de ressources pointant vers cette date. C'est
  **l'unité de rétention** : un run est conservé ou supprimé dans son ensemble.
