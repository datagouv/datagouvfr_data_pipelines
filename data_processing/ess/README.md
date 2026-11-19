# Documentation

## data_processing_ess_consolidation

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `dag.py`     |
| Description | Ce traitement concatène en un seul CSV les fichiers Excel régionaux de la liste des entreprises de l'ESS publiés par ESS France |
| Fréquence de mise à jour | Hebdomadaire, le dimanche |
| Données sources | [Liste des entreprises de l'ESS sur le site d'ESS France](https://www.ess-france.org/fr/la-liste-des-entreprises-de-less) |
| Données de sorties | CSV sur data.gouv.fr (IDs dans `config.json`) |
| Notification | bot-datagouv-dataeng via `meta_dag` |

## Sources et publication

ESS France publie la liste sous forme de plusieurs classeurs Excel regroupés par régions.
Le traitement récupère tous les liens `.xlsx` de la page source puis construit un fichier agrégé.
Le CSV est uploadé sur la ressource existante du jeu de données dédié.
Le titre de la ressource mentionne l'année et le mois présents dans le chemin des fichiers sources, e.g. : `/system/files/2026-06/Liste1CvlNorHdfPdl_0.xlsx` donne `entreprises_ess_2026-06.csv`.

## Transformations

Les lignes des classeurs sont concaténées sans dédoublonnage, avec la logique suivante :

- seule la première feuille de chaque classeur est lue (son nom varie selon les fichiers : `Sheet1`, `Liste`)
- les colonnes doivent être les mêmes dans tous les classeurs, sinon le traitement échoue
- les colonnes reprennent les noms et l'ordre du [CSV publié auparavant par ESS France](https://www.data.gouv.fr/datasets/liste-des-entreprises-de-less).
- toutes les valeurs sont lues comme du texte
- Nettoyage spécifiques :
  - `Code postal` : certains codes sont stockés dans les classeurs sous forme de texte avec une partie décimale (`3000.0`), les codes numériques sont ramenés sur 5 chiffres (`03000`), les adresses non diffusibles parfois notées `0[ND]` dans les classeurs deviennent `[ND]`, les autres valeurs sont laissées telles quelles
  - `Code du département de l'établissement` : les classeurs mélangent plusieurs formats (`2`, `021`, `02A`), les codes sont convertis au format INSEE (`02`, `21`, `2A`, `971`)
