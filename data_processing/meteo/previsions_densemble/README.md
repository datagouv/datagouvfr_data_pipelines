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
- **Membre** : un des modèles qui composent la prévision d'ensemble. La
  prévision d'ensemble repose sur plusieurs membres (un ensemble de simulations)
  pour rendre compte de l'incertitude. Pour chaque échéance d'un run,
  Météo-France produit un fichier par membre (`_mb0_`, `_mb1_`, …) ; le pipeline
  attend que tous les membres d'une échéance soient arrivés (leur nombre est
  décrit par `nb_membres` dans `config.json`), puis les concatène en un seul
  fichier GRIB avant de le publier. Pendant une alerte cyclonique, certains
  packs comptent davantage de membres que d'habitude.
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

## Nomenclature des fichiers

Les fichiers portent le même type d'information à chaque étape
(pack, grille, date/heure, membre, échéance), mais la forme de leur nom change
selon qu'ils sont sur le SFTP Météo-France, sur S3 ou publiés sur data.gouv.fr.

### Sur le SFTP source

Un fichier par **membre**, déposé par Météo-France :

```
   arome_pecaledonie_202409230600_mb0_ncaled0025_00:00.grib
   ^^^^^ ^^^^^^^^^^^ ^^^^^^^^^^^^ ^^^ ^^^^^^^^^^ ^^^^^^^^^^
    pack   (ignoré)      run     membre  grid     échéance
```

- `pack` : famille de modèle (`arome`, `arpege`).
- Le second segment (`pecaledonie` ici) n'est **pas utilisé** par le pipeline.
- `run` : date/heure du run, format `%Y%m%d%H%M`.
- `membre` : identifie le membre dans `mb0`, `mb1`, etc.
- `grid` : grille géographique (ex. `ncaled0025`).
- `échéance` : horizon de prévision, ex. `00:00`.

### Sur S3

Les membres d'une même échéance sont **concaténés en un seul fichier**, puis
stockés dans un dossier par run ; le fichier ne porte donc plus de membre :

```
   data/arome/ncaled0025/202409231200/arome_ncaled0025_202409231200_00:00.grib
        ^^^^^ ^^^^^^^^^^ ^^^^^^^^^^^^ ^^^^^ ^^^^^^^^^^ ^^^^^^^^^^^^ ^^^^^
         pack    grid        run       pack    grid         run    échéance
```

- Le chemin `data/{pack}/{grid}/{run}/` est le **run** : sa date est
  **l'unité de rétention** (tout le dossier est supprimé ensemble).
- Le nom du fichier `{pack}_{grid}_{run}_{échéance}.grib` reprend la même
  date que celle du dossier.

### Publié sur data.gouv.fr

On n'expose que la **dernière occurrence** pour chaque combinaison
`{pack}_{grid}_{échéance}` : la date est retirée de l'identifiant de la
ressource, et chaque échéance pointe vers le run le plus récent dans lequel
elle est disponible.

- Identifiant de ressource : `{pack}_{grid}_{échéance}` (ex. `arome_ncaled0025_00:00`),
  sans référence à un run particulier.
- Le titre affiché est dérivé du nom du fichier, avec `arome` → `pearome` et
  `arpege` → `pearp` (`fix_title`).

## Politique de rétention

La rétention est pilotée par la constante `TIME_DEPTH_TO_KEEP` (15 jours) dans
`task_functions.py`. Pour chaque `{pack}_{grid}`, une date de seuil est
calculée : **le run le plus récent** publié sur data.gouv.fr **moins la durée
de rétention**. Tout élément strictement antérieur à ce seuil est supprimé :

- **Sur S3** : le **run entier** est supprimé, c'est-à-dire tout le dossier
  `data/{pack}/{grid}/{run}/` et tous les fichiers (l'ensemble de ses
  échéances) qu'il contient.
- **Sur le SFTP** : chaque **fichier** source du répertoire d'arrivée
  strictement antérieur au seuil est supprimé, pour éviter l'accumulation des
  fichiers non encore traités. Le seuil, propre à la grille, est comparé à tous
  les fichiers présents sur le SFTP (la grille n'y est pas pré-filtrée).

Le seuil est ancré sur le run le plus **récent** (et non le plus ancien) : la
rétention est donc appliquée relativement à la dernière donnée publiée plutôt
qu'à la plus vieille. Les éléments exactement égaux au seuil sont conservés.
