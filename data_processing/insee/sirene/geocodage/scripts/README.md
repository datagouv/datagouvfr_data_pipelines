# Scripts Distants

Scripts de géocodages exécutés sur un serveur distant distinct de celui d'Airflow.

## Configuration

Installer Python 3.7 sur le serveur distant qui exécuter les scripts.
Puis les dépendences de `normadresse-requirements.txt`.

## Normadresse

Le code source d'origine provient d'un repo de la BAN qui n'est plus maintenu depuis 2017 : https://github.com/BaseAdresseNationale/normadresse

Scripts pour abréger des libellés d'adresses en suivant la norme NF Z10-011.

### Utilisation

#### En ligne de commande:
```
python normadresse.py 'BOULEVARD DU MARECHAL JEAN MARIE DE LATTRE DE TASSIGNY'
echo 'BOULEVARD DU MARECHAL JEAN MARIE DE LATTRE DE TASSIGNY' | python normadresse.py
cat libelles.txt | python normadresse.py
```
#### Comme module python:
```
from normadresse import abrev

print(abrev('BOULEVARD DU MARECHAL JEAN MARIE DE LATTRE DE TASSIGNY'))

```
Retournera: `bd mal j m DE LATTRE DE TASSIGNY`

Les mot abrégés sont mis en minuscule à titre indicatif, il suffit d'un upper() pour obtenir une version normalisée.

Il est possible de modifier la longueur maximale (32 caractères par défaut) pour s'adapter à la longueur variable du numéro précédent le libellé de voie ou lieu-dit.

### Principe

Une série d'expressions régulières est appliquée pour chaque étape d'abréviation. Ces regex sont stockées dans un fichier CSV, **leur ordre est important**.

A chaque étape, les mots abrégés sont pris de gauche à droite et dès que la longueur maximale n'est pas dépassée le résultat est envoyé.
