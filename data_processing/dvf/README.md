# Documentation

## data_processing_dvf

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG.py`     |
| Description | Ce traitement permet de récupérer les données DVF et les traite afin de générer des statistiques à différents niveaux géographiques. Le choix a été fait de calculer les indicateurs suivants : nombre de mutations, moyenne des prix au m² et médiane des prix au m² pour chaque type de bien sélectionné (parmi : maisons, appartements, appartements + maisons, locaux). Seules les ventes concernant uniquement un seul bien sont prises en compte (par exemple, une vente incluant 1 appartement et 1 local commercial n'est pas compatbilisée, de même qu'une vente de 3 appartements). Le prix au m² est calculé en divisant la valeur foncière par la surface réelle du bâti.  |
| Fréquence de mise à jour | Manuelle |
| Données sources | [JDD DVF](https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/) |
| Données de sorties | Postgres et [jeu de données](https://www.data.gouv.fr/fr/datasets/statistiques-dvf/) |
| Channel Mattermost d'information | ~startup-datagouv-dataeng |

## Description du traitement
Le [code](https://github.com/etalab/datagouvfr_data_pipelines/tree/main/data_processing/dvf) permet de générer des statistiques à partir des [données des demandes de valeurs foncières](https://files.data.gouv.fr/geo-dvf/latest/csv/), agrégées à différentes échelles, et leur évolution dans le temps (au mois). Le choix a été fait de calculer les indicateurs suivants, mensuellement et sur la totalité de la période disponible (10 semestres) :
* nombre de mutations
* moyenne des prix au m²
* médiane des prix au m²
* répartition des prix de ventes par tranches

pour chaque type de bien parmi :
* maisons
* appartements
* maisons + appartements
* locaux commerciaux

et pour chaque échelle parmi :
* nation
* département
* EPCI
* commune
* section cadastrale

Les données source contiennent les types de mutations suivants : vente, vente en l’état futur d’achèvement, vente de terrain à bâtir, adjudication, expropriation et échange. Nous avons fait le choix de ne garder que les ventes, ventes en l'état futur d'achèvement et adjudications pour les statistiques*.

Par ailleurs, dans un souci de simplicité, nous avons fait le choix de ne garder que les mutations qui concernent un seul bien (hors dépendance)*. Notre cheminement est le suivant :
1. pour une mutation qui inclurait des biens de plusieurs types (par exemple une maison + un local commercial), il n'est pas possible de reconstituer la part de la valeur foncière attribuée à chacun des biens inclus.
2. pour une mutation qui inclurait plusieurs biens d'un même type (par exemple X appartements), la valeur totale de la mutation n'est pas forcément égale à X fois la valeur d'un appartement, notamment dans le cas où les biens sont très différents (surface, travaux à réaliser, étage, etc.). Nous avions dans un premier temps gardé ces biens en calculant le prix au m² de la mutation en considérant les biens de la mutation comme un seul bien d'une surface à la somme des surfaces des biens, mais cette méthode, qui ne concernait finalement qu'une quantité marginale de biens, ne nous a pas convaincus pour la version finale.

Le prix au m² est ensuite calculé en divisant la valeur foncière de la mutation par la surface du bâti du bien concerné. Nous excluons finalement les mutations pour lesquelles nous n'avons pas pu calculer le prix au m², ainsi que celles dont le prix au m² est supérieur à 100k€ (choix arbitraire)*. Nous n'avons pas intégré d'autres restrictions sur les valeurs aberrantes afin de conserver la fidélité aux données d'origine et de faire remonter de potentielles anomalies. L'affichage de la médiane sur le site réduit l'impact des valeurs aberrantes sur les échelles de couleurs.

_* : Les filtres mentionnés sont appliqués pour le calcul des statistiques, mais toutes les mutations des fichiers source sont bien affichées sur l'application au niveau des parcelles._
