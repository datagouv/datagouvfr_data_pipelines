# DAGs afférent à schema.data.gouv.fr

Ce sous-dossier héberge l'ensemble des scripts d'automatisation liés au schéma de données sur schema.data.gouv.fr.

Les scripts d'automatisation sont développés en tant que DAG Airflow. Le code source permettant de générer la stack airflow que nous utilisons est hébergé [sur ce repo](https://github.com/etalab/data-engineering-stack).

3 DAGs sont présents dans ce repo : 
- DAG permettant de générer l'ensemble de la documentation liée aux schémas sur le site schema.data.gouv.fr. Ce DAG scrute les différents repo Git des schémas référencés, récupère l'ensemble des release et génère la documentation markdown associée. Celle-ci est ensuite poussée vers [le repo du site schema.data.gouv.fr](https://github.com/etalab/schema.data.gouv.fr) pour mise à jour. Ce DAG est executé tous les jours.
- DAG permettant de générer des fichiers de consolidation basés sur les ressources publiées sur le site data.gouv.fr. Ce DAG scrute les ressources présentes sur le site data.gouv.fr et vérifie si celles-ci sont en cohérence avec un schéma. Le DAG concatène ensuite l'ensemble des données en un seul fichier par schéma. Ce DAG est executé une fois par semaine.
- DAG permettant de générer des recommendations sur data.gouv.fr. Les recommendations consistent en un lien visible sur la page d'un jeu de donnée data.gouv.fr vers les fichiers nationaux lié au schéma de la ressource.
