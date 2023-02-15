# Récupération des données INPI

DAG Airflow permettant de récupérer les données INPI du jour précédent. Le DAG se connecte au FTP de l'INPI et sauvegarde les données sur une instance Minio.

Ces données sont alors traitées pour générer deux fichiers rep_pp.csv et rep_pm.csv listant les dirigeants du stock des entreprises traitées par l'INPI.

Ces données ont par la suite vocation à alimenter l'[annuaire des entreprises](https://annuaire-entreprises.data.gouv.fr).

