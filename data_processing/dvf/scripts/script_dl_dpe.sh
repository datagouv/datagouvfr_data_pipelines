DATADIR="$1"

URL_dpe="https://www.data.gouv.fr/fr/datasets/r/ad4bb2f6-0f40-46d2-a636-8d2604532f74"
curl -o $DATADIR/dpe.tar.gz $URL_dpe -L

tar -xzvf $DATADIR/dpe.tar.gz ./csv/batiment_groupe_dpe_representatif_logement.csv ./csv/rel_batiment_groupe_parcelle.csv

cd $DATADIR && ls -lh
