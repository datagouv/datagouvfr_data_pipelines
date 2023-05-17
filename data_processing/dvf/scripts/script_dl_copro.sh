DATADIR="$1"

URL_copro="https://www.data.gouv.fr/fr/datasets/r/3ea8e2c3-0038-464a-b17e-cd5c91f65ce2"
wget $URL_copro -O $DATADIR/copro.csv

cd $DATADIR && ls -lh