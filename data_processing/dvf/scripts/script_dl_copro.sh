DATADIR="$1"

URL_copro="https://www.data.gouv.fr/api/1/datasets/r/3ea8e2c3-0038-464a-b17e-cd5c91f65ce2"
curl -o $DATADIR/copro.csv $URL_copro -L

cd $DATADIR && ls -lh