DATADIR="$1"
rm -rf $DATADIR
mkdir -p $DATADIR

curl -o "$DATADIR/history.csv" "https://object.files.data.gouv.fr/data-pipeline-open/prod/dgv/impact/statistiques_impact_datagouvfr.csv" -L

cd $DATADIR && ls