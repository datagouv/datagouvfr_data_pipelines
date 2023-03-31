DATADIR="$1"
rm -rf $DATADIR
mkdir -p $DATADIR

URL_rna="$2"

curl $URL_rna > $DATADIR/rna.zip

unzip -d $DATADIR/rna $DATADIR/rna.zip

cd $DATADIR && ls