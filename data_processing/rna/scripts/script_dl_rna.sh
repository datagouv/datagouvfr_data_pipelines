DATADIR="$1"
rm -rf $DATADIR
mkdir -p $DATADIR

URL_rna="$2"
echo $URL_rna

curl $URL_rna > $DATADIR/rna.zip

#unzip -d $DATADIR/rna $DATADIR/rna.zip
# airflow has only 7zip instlaled
7z e $DATADIR/rna.zip -o$DATADIR/rna

SQLDIR="$3"
rm -rf $SQLDIR
mkdir -p $SQLDIR

cd $DATADIR && ls