DATADIR="$1"
rm -rf $DATADIR
mkdir -p $DATADIR

url="https://rdf.insee.fr/geo/cog-20210310.zip"
curl -o "$DATADIR/cog.zip" "$url" -L

7z e $DATADIR/cog.zip -o$DATADIR/cog

cd $DATADIR && ls