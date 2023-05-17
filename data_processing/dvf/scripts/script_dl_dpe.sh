DATADIR="$1"

URL_dpe="http://opendata.ademe.fr/dump_dpev2_prod_fdld.sql.gz"
wget $URL_dpe -O $DATADIR/dpe.sql.gz

find $DATADIR -name '*.gz' -exec gunzip -f '{}' \;

cd $DATADIR && rm *.gz && ls -lh