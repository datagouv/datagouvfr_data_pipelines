# curr_year=`date +'%Y'`
curr_year=2022
five_ago=$((curr_year-5))
echo $PWD
DATADIR="$1"
rm -rf $DATADIR
mkdir -p $DATADIR

URL_departements='https://www.insee.fr/fr/statistiques/fichier/6051727/departement_2022.csv'
URL_communes='https://www.insee.fr/fr/statistiques/fichier/6051727/commune_2022.csv'

curl $URL_departements > $DATADIR/departements.csv
curl $URL_communes > $DATADIR/communes.csv

for YEAR in `seq $five_ago $curr_year`
do
  echo $YEAR && [ ! -f $DATADIR/full_$YEAR.csv.gz ] && wget -r -np -nH -N --cut-dirs 5  https://files.data.gouv.fr/geo-dvf/latest/csv/$YEAR/full.csv.gz -O $DATADIR/full_$YEAR.csv.gz
done

find $DATADIR -name '*.gz' -exec gunzip -f '{}' \;

cd $DATADIR && ls