curr_year=`date +'%Y'`
five_ago=$((curr_year-5))
echo $PWD
DATADIR="$1"
rm -rf $DATADIR
mkdir -p $DATADIR

URL_departements='https://www.insee.fr/fr/statistiques/fichier/6051727/departement_2022.csv'
URL_communes='https://www.insee.fr/fr/statistiques/fichier/6051727/commune_2022.csv'

curl $URL_departements > $DATADIR/departements.csv
curl $URL_communes > $DATADIR/communes.csv

wget --header="Accept-Encoding: gzip" https://cadastre.data.gouv.fr/data/dgfip-pci-vecteur-latest.json -O - | gunzip -d > $DATADIR/dgfip-pci-vecteur-latest.json
jq -r '.[].contents[] | select(.name | endswith("edigeo")) | .contents[] | select(.name | endswith("feuilles"))| .contents[].contents[].contents[].name' $DATADIR/dgfip-pci-vecteur-latest.json | cut -d '/' -f 11 | sed 's/edigeo-\|\.tar\.bz2//g' >| $DATADIR/sections.txt

for YEAR in `seq $five_ago $curr_year`
do
  echo $YEAR && [ ! -f $DATADIR/full_$YEAR.csv.gz ] && wget -r -np -nH -N --cut-dirs 5  https://files.data.gouv.fr/geo-dvf/latest/csv/$YEAR/full.csv.gz -O $DATADIR/full_$YEAR.csv.gz
done

find $DATADIR -name '*.gz' -exec gunzip -f '{}' \;

URL_copro="https://www.data.gouv.fr/fr/datasets/r/3ea8e2c3-0038-464a-b17e-cd5c91f65ce2"
wget $URL_copro -O $DATADIR/copro.csv

cd $DATADIR && rm *.gz && ls -lh