DATADIR="$1"
five_ago=$2
curr_year=$3
rm -rf $DATADIR
mkdir -p $DATADIR

URL_departements='https://www.insee.fr/api/1/statistiques/fichier/6051727/departement_2022.csv'
URL_communes='https://www.insee.fr/api/1/statistiques/fichier/6051727/commune_2022.csv'

echo Downloading geo files...
curl $URL_departements > $DATADIR/departements.csv
curl $URL_communes > $DATADIR/communes.csv

curl https://cadastre.data.gouv.fr/data/dgfip-pci-vecteur-latest.json > $DATADIR/dgfip-pci-vecteur-latest.json

jq -r '.[].contents[] | select(.name | endswith("edigeo")) | .contents[] | select(.name | endswith("feuilles"))| .contents[].contents[].contents[].name' $DATADIR/dgfip-pci-vecteur-latest.json | cut -d '/' -f 11 | sed 's/edigeo-\|\.tar\.bz2//g' >| $DATADIR/sections.txt

cd $DATADIR && ls -lh