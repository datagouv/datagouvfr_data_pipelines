#! /bin/bash
env=$1
script_path=$2
set -e
if [ -z "$env" ] || [ "$env" = "prod" ]; then
    data_path="/srv/sirene/data-sirene"
else
    data_path="/srv/sirene/data-sirene/$env"
fi
echo "Geocoding by increasing size of departments"
cd /srv/sirene/geocodage-sirene/
# extrait la liste des anciens codes INSEE et nouveau correspondant
csvgrep -c POLE -r '^.+$' -t $script_path/resources/France2018.tsv -e iso8859-1 | csvcut -c 6,7,11,14 | sed 's/,//' > $script_path/resources/histo_depcom.csv
time wc -l $data_path/data/dep_*.csv | sort -n -r | grep dep | sed 's/^.*_\(.*\).csv/\1/' | \
  parallel -j 36 -t /srv/sirene/venv/bin/python $script_path/geocode.py $data_path/data/dep_{}.csv $data_path/data/geo_siret_{}.csv.gz /srv/sirene/data-sirene/cache_geo/cache_addok_sirene_{}.csv.db \> $data_path/data/geo_siret_{}.log
echo "Geocode OK!"
