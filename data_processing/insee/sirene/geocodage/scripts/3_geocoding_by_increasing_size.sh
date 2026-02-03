#! /bin/bash
env=$1
script_path=$2
set -e

echo "Geocoding by increasing size of departments"
cd /srv/sirene/geocodage-sirene/
# extrait la liste des anciens codes INSEE et nouveau correspondant
csvgrep -c POLE -r '^.+$' -t $script_path/resources/France2018.tsv -e iso8859-1 | csvcut -c 6,7,11,14 | sed 's/,//' > $script_path/resources/histo_depcom.csv

if [ -z "$env" ] || [ "$env" = "prod" ]; then
    data_path="/srv/sirene/data-sirene/data"
    # Production mode: process all departments
    time wc -l $data_path/dep_*.csv | sort -n -r | grep dep | sed 's/^.*_\(.*\).csv/\1/' | \
      parallel -t /srv/sirene/venv/bin/python $script_path/geocode.py $data_path/dep_{}.csv $data_path/geo_siret_{}.csv.gz /srv/sirene/data-sirene/cache_geo/cache_addok_sirene_{}.csv.db \> $data_path/geo_siret_{}.log
else
    data_path="/srv/sirene/data-sirene/$env/data"
    echo "Running in dev mode: processing only dep_23.csv, dep_75111.csv and dep_972.csv"
    departments="23 75111 972"
    for dep in $departments; do
        /srv/sirene/venv/bin/python $script_path/geocode.py $data_path/dep_${dep}.csv $data_path/geo_siret_${dep}.csv.gz /srv/sirene/data-sirene/cache_geo/cache_addok_sirene_${dep}.csv.db > $data_path/geo_siret_${dep}.log &
    done
    wait
fi
echo "Geocode OK!"
