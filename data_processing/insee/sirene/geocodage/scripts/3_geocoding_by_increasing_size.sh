#! /bin/bash
path=$1
echo "Geocoding by increasing size of departments"
cd /srv/sirene/geocodage-sirene/
# extrait la liste des anciens codes INSEE et nouveau correspondant
csvgrep -c POLE -r '^.+$' -t /srv/sirene/geocodage-sirene/France2018.tsv -e iso8859-1 | csvcut -c 6,7,11,14 | sed 's/,//' > /srv/sirene/geocodage-sirene/histo_depcom.csv
time wc -l /srv/sirene/data-sirene/data/dep_*.csv | sort -n -r | grep dep | sed 's/^.*_\(.*\).csv/\1/' | \
  parallel -j 36 -t /srv/sirene/venv/bin/python $path/geocode.py /srv/sirene/data-sirene/data/dep_{}.csv /srv/sirene/data-sirene/data/geo_siret_{}.csv.gz /srv/sirene/data-sirene/cache_geo/cache_addok_sirene_{}.csv.db \> /srv/sirene/data-sirene/data/geo_siret_{}.log
echo "Geocode OK!"
