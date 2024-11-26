#! /bin/bash
echo "Splitting by locality"
cd /srv/sirene/data-sirene/data
ls -1 geo_siret_*.csv.gz | parallel sh /srv/sirene/geocodage-sirene/communes_split.sh {}
echo "Split OK!"
