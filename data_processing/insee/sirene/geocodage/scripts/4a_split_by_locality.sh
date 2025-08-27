#! /bin/bash
env=$1
script_path=$2
echo "Splitting by locality"
if [ -z "$env" ] || [ "$env" = "prod" ]; then
    cd /srv/sirene/data-sirene/data
else
    cd /srv/sirene/data-sirene/$env/data
fi
ls -1 geo_siret_*.csv.gz | parallel sh $script_path/communes_split.sh {} $env
echo "Split OK!"
