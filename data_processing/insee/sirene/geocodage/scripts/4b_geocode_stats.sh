#! /bin/bash
env=$1
if [ -z "$env" ] || [ "$env" == "prod" ]; then
    cd /srv/sirene/data-sirene/data
else
    cd /srv/sirene/data-sirene/$env/data
fi
echo "Store performance stats of geocoding"
grep final -h -a data/*.log | jq -s '.' > stats.json
tar czf data/logs.tgz data/*.log
echo "Store performance OK!"
