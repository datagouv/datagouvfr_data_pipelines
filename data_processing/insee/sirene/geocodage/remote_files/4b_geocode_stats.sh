#! /bin/bash
echo "Store performance stats of geocoding"
grep final -h /srv/sirene/data-sirene/data/*.log | jq -s '.' > /srv/sirene/data-sirene/stats.json
tar czf data/logs.tgz data/*.log
echo "Store performance OK!"
