mkdir /srv/sirene/data-sirene/$(date +%Y-%m)
mkdir /srv/sirene/data-sirene/$(date +%Y-%m)/dep
mkdir /srv/sirene/data-sirene/$(date +%Y-%m)/communes
mv /srv/sirene/data-sirene/Stock*.csv.gz /srv/sirene/data-sirene/$(date +%Y-%m)/
mv /srv/sirene/data-sirene/data/geo*.csv.gz /srv/sirene/data-sirene/$(date +%Y-%m)/dep/
mv /srv/sirene/data-sirene/communes/* /srv/sirene/data-sirene/$(date +%Y-%m)/communes/
mv /srv/sirene/data-sirene/stats.json /srv/sirene/data-sirene/$(date +%Y-%m)/
