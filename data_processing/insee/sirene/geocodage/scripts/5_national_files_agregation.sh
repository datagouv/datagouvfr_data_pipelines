#! /bin/bash
echo "Agregating national files"
zcat /srv/sirene/data-sirene/data/geo_siret_01.csv.gz | head -n 1 | gzip -9 --rsyncable > /srv/sirene/data-sirene/StockEtablissement_utf8_geo.csv.gz
for f in /srv/sirene/data-sirene/data/geo_siret*.csv.gz; do zcat "$f" | tail -n +2 | gzip -9 --rsyncable >> /srv/sirene/data-sirene/StockEtablissement_utf8_geo.csv.gz ; done
# fichier séparé pour établissements actifs/fermés seuls
zcat /srv/sirene/data-sirene/StockEtablissement_utf8_geo.csv.gz \
| tee >(csvgrep -c etatAdministratifEtablissement -m "A" | gzip -9 --rsyncable > /srv/sirene/data-sirene/StockEtablissementActif_utf8_geo.csv.gz) >(csvgrep -c etatAdministratifEtablissement -m "F" | gzip -9 --rsyncable > /srv/sirene/data-sirene/StockEtablissementFerme_utf8_geo.csv.gz) > /dev/null
echo "National files agregation OK!"
