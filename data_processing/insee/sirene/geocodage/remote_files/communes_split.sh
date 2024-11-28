#! /bin/bash

echo $1
mkdir -p /srv/sirene/data-sirene/communes

# entête des fichiers CSV par commune
HEAD=$(zcat $1 | head -n 1)
for i in `zcat $1 | csvcut -c codeCommuneEtablissement | sort -u | grep '^[0-9]'`; do
  echo $HEAD > /srv/sirene/data-sirene/communes/$i.csv;
done

# split des fichiers
zcat $1 | tail -n +2 | awk -v FPAT='[^,]*|"([^"]|"")*"' '{print >> "/srv/sirene/data-sirene/communes/"$23".csv"}'
