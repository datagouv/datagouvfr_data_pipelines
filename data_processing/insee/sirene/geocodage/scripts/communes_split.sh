#! /bin/bash

env=$2
if [ -z "$env" ] || [ "$env" = "prod" ]; then
    data_path="/srv/sirene/data-sirene"
else
    data_path="/srv/sirene/data-sirene/$env"
fi
mkdir -p $data_path/communes

# entête des fichiers CSV par commune
HEAD=$(zcat $1 | head -n 1)
for i in `zcat $1 | csvcut -c codeCommuneEtablissement | sort -u | grep '^[0-9]'`; do
  echo $HEAD > $data_path/communes/$i.csv;
done

# split des fichiers
zcat $1 | tail -n +2 | awk -v FPAT='[^,]*|"([^"]|"")*"' '{print >> "${data_path}/communes/"$23".csv"}'
