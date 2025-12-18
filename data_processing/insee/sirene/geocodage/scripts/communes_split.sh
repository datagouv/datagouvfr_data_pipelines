#! /bin/bash

env=$2
if [ -z "$env" ] || [ "$env" = "prod" ]; then
    data_path="/srv/sirene/data-sirene/communes"
else
    data_path="/srv/sirene/data-sirene/$env/communes"
fi

# entête des fichiers CSV par commune
HEAD=$(zcat $1 | head -n 1)
for i in `zcat $1 | csvcut -c codeCommuneEtablissement | sort -u | grep '^[0-9]'`; do
  echo $HEAD > $data_path/$i.csv;
done

# split des fichiers
zcat $1 | tail -n +2 | awk -v data_path="$data_path" 'BEGIN{FPAT="[^,]*|\"([^\"]|\"\")*\""} {print >> (data_path "/" $23 ".csv")}'
