#! /bin/bash
cd /srv/sirene/data-sirene/
echo "Splitting in one file per department"
mkdir /srv/sirene/data-sirene/data/
rm -f /srv/sirene/data-sirene/data/dep_*.csv
unzip -p /srv/sirene/data-sirene/StockEtablissement_utf8.zip | awk -v FPAT='[^,]*|"([^"]|"")*"' '{ print >> "/srv/sirene/data-sirene/data/dep_"substr($23,1,2)".csv"}'
awk -v FPAT='[^,]*|"([^"]|"")*"' '{ print >> "/srv/sirene/data-sirene/data/dep_"$23".csv"}' /srv/sirene/data-sirene/data/dep_75.csv
awk -v FPAT='[^,]*|"([^"]|"")*"' '{ print >> "/srv/sirene/data-sirene/data/dep_"substr($23,1,3)".csv"}' /srv/sirene/data-sirene/data/dep_97.csv
rm -f /srv/sirene/data-sirene/data/dep_75.csv /srv/sirene/data-sirene/data/dep_97.csv /srv/sirene/data-sirene/data/dep_co.csv
echo "Split OK!"
