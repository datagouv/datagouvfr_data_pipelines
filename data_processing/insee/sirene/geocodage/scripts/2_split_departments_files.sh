#! /bin/bash
env=$1
if [ -z "$env" ] || [ "$env" == "prod" ]; then
    cd "/srv/sirene/data-sirene"
else
    cd "/srv/sirene/data-sirene/$env"
fi
echo "Splitting in one file per department"
mkdir -p data/
rm -f data/dep_*.csv
unzip -p StockEtablissement_utf8.zip | awk -v FPAT='[^,]*|"([^"]|"")*"' '{ print >> "data/dep_"substr($23,1,2)".csv"}'
awk -v FPAT='[^,]*|"([^"]|"")*"' '{ print >> "data/dep_"$23".csv"}' data/dep_75.csv
awk -v FPAT='[^,]*|"([^"]|"")*"' '{ print >> "data/dep_"substr($23,1,3)".csv"}' data/dep_97.csv
rm -f data/dep_75.csv data/dep_97.csv data/dep_co.csv
echo "Split OK!"
