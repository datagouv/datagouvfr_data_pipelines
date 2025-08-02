#! /bin/bash
env=$1
if [ -z "$env" ] || [ "$env" = "prod" ]; then
    data_path="/srv/sirene/data-sirene"
else
    data_path="/srv/sirene/data-sirene/$env"
fi
mkdir -p cd $data_path
cd $data_path
rm -rf data/*
rm Stock*
rm -rf communes/*
echo "Downloading last SIRENE batch..." # ------------------------------------------------------
wget -N https://www.data.gouv.fr/fr/datasets/r/0651fb76-bcf3-4f6a-a38d-bc04fa708576 -O StockEtablissement_utf8.zip
echo "Download OK!"
