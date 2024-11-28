#! /bin/bash
cd /srv/sirene/data-sirene/
rm -rf data/*
rm Stock*
rm -rf communes/*
echo "Downloading last SIRENE batch..." # ------------------------------------------------------
wget -N https://files.data.gouv.fr/insee-sirene/StockEtablissement_utf8.zip
echo "Download OK!"
