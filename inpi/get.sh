#! /bin/bash
USER=`echo $2`
PASS=`echo $3`

DATETOPROCESS=`echo $1 | sed 's/-/\//g'`
echo $DATETOPROCESS

mkdir -p /tmp/inpi/flux-tc
mkdir -p /tmp/inpi/stock
mkdir -p /tmp/inpi/synthese

REGEX="/public/IMR_Donnees_Saisies/tc/flux/"$DATETOPROCESS"/*/*/*.csv"
cd /tmp/inpi/flux-tc
mkdir -p $DATETOPROCESS
cd $DATETOPROCESS

lftp ftp://opendata-rncs.inpi.fr << DOWNLOAD
  user $USER "$PASS"
  mget -E $REGEX
DOWNLOAD

if ls ./*.csv 1> /dev/null 2>&1; then
for f in ./*.csv;
do
item1=$(echo $f | cut -d "_" -f 1)
item2=$(echo $f | cut -d "_" -f 2)
mkdir -p ${item1:2:4}/$item2
mv $f ${item1:2:4}/$item2/
done;
fi

REGEX="/public/IMR_Donnees_Saisies/tc/stock/"$DATETOPROCESS"/*.zip"
cd /tmp/inpi/stock
mkdir -p $DATETOPROCESS
cd $DATETOPROCESS

lftp ftp://opendata-rncs.inpi.fr << DOWNLOAD
  user $USER "$PASS"
  mget -E $REGEX
DOWNLOAD

if ls ./*.zip 1> /dev/null 2>&1; then
for f in ./*.zip;
do
unzip $f
rm $f
done;
fi

if ls ./*.csv 1> /dev/null 2>&1; then
for f in ./*.csv;
do
item1=$(echo $f | cut -d "_" -f 1)
item2=$(echo $f | cut -d "_" -f 2)
mkdir -p ${item1:2:4}/${item2:0:2}
mv $f ${item1:2:4}/${item2:0:2}/
done;
fi

echo "recuperation done!"