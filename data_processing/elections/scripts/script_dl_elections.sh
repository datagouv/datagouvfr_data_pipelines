DATADIR="$1"
rm -rf $DATADIR
mkdir -p $DATADIR

URL_2022_pres_t1='https://www.data.gouv.fr/fr/datasets/r/79b5cac4-4957-486b-bbda-322d80868224'
URL_2022_pres_t2='https://www.data.gouv.fr/fr/datasets/r/4dfd05a9-094e-4043-8a19-43b6b6bbe086'
URL_2022_legi_t1='https://www.data.gouv.fr/fr/datasets/r/a1f73b85-8194-44f4-a2b7-c343edb47d32'
URL_2022_legi_t2='https://www.data.gouv.fr/fr/datasets/r/96ffddda-59b4-41b8-a6a3-dfe1adb7fa36'

wget -O $DATADIR/2022_pres_t1.txt $URL_2022_pres_t1
wget -O $DATADIR/2022_pres_t2.txt $URL_2022_pres_t2
# wget -O $DATADIR/2022_legi_t1.txt $URL_2022_legi_t1
wget -O $DATADIR/2022_legi_t2.txt $URL_2022_legi_t2

cd $DATADIR && ls