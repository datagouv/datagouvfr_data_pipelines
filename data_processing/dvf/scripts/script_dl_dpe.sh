DATADIR="$1"

# for each dep
# mkdir dep
# retrieve file
# unzip it
# copy good files
# remove zip and unused files

rm -rf $DATADIR/dpe
mkdir $DATADIR/dpe

mkdir $DATADIR/dpe/01/
curl -L https://www.data.gouv.fr/fr/datasets/r/207fa2c4-00ca-45bc-b603-50caed3828ec > $DATADIR/dpe/01/01.csv.zip
cd $DATADIR/dpe/01
unzip 01.csv.zip
cp $DATADIR/dpe/01/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/01/
cp $DATADIR/dpe/01/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/01/
rm -rf $DATADIR/dpe/01/01.csv.zip && rm -rf $DATADIR/dpe/01/csv && rm -rf $DATADIR/dpe/01/doc

mkdir $DATADIR/dpe/02/
curl -L https://www.data.gouv.fr/fr/datasets/r/44228492-2d10-47dc-a4d0-dced04f4a71d > $DATADIR/dpe/02/02.csv.zip
cd $DATADIR/dpe/02
unzip 02.csv.zip
cp $DATADIR/dpe/02/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/02/
cp $DATADIR/dpe/02/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/02/
rm -rf $DATADIR/dpe/02/02.csv.zip && rm -rf $DATADIR/dpe/02/csv && rm -rf $DATADIR/dpe/02/doc

mkdir $DATADIR/dpe/03/
curl -L https://www.data.gouv.fr/fr/datasets/r/de645a01-12d1-40b2-9ad5-d87e1b750616 > $DATADIR/dpe/03/03.csv.zip
cd $DATADIR/dpe/03
unzip 03.csv.zip
cp $DATADIR/dpe/03/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/03/
cp $DATADIR/dpe/03/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/03/
rm -rf $DATADIR/dpe/03/03.csv.zip && rm -rf $DATADIR/dpe/03/csv && rm -rf $DATADIR/dpe/03/doc

mkdir $DATADIR/dpe/04/
curl -L https://www.data.gouv.fr/fr/datasets/r/b1a8709f-c4c6-433c-a42c-3b6af2470a35 > $DATADIR/dpe/04/04.csv.zip
cd $DATADIR/dpe/04
unzip 04.csv.zip
cp $DATADIR/dpe/04/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/04/
cp $DATADIR/dpe/04/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/04/
rm -rf $DATADIR/dpe/04/04.csv.zip && rm -rf $DATADIR/dpe/04/csv && rm -rf $DATADIR/dpe/04/doc

mkdir $DATADIR/dpe/05/
curl -L https://www.data.gouv.fr/fr/datasets/r/0457ace4-56c0-4f80-9258-31296eebe419 > $DATADIR/dpe/05/05.csv.zip
cd $DATADIR/dpe/05
unzip 05.csv.zip
cp $DATADIR/dpe/05/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/05/
cp $DATADIR/dpe/05/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/05/
rm -rf $DATADIR/dpe/05/05.csv.zip && rm -rf $DATADIR/dpe/05/csv && rm -rf $DATADIR/dpe/05/doc

mkdir $DATADIR/dpe/06/
curl -L https://www.data.gouv.fr/fr/datasets/r/87fca48d-b236-4ec6-8332-1790a7528fa6 > $DATADIR/dpe/06/06.csv.zip
cd $DATADIR/dpe/06
unzip 06.csv.zip
cp $DATADIR/dpe/06/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/06/
cp $DATADIR/dpe/06/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/06/
rm -rf $DATADIR/dpe/06/06.csv.zip && rm -rf $DATADIR/dpe/06/csv && rm -rf $DATADIR/dpe/06/doc

mkdir $DATADIR/dpe/07/
curl -L https://www.data.gouv.fr/fr/datasets/r/eaa99e21-3d95-452a-b6bc-41367f6e6cd7 > $DATADIR/dpe/07/07.csv.zip
cd $DATADIR/dpe/07
unzip 07.csv.zip
cp $DATADIR/dpe/07/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/07/
cp $DATADIR/dpe/07/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/07/
rm -rf $DATADIR/dpe/07/07.csv.zip && rm -rf $DATADIR/dpe/07/csv && rm -rf $DATADIR/dpe/07/doc

mkdir $DATADIR/dpe/08/
curl -L https://www.data.gouv.fr/fr/datasets/r/cccb2810-dac9-4ae9-bd27-46aa4d69a193 > $DATADIR/dpe/08/08.csv.zip
cd $DATADIR/dpe/08
unzip 08.csv.zip
cp $DATADIR/dpe/08/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/08/
cp $DATADIR/dpe/08/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/08/
rm -rf $DATADIR/dpe/08/08.csv.zip && rm -rf $DATADIR/dpe/08/csv && rm -rf $DATADIR/dpe/08/doc

mkdir $DATADIR/dpe/09/
curl -L https://www.data.gouv.fr/fr/datasets/r/d87f6cba-4418-4071-b911-d1d09a9f1d29 > $DATADIR/dpe/09/09.csv.zip
cd $DATADIR/dpe/09
unzip 09.csv.zip
cp $DATADIR/dpe/09/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/09/
cp $DATADIR/dpe/09/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/09/
rm -rf $DATADIR/dpe/09/09.csv.zip && rm -rf $DATADIR/dpe/09/csv && rm -rf $DATADIR/dpe/09/doc

mkdir $DATADIR/dpe/10/
curl -L https://www.data.gouv.fr/fr/datasets/r/bff4ca71-92ef-48ae-ad82-2f0f6060f2b5 > $DATADIR/dpe/10/10.csv.zip
cd $DATADIR/dpe/10
unzip 10.csv.zip
cp $DATADIR/dpe/10/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/10/
cp $DATADIR/dpe/10/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/10/
rm -rf $DATADIR/dpe/10/10.csv.zip && rm -rf $DATADIR/dpe/10/csv && rm -rf $DATADIR/dpe/10/doc

mkdir $DATADIR/dpe/11/
curl -L https://www.data.gouv.fr/fr/datasets/r/731e40f8-9661-4b30-9937-ed016d9abac8 > $DATADIR/dpe/11/11.csv.zip
cd $DATADIR/dpe/11
unzip 11.csv.zip
cp $DATADIR/dpe/11/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/11/
cp $DATADIR/dpe/11/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/11/
rm -rf $DATADIR/dpe/11/11.csv.zip && rm -rf $DATADIR/dpe/11/csv && rm -rf $DATADIR/dpe/11/doc

mkdir $DATADIR/dpe/12/
curl -L https://www.data.gouv.fr/fr/datasets/r/2a448a03-056b-4008-81fd-8fbf19e3aa99 > $DATADIR/dpe/12/12.csv.zip
cd $DATADIR/dpe/12
unzip 12.csv.zip
cp $DATADIR/dpe/12/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/12/
cp $DATADIR/dpe/12/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/12/
rm -rf $DATADIR/dpe/12/12.csv.zip && rm -rf $DATADIR/dpe/12/csv && rm -rf $DATADIR/dpe/12/doc

mkdir $DATADIR/dpe/13/
curl -L https://www.data.gouv.fr/fr/datasets/r/8539ae79-41e9-494d-bc55-1824cd5984d7 > $DATADIR/dpe/13/13.csv.zip
cd $DATADIR/dpe/13
unzip 13.csv.zip
cp $DATADIR/dpe/13/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/13/
cp $DATADIR/dpe/13/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/13/
rm -rf $DATADIR/dpe/13/13.csv.zip && rm -rf $DATADIR/dpe/13/csv && rm -rf $DATADIR/dpe/13/doc

mkdir $DATADIR/dpe/14/
curl -L https://www.data.gouv.fr/fr/datasets/r/2fff64e7-148b-4486-a901-4ba10217810d > $DATADIR/dpe/14/14.csv.zip
cd $DATADIR/dpe/14
unzip 14.csv.zip
cp $DATADIR/dpe/14/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/14/
cp $DATADIR/dpe/14/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/14/
rm -rf $DATADIR/dpe/14/14.csv.zip && rm -rf $DATADIR/dpe/14/csv && rm -rf $DATADIR/dpe/14/doc

mkdir $DATADIR/dpe/15/
curl -L https://www.data.gouv.fr/fr/datasets/r/50921c89-280d-40f8-bd07-66cd21de334d > $DATADIR/dpe/15/15.csv.zip
cd $DATADIR/dpe/15
unzip 15.csv.zip
cp $DATADIR/dpe/15/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/15/
cp $DATADIR/dpe/15/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/15/
rm -rf $DATADIR/dpe/15/15.csv.zip && rm -rf $DATADIR/dpe/15/csv && rm -rf $DATADIR/dpe/15/doc

mkdir $DATADIR/dpe/16/
curl -L https://www.data.gouv.fr/fr/datasets/r/cc4134f6-c856-4bfb-8bc1-dd032d8dd547 > $DATADIR/dpe/16/16.csv.zip
cd $DATADIR/dpe/16
unzip 16.csv.zip
cp $DATADIR/dpe/16/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/16/
cp $DATADIR/dpe/16/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/16/
rm -rf $DATADIR/dpe/16/16.csv.zip && rm -rf $DATADIR/dpe/16/csv && rm -rf $DATADIR/dpe/16/doc

mkdir $DATADIR/dpe/17/
curl -L https://www.data.gouv.fr/fr/datasets/r/6aaf8863-1e40-4d3e-b519-dbef213fe505 > $DATADIR/dpe/17/17.csv.zip
cd $DATADIR/dpe/17
unzip 17.csv.zip
cp $DATADIR/dpe/17/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/17/
cp $DATADIR/dpe/17/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/17/
rm -rf $DATADIR/dpe/17/17.csv.zip && rm -rf $DATADIR/dpe/17/csv && rm -rf $DATADIR/dpe/17/doc

mkdir $DATADIR/dpe/18/
curl -L https://www.data.gouv.fr/fr/datasets/r/8d09ac4f-9f56-4e11-a887-6116605bbf92 > $DATADIR/dpe/18/18.csv.zip
cd $DATADIR/dpe/18
unzip 18.csv.zip
cp $DATADIR/dpe/18/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/18/
cp $DATADIR/dpe/18/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/18/
rm -rf $DATADIR/dpe/18/18.csv.zip && rm -rf $DATADIR/dpe/18/csv && rm -rf $DATADIR/dpe/18/doc

mkdir $DATADIR/dpe/19/
curl -L https://www.data.gouv.fr/fr/datasets/r/80c43b4f-dd23-4e16-aaaf-5b58f64a604f > $DATADIR/dpe/19/19.csv.zip
cd $DATADIR/dpe/19
unzip 19.csv.zip
cp $DATADIR/dpe/19/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/19/
cp $DATADIR/dpe/19/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/19/
rm -rf $DATADIR/dpe/19/19.csv.zip && rm -rf $DATADIR/dpe/19/csv && rm -rf $DATADIR/dpe/19/doc

mkdir $DATADIR/dpe/21/
curl -L https://www.data.gouv.fr/fr/datasets/r/8e264edd-3415-49d4-9e80-409c6c165e38 > $DATADIR/dpe/21/21.csv.zip
cd $DATADIR/dpe/21
unzip 21.csv.zip
cp $DATADIR/dpe/21/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/21/
cp $DATADIR/dpe/21/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/21/
rm -rf $DATADIR/dpe/21/21.csv.zip && rm -rf $DATADIR/dpe/21/csv && rm -rf $DATADIR/dpe/21/doc

mkdir $DATADIR/dpe/22/
curl -L https://www.data.gouv.fr/fr/datasets/r/f41fcccf-4998-4528-bb2a-c923ca0b1704 > $DATADIR/dpe/22/22.csv.zip
cd $DATADIR/dpe/22
unzip 22.csv.zip
cp $DATADIR/dpe/22/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/22/
cp $DATADIR/dpe/22/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/22/
rm -rf $DATADIR/dpe/22/22.csv.zip && rm -rf $DATADIR/dpe/22/csv && rm -rf $DATADIR/dpe/22/doc

mkdir $DATADIR/dpe/23/
curl -L https://www.data.gouv.fr/fr/datasets/r/5aaa2241-193a-4afe-892d-fef49b57ee3c > $DATADIR/dpe/23/23.csv.zip
cd $DATADIR/dpe/23
unzip 23.csv.zip
cp $DATADIR/dpe/23/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/23/
cp $DATADIR/dpe/23/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/23/
rm -rf $DATADIR/dpe/23/23.csv.zip && rm -rf $DATADIR/dpe/23/csv && rm -rf $DATADIR/dpe/23/doc

mkdir $DATADIR/dpe/24/
curl -L https://www.data.gouv.fr/fr/datasets/r/ee2ed2ac-7364-471e-8f5b-0cfb6096be97 > $DATADIR/dpe/24/24.csv.zip
cd $DATADIR/dpe/24
unzip 24.csv.zip
cp $DATADIR/dpe/24/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/24/
cp $DATADIR/dpe/24/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/24/
rm -rf $DATADIR/dpe/24/24.csv.zip && rm -rf $DATADIR/dpe/24/csv && rm -rf $DATADIR/dpe/24/doc

mkdir $DATADIR/dpe/25/
curl -L https://www.data.gouv.fr/fr/datasets/r/f0db0d6f-058f-4ffd-aadd-266253e13ce1 > $DATADIR/dpe/25/25.csv.zip
cd $DATADIR/dpe/25
unzip 25.csv.zip
cp $DATADIR/dpe/25/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/25/
cp $DATADIR/dpe/25/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/25/
rm -rf $DATADIR/dpe/25/25.csv.zip && rm -rf $DATADIR/dpe/25/csv && rm -rf $DATADIR/dpe/25/doc

mkdir $DATADIR/dpe/26/
curl -L https://www.data.gouv.fr/fr/datasets/r/b9bc9390-7681-42a5-9086-4b62cee1f353 > $DATADIR/dpe/26/26.csv.zip
cd $DATADIR/dpe/26
unzip 26.csv.zip
cp $DATADIR/dpe/26/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/26/
cp $DATADIR/dpe/26/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/26/
rm -rf $DATADIR/dpe/26/26.csv.zip && rm -rf $DATADIR/dpe/26/csv && rm -rf $DATADIR/dpe/26/doc

mkdir $DATADIR/dpe/27/
curl -L https://www.data.gouv.fr/fr/datasets/r/b10bcfc8-f4aa-43c0-847a-28fb6ee5d9b1 > $DATADIR/dpe/27/27.csv.zip
cd $DATADIR/dpe/27
unzip 27.csv.zip
cp $DATADIR/dpe/27/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/27/
cp $DATADIR/dpe/27/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/27/
rm -rf $DATADIR/dpe/27/27.csv.zip && rm -rf $DATADIR/dpe/27/csv && rm -rf $DATADIR/dpe/27/doc

mkdir $DATADIR/dpe/28/
curl -L https://www.data.gouv.fr/fr/datasets/r/7e22b82e-8aa5-485a-b058-7a853e848b34 > $DATADIR/dpe/28/28.csv.zip
cd $DATADIR/dpe/28
unzip 28.csv.zip
cp $DATADIR/dpe/28/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/28/
cp $DATADIR/dpe/28/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/28/
rm -rf $DATADIR/dpe/28/28.csv.zip && rm -rf $DATADIR/dpe/28/csv && rm -rf $DATADIR/dpe/28/doc

mkdir $DATADIR/dpe/29/
curl -L https://www.data.gouv.fr/fr/datasets/r/b46c7c3c-5b53-482e-8c88-dea0d9c25682 > $DATADIR/dpe/29/29.csv.zip
cd $DATADIR/dpe/29
unzip 29.csv.zip
cp $DATADIR/dpe/29/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/29/
cp $DATADIR/dpe/29/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/29/
rm -rf $DATADIR/dpe/29/29.csv.zip && rm -rf $DATADIR/dpe/29/csv && rm -rf $DATADIR/dpe/29/doc

mkdir $DATADIR/dpe/2a/
curl -L https://www.data.gouv.fr/fr/datasets/r/c54dc820-c440-4212-b699-bb62a8280c86 > $DATADIR/dpe/2a/2a.csv.zip
cd $DATADIR/dpe/2a
unzip 2a.csv.zip
cp $DATADIR/dpe/2a/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/2a/
cp $DATADIR/dpe/2a/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/2a/
rm -rf $DATADIR/dpe/2a/2a.csv.zip && rm -rf $DATADIR/dpe/2a/csv && rm -rf $DATADIR/dpe/2a/doc

mkdir $DATADIR/dpe/2b/
curl -L https://www.data.gouv.fr/fr/datasets/r/ba9b8819-30f8-4ff8-9dd7-b56b89f214ac > $DATADIR/dpe/2b/2b.csv.zip
cd $DATADIR/dpe/2b
unzip 2b.csv.zip
cp $DATADIR/dpe/2b/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/2b/
cp $DATADIR/dpe/2b/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/2b/
rm -rf $DATADIR/dpe/2b/2b.csv.zip && rm -rf $DATADIR/dpe/2b/csv && rm -rf $DATADIR/dpe/2b/doc

mkdir $DATADIR/dpe/30/
curl -L https://www.data.gouv.fr/fr/datasets/r/1c912c71-493f-42cd-b306-17cdc9a1dba6 > $DATADIR/dpe/30/30.csv.zip
cd $DATADIR/dpe/30
unzip 30.csv.zip
cp $DATADIR/dpe/30/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/30/
cp $DATADIR/dpe/30/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/30/
rm -rf $DATADIR/dpe/30/30.csv.zip && rm -rf $DATADIR/dpe/30/csv && rm -rf $DATADIR/dpe/30/doc

mkdir $DATADIR/dpe/31/
curl -L https://www.data.gouv.fr/fr/datasets/r/b1157de8-5c76-4543-b01a-30890089fc3c > $DATADIR/dpe/31/31.csv.zip
cd $DATADIR/dpe/31
unzip 31.csv.zip
cp $DATADIR/dpe/31/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/31/
cp $DATADIR/dpe/31/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/31/
rm -rf $DATADIR/dpe/31/31.csv.zip && rm -rf $DATADIR/dpe/31/csv && rm -rf $DATADIR/dpe/31/doc

mkdir $DATADIR/dpe/32/
curl -L https://www.data.gouv.fr/fr/datasets/r/7e85a49b-8370-419c-b0f7-c6c1dd265498 > $DATADIR/dpe/32/32.csv.zip
cd $DATADIR/dpe/32
unzip 32.csv.zip
cp $DATADIR/dpe/32/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/32/
cp $DATADIR/dpe/32/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/32/
rm -rf $DATADIR/dpe/32/32.csv.zip && rm -rf $DATADIR/dpe/32/csv && rm -rf $DATADIR/dpe/32/doc

mkdir $DATADIR/dpe/33/
curl -L https://www.data.gouv.fr/fr/datasets/r/7be71faf-4501-4e28-9367-618e729e217c > $DATADIR/dpe/33/33.csv.zip
cd $DATADIR/dpe/33
unzip 33.csv.zip
cp $DATADIR/dpe/33/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/33/
cp $DATADIR/dpe/33/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/33/
rm -rf $DATADIR/dpe/33/33.csv.zip && rm -rf $DATADIR/dpe/33/csv && rm -rf $DATADIR/dpe/33/doc

mkdir $DATADIR/dpe/34/
curl -L https://www.data.gouv.fr/fr/datasets/r/acde4e43-4c01-41f8-a879-6de2b222665d > $DATADIR/dpe/34/34.csv.zip
cd $DATADIR/dpe/34
unzip 34.csv.zip
cp $DATADIR/dpe/34/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/34/
cp $DATADIR/dpe/34/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/34/
rm -rf $DATADIR/dpe/34/34.csv.zip && rm -rf $DATADIR/dpe/34/csv && rm -rf $DATADIR/dpe/34/doc

mkdir $DATADIR/dpe/35/
curl -L https://www.data.gouv.fr/fr/datasets/r/8027e2f0-0fff-4265-9cbb-f96fd1b0f4d1 > $DATADIR/dpe/35/35.csv.zip
cd $DATADIR/dpe/35
unzip 35.csv.zip
cp $DATADIR/dpe/35/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/35/
cp $DATADIR/dpe/35/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/35/
rm -rf $DATADIR/dpe/35/35.csv.zip && rm -rf $DATADIR/dpe/35/csv && rm -rf $DATADIR/dpe/35/doc

mkdir $DATADIR/dpe/36/
curl -L https://www.data.gouv.fr/fr/datasets/r/61359dea-56d1-48a1-8de8-0e220ff8b049 > $DATADIR/dpe/36/36.csv.zip
cd $DATADIR/dpe/36
unzip 36.csv.zip
cp $DATADIR/dpe/36/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/36/
cp $DATADIR/dpe/36/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/36/
rm -rf $DATADIR/dpe/36/36.csv.zip && rm -rf $DATADIR/dpe/36/csv && rm -rf $DATADIR/dpe/36/doc

mkdir $DATADIR/dpe/37/
curl -L https://www.data.gouv.fr/fr/datasets/r/4909cff2-f73c-44d7-a73b-4625896ef672 > $DATADIR/dpe/37/37.csv.zip
cd $DATADIR/dpe/37
unzip 37.csv.zip
cp $DATADIR/dpe/37/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/37/
cp $DATADIR/dpe/37/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/37/
rm -rf $DATADIR/dpe/37/37.csv.zip && rm -rf $DATADIR/dpe/37/csv && rm -rf $DATADIR/dpe/37/doc

mkdir $DATADIR/dpe/38/
curl -L https://www.data.gouv.fr/fr/datasets/r/ec16ab43-3fec-422f-86c6-df6f085ab1ea > $DATADIR/dpe/38/38.csv.zip
cd $DATADIR/dpe/38
unzip 38.csv.zip
cp $DATADIR/dpe/38/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/38/
cp $DATADIR/dpe/38/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/38/
rm -rf $DATADIR/dpe/38/38.csv.zip && rm -rf $DATADIR/dpe/38/csv && rm -rf $DATADIR/dpe/38/doc

mkdir $DATADIR/dpe/39/
curl -L https://www.data.gouv.fr/fr/datasets/r/c3387398-05a7-47c9-93ff-81a9f6f7507d > $DATADIR/dpe/39/39.csv.zip
cd $DATADIR/dpe/39
unzip 39.csv.zip
cp $DATADIR/dpe/39/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/39/
cp $DATADIR/dpe/39/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/39/
rm -rf $DATADIR/dpe/39/39.csv.zip && rm -rf $DATADIR/dpe/39/csv && rm -rf $DATADIR/dpe/39/doc

mkdir $DATADIR/dpe/40/
curl -L https://www.data.gouv.fr/fr/datasets/r/da787105-dc28-4d37-a146-091849586d4c > $DATADIR/dpe/40/40.csv.zip
cd $DATADIR/dpe/40
unzip 40.csv.zip
cp $DATADIR/dpe/40/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/40/
cp $DATADIR/dpe/40/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/40/
rm -rf $DATADIR/dpe/40/40.csv.zip && rm -rf $DATADIR/dpe/40/csv && rm -rf $DATADIR/dpe/40/doc

mkdir $DATADIR/dpe/41/
curl -L https://www.data.gouv.fr/fr/datasets/r/5a8a20bd-9707-4b64-b7a0-e7c73478b689 > $DATADIR/dpe/41/41.csv.zip
cd $DATADIR/dpe/41
unzip 41.csv.zip
cp $DATADIR/dpe/41/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/41/
cp $DATADIR/dpe/41/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/41/
rm -rf $DATADIR/dpe/41/41.csv.zip && rm -rf $DATADIR/dpe/41/csv && rm -rf $DATADIR/dpe/41/doc

mkdir $DATADIR/dpe/42/
curl -L https://www.data.gouv.fr/fr/datasets/r/81a7b84d-0d1d-42ae-bd10-5dc61df1104d > $DATADIR/dpe/42/42.csv.zip
cd $DATADIR/dpe/42
unzip 42.csv.zip
cp $DATADIR/dpe/42/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/42/
cp $DATADIR/dpe/42/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/42/
rm -rf $DATADIR/dpe/42/42.csv.zip && rm -rf $DATADIR/dpe/42/csv && rm -rf $DATADIR/dpe/42/doc

mkdir $DATADIR/dpe/43/
curl -L https://www.data.gouv.fr/fr/datasets/r/353e0914-763c-4a8f-ae19-765ef7d005a2 > $DATADIR/dpe/43/43.csv.zip
cd $DATADIR/dpe/43
unzip 43.csv.zip
cp $DATADIR/dpe/43/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/43/
cp $DATADIR/dpe/43/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/43/
rm -rf $DATADIR/dpe/43/43.csv.zip && rm -rf $DATADIR/dpe/43/csv && rm -rf $DATADIR/dpe/43/doc

mkdir $DATADIR/dpe/44/
curl -L https://www.data.gouv.fr/fr/datasets/r/5bb464f6-7636-413a-a328-052f6d39028b > $DATADIR/dpe/44/44.csv.zip
cd $DATADIR/dpe/44
unzip 44.csv.zip
cp $DATADIR/dpe/44/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/44/
cp $DATADIR/dpe/44/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/44/
rm -rf $DATADIR/dpe/44/44.csv.zip && rm -rf $DATADIR/dpe/44/csv && rm -rf $DATADIR/dpe/44/doc

mkdir $DATADIR/dpe/45/
curl -L https://www.data.gouv.fr/fr/datasets/r/e59fd48c-2715-478f-ad03-700fdf2785e7 > $DATADIR/dpe/45/45.csv.zip
cd $DATADIR/dpe/45
unzip 45.csv.zip
cp $DATADIR/dpe/45/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/45/
cp $DATADIR/dpe/45/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/45/
rm -rf $DATADIR/dpe/45/45.csv.zip && rm -rf $DATADIR/dpe/45/csv && rm -rf $DATADIR/dpe/45/doc

mkdir $DATADIR/dpe/46/
curl -L https://www.data.gouv.fr/fr/datasets/r/96e2f78a-8ef2-4731-a9ad-f3583f60a243 > $DATADIR/dpe/46/46.csv.zip
cd $DATADIR/dpe/46
unzip 46.csv.zip
cp $DATADIR/dpe/46/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/46/
cp $DATADIR/dpe/46/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/46/
rm -rf $DATADIR/dpe/46/46.csv.zip && rm -rf $DATADIR/dpe/46/csv && rm -rf $DATADIR/dpe/46/doc

mkdir $DATADIR/dpe/47/
curl -L https://www.data.gouv.fr/fr/datasets/r/04e4fb6d-8366-40a6-8dca-38c260b85de9 > $DATADIR/dpe/47/47.csv.zip
cd $DATADIR/dpe/47
unzip 47.csv.zip
cp $DATADIR/dpe/47/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/47/
cp $DATADIR/dpe/47/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/47/
rm -rf $DATADIR/dpe/47/47.csv.zip && rm -rf $DATADIR/dpe/47/csv && rm -rf $DATADIR/dpe/47/doc

mkdir $DATADIR/dpe/48/
curl -L https://www.data.gouv.fr/fr/datasets/r/b5d78c0f-7307-4f25-a8a7-d5023e184117 > $DATADIR/dpe/48/48.csv.zip
cd $DATADIR/dpe/48
unzip 48.csv.zip
cp $DATADIR/dpe/48/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/48/
cp $DATADIR/dpe/48/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/48/
rm -rf $DATADIR/dpe/48/48.csv.zip && rm -rf $DATADIR/dpe/48/csv && rm -rf $DATADIR/dpe/48/doc

mkdir $DATADIR/dpe/49/
curl -L https://www.data.gouv.fr/fr/datasets/r/56218d17-56de-404f-b8a6-2a3553facd8b > $DATADIR/dpe/49/49.csv.zip
cd $DATADIR/dpe/49
unzip 49.csv.zip
cp $DATADIR/dpe/49/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/49/
cp $DATADIR/dpe/49/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/49/
rm -rf $DATADIR/dpe/49/49.csv.zip && rm -rf $DATADIR/dpe/49/csv && rm -rf $DATADIR/dpe/49/doc

mkdir $DATADIR/dpe/50/
curl -L https://www.data.gouv.fr/fr/datasets/r/6ce039ab-4c91-475a-a434-1ddc06890090 > $DATADIR/dpe/50/50.csv.zip
cd $DATADIR/dpe/50
unzip 50.csv.zip
cp $DATADIR/dpe/50/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/50/
cp $DATADIR/dpe/50/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/50/
rm -rf $DATADIR/dpe/50/50.csv.zip && rm -rf $DATADIR/dpe/50/csv && rm -rf $DATADIR/dpe/50/doc

mkdir $DATADIR/dpe/51/
curl -L https://www.data.gouv.fr/fr/datasets/r/32d3884b-a440-4ac7-8b91-2a242e8dd5eb > $DATADIR/dpe/51/51.csv.zip
cd $DATADIR/dpe/51
unzip 51.csv.zip
cp $DATADIR/dpe/51/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/51/
cp $DATADIR/dpe/51/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/51/
rm -rf $DATADIR/dpe/51/51.csv.zip && rm -rf $DATADIR/dpe/51/csv && rm -rf $DATADIR/dpe/51/doc

mkdir $DATADIR/dpe/52/
curl -L https://www.data.gouv.fr/fr/datasets/r/5cf5af62-60c1-4936-b04e-c90db0623bde > $DATADIR/dpe/52/52.csv.zip
cd $DATADIR/dpe/52
unzip 52.csv.zip
cp $DATADIR/dpe/52/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/52/
cp $DATADIR/dpe/52/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/52/
rm -rf $DATADIR/dpe/52/52.csv.zip && rm -rf $DATADIR/dpe/52/csv && rm -rf $DATADIR/dpe/52/doc

mkdir $DATADIR/dpe/53/
curl -L https://www.data.gouv.fr/fr/datasets/r/a12c9404-8bef-4104-8007-0fdfae94103b > $DATADIR/dpe/53/53.csv.zip
cd $DATADIR/dpe/53
unzip 53.csv.zip
cp $DATADIR/dpe/53/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/53/
cp $DATADIR/dpe/53/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/53/
rm -rf $DATADIR/dpe/53/53.csv.zip && rm -rf $DATADIR/dpe/53/csv && rm -rf $DATADIR/dpe/53/doc

mkdir $DATADIR/dpe/54/
curl -L https://www.data.gouv.fr/fr/datasets/r/e68aff93-55b3-4e4d-a225-9657b0a7299a > $DATADIR/dpe/54/54.csv.zip
cd $DATADIR/dpe/54
unzip 54.csv.zip
cp $DATADIR/dpe/54/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/54/
cp $DATADIR/dpe/54/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/54/
rm -rf $DATADIR/dpe/54/54.csv.zip && rm -rf $DATADIR/dpe/54/csv && rm -rf $DATADIR/dpe/54/doc

mkdir $DATADIR/dpe/55/
curl -L https://www.data.gouv.fr/fr/datasets/r/f1ca4e3a-9ad5-494c-8bc7-383f85db31dc > $DATADIR/dpe/55/55.csv.zip
cd $DATADIR/dpe/55
unzip 55.csv.zip
cp $DATADIR/dpe/55/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/55/
cp $DATADIR/dpe/55/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/55/
rm -rf $DATADIR/dpe/55/55.csv.zip && rm -rf $DATADIR/dpe/55/csv && rm -rf $DATADIR/dpe/55/doc

mkdir $DATADIR/dpe/56/
curl -L https://www.data.gouv.fr/fr/datasets/r/c34f9ccf-c7cf-4bad-85c9-b983cedc3fb4 > $DATADIR/dpe/56/56.csv.zip
cd $DATADIR/dpe/56
unzip 56.csv.zip
cp $DATADIR/dpe/56/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/56/
cp $DATADIR/dpe/56/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/56/
rm -rf $DATADIR/dpe/56/56.csv.zip && rm -rf $DATADIR/dpe/56/csv && rm -rf $DATADIR/dpe/56/doc

mkdir $DATADIR/dpe/57/
curl -L https://www.data.gouv.fr/fr/datasets/r/61269a46-c54f-4baa-a768-be9fe88b38e8 > $DATADIR/dpe/57/57.csv.zip
cd $DATADIR/dpe/57
unzip 57.csv.zip
cp $DATADIR/dpe/57/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/57/
cp $DATADIR/dpe/57/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/57/
rm -rf $DATADIR/dpe/57/57.csv.zip && rm -rf $DATADIR/dpe/57/csv && rm -rf $DATADIR/dpe/57/doc

mkdir $DATADIR/dpe/58/
curl -L https://www.data.gouv.fr/fr/datasets/r/55523ae6-0095-4d2c-b9ad-8bef6e6d2982 > $DATADIR/dpe/58/58.csv.zip
cd $DATADIR/dpe/58
unzip 58.csv.zip
cp $DATADIR/dpe/58/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/58/
cp $DATADIR/dpe/58/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/58/
rm -rf $DATADIR/dpe/58/58.csv.zip && rm -rf $DATADIR/dpe/58/csv && rm -rf $DATADIR/dpe/58/doc

mkdir $DATADIR/dpe/59/
curl -L https://www.data.gouv.fr/fr/datasets/r/8a6c43f4-6ebb-448f-8170-e3e1ab4e8f22 > $DATADIR/dpe/59/59.csv.zip
cd $DATADIR/dpe/59
unzip 59.csv.zip
cp $DATADIR/dpe/59/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/59/
cp $DATADIR/dpe/59/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/59/
rm -rf $DATADIR/dpe/59/59.csv.zip && rm -rf $DATADIR/dpe/59/csv && rm -rf $DATADIR/dpe/59/doc

mkdir $DATADIR/dpe/60/
curl -L https://www.data.gouv.fr/fr/datasets/r/226614cd-a60f-4d47-b965-81eff2456415 > $DATADIR/dpe/60/60.csv.zip
cd $DATADIR/dpe/60
unzip 60.csv.zip
cp $DATADIR/dpe/60/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/60/
cp $DATADIR/dpe/60/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/60/
rm -rf $DATADIR/dpe/60/60.csv.zip && rm -rf $DATADIR/dpe/60/csv && rm -rf $DATADIR/dpe/60/doc

mkdir $DATADIR/dpe/61/
curl -L https://www.data.gouv.fr/fr/datasets/r/97888f00-d37b-4057-ba47-170019e521bc > $DATADIR/dpe/61/61.csv.zip
cd $DATADIR/dpe/61
unzip 61.csv.zip
cp $DATADIR/dpe/61/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/61/
cp $DATADIR/dpe/61/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/61/
rm -rf $DATADIR/dpe/61/61.csv.zip && rm -rf $DATADIR/dpe/61/csv && rm -rf $DATADIR/dpe/61/doc

mkdir $DATADIR/dpe/62/
curl -L https://www.data.gouv.fr/fr/datasets/r/0f2af10c-3ba5-416e-b119-3780b202e894 > $DATADIR/dpe/62/62.csv.zip
cd $DATADIR/dpe/62
unzip 62.csv.zip
cp $DATADIR/dpe/62/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/62/
cp $DATADIR/dpe/62/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/62/
rm -rf $DATADIR/dpe/62/62.csv.zip && rm -rf $DATADIR/dpe/62/csv && rm -rf $DATADIR/dpe/62/doc

mkdir $DATADIR/dpe/63/
curl -L https://www.data.gouv.fr/fr/datasets/r/b02e1ad9-203e-4dcb-a3d4-94491a966339 > $DATADIR/dpe/63/63.csv.zip
cd $DATADIR/dpe/63
unzip 63.csv.zip
cp $DATADIR/dpe/63/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/63/
cp $DATADIR/dpe/63/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/63/
rm -rf $DATADIR/dpe/63/63.csv.zip && rm -rf $DATADIR/dpe/63/csv && rm -rf $DATADIR/dpe/63/doc

mkdir $DATADIR/dpe/64/
curl -L https://www.data.gouv.fr/fr/datasets/r/665072f5-e8da-4297-ae47-0b97543ca14d > $DATADIR/dpe/64/64.csv.zip
cd $DATADIR/dpe/64
unzip 64.csv.zip
cp $DATADIR/dpe/64/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/64/
cp $DATADIR/dpe/64/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/64/
rm -rf $DATADIR/dpe/64/64.csv.zip && rm -rf $DATADIR/dpe/64/csv && rm -rf $DATADIR/dpe/64/doc

mkdir $DATADIR/dpe/65/
curl -L https://www.data.gouv.fr/fr/datasets/r/f7cda666-ffed-46e7-a811-e79b9e389222 > $DATADIR/dpe/65/65.csv.zip
cd $DATADIR/dpe/65
unzip 65.csv.zip
cp $DATADIR/dpe/65/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/65/
cp $DATADIR/dpe/65/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/65/
rm -rf $DATADIR/dpe/65/65.csv.zip && rm -rf $DATADIR/dpe/65/csv && rm -rf $DATADIR/dpe/65/doc

mkdir $DATADIR/dpe/66/
curl -L https://www.data.gouv.fr/fr/datasets/r/54b9b3fc-7613-46e8-85a5-627385a139f7 > $DATADIR/dpe/66/66.csv.zip
cd $DATADIR/dpe/66
unzip 66.csv.zip
cp $DATADIR/dpe/66/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/66/
cp $DATADIR/dpe/66/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/66/
rm -rf $DATADIR/dpe/66/66.csv.zip && rm -rf $DATADIR/dpe/66/csv && rm -rf $DATADIR/dpe/66/doc

mkdir $DATADIR/dpe/67/
curl -L https://www.data.gouv.fr/fr/datasets/r/b0598222-e607-4ade-bda6-78cd6dd50859 > $DATADIR/dpe/67/67.csv.zip
cd $DATADIR/dpe/67
unzip 67.csv.zip
cp $DATADIR/dpe/67/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/67/
cp $DATADIR/dpe/67/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/67/
rm -rf $DATADIR/dpe/67/67.csv.zip && rm -rf $DATADIR/dpe/67/csv && rm -rf $DATADIR/dpe/67/doc

mkdir $DATADIR/dpe/68/
curl -L https://www.data.gouv.fr/fr/datasets/r/0f032788-1156-47ac-83ae-1e6b349fedac > $DATADIR/dpe/68/68.csv.zip
cd $DATADIR/dpe/68
unzip 68.csv.zip
cp $DATADIR/dpe/68/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/68/
cp $DATADIR/dpe/68/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/68/
rm -rf $DATADIR/dpe/68/68.csv.zip && rm -rf $DATADIR/dpe/68/csv && rm -rf $DATADIR/dpe/68/doc

mkdir $DATADIR/dpe/69/
curl -L https://www.data.gouv.fr/fr/datasets/r/9ba46536-7dfb-495c-87f8-ffd4b63685b7 > $DATADIR/dpe/69/69.csv.zip
cd $DATADIR/dpe/69
unzip 69.csv.zip
cp $DATADIR/dpe/69/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/69/
cp $DATADIR/dpe/69/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/69/
rm -rf $DATADIR/dpe/69/69.csv.zip && rm -rf $DATADIR/dpe/69/csv && rm -rf $DATADIR/dpe/69/doc

mkdir $DATADIR/dpe/70/
curl -L https://www.data.gouv.fr/fr/datasets/r/23fdf417-96b0-44cf-bb41-ebc502fe0cbf > $DATADIR/dpe/70/70.csv.zip
cd $DATADIR/dpe/70
unzip 70.csv.zip
cp $DATADIR/dpe/70/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/70/
cp $DATADIR/dpe/70/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/70/
rm -rf $DATADIR/dpe/70/70.csv.zip && rm -rf $DATADIR/dpe/70/csv && rm -rf $DATADIR/dpe/70/doc

mkdir $DATADIR/dpe/71/
curl -L https://www.data.gouv.fr/fr/datasets/r/97182400-abb0-462d-90ee-ae237ab0bd3a > $DATADIR/dpe/71/71.csv.zip
cd $DATADIR/dpe/71
unzip 71.csv.zip
cp $DATADIR/dpe/71/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/71/
cp $DATADIR/dpe/71/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/71/
rm -rf $DATADIR/dpe/71/71.csv.zip && rm -rf $DATADIR/dpe/71/csv && rm -rf $DATADIR/dpe/71/doc

mkdir $DATADIR/dpe/72/
curl -L https://www.data.gouv.fr/fr/datasets/r/a2052831-0447-44b5-bc6f-84672f4901e4 > $DATADIR/dpe/72/72.csv.zip
cd $DATADIR/dpe/72
unzip 72.csv.zip
cp $DATADIR/dpe/72/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/72/
cp $DATADIR/dpe/72/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/72/
rm -rf $DATADIR/dpe/72/72.csv.zip && rm -rf $DATADIR/dpe/72/csv && rm -rf $DATADIR/dpe/72/doc

mkdir $DATADIR/dpe/73/
curl -L https://www.data.gouv.fr/fr/datasets/r/c68ad0be-41f0-4fc1-a4f7-9380253725ca > $DATADIR/dpe/73/73.csv.zip
cd $DATADIR/dpe/73
unzip 73.csv.zip
cp $DATADIR/dpe/73/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/73/
cp $DATADIR/dpe/73/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/73/
rm -rf $DATADIR/dpe/73/73.csv.zip && rm -rf $DATADIR/dpe/73/csv && rm -rf $DATADIR/dpe/73/doc

mkdir $DATADIR/dpe/74/
curl -L https://www.data.gouv.fr/fr/datasets/r/2cef3ebb-9d0b-4f78-b674-503982aa56d8 > $DATADIR/dpe/74/74.csv.zip
cd $DATADIR/dpe/74
unzip 74.csv.zip
cp $DATADIR/dpe/74/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/74/
cp $DATADIR/dpe/74/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/74/
rm -rf $DATADIR/dpe/74/74.csv.zip && rm -rf $DATADIR/dpe/74/csv && rm -rf $DATADIR/dpe/74/doc

mkdir $DATADIR/dpe/75/
curl -L https://www.data.gouv.fr/fr/datasets/r/fdaa9020-fce2-458d-826e-8689a2989e4c > $DATADIR/dpe/75/75.csv.zip
cd $DATADIR/dpe/75
unzip 75.csv.zip
cp $DATADIR/dpe/75/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/75/
cp $DATADIR/dpe/75/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/75/
rm -rf $DATADIR/dpe/75/75.csv.zip && rm -rf $DATADIR/dpe/75/csv && rm -rf $DATADIR/dpe/75/doc

mkdir $DATADIR/dpe/76/
curl -L https://www.data.gouv.fr/fr/datasets/r/4bb3ec41-6a18-4146-ab9e-2f776966e812 > $DATADIR/dpe/76/76.csv.zip
cd $DATADIR/dpe/76
unzip 76.csv.zip
cp $DATADIR/dpe/76/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/76/
cp $DATADIR/dpe/76/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/76/
rm -rf $DATADIR/dpe/76/76.csv.zip && rm -rf $DATADIR/dpe/76/csv && rm -rf $DATADIR/dpe/76/doc

mkdir $DATADIR/dpe/77/
curl -L https://www.data.gouv.fr/fr/datasets/r/bc6c6a6f-f2a9-4480-b517-90520677d2dc > $DATADIR/dpe/77/77.csv.zip
cd $DATADIR/dpe/77
unzip 77.csv.zip
cp $DATADIR/dpe/77/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/77/
cp $DATADIR/dpe/77/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/77/
rm -rf $DATADIR/dpe/77/77.csv.zip && rm -rf $DATADIR/dpe/77/csv && rm -rf $DATADIR/dpe/77/doc

mkdir $DATADIR/dpe/78/
curl -L https://www.data.gouv.fr/fr/datasets/r/5a263677-06da-42f0-a833-ee4bc6dfb24c > $DATADIR/dpe/78/78.csv.zip
cd $DATADIR/dpe/78
unzip 78.csv.zip
cp $DATADIR/dpe/78/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/78/
cp $DATADIR/dpe/78/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/78/
rm -rf $DATADIR/dpe/78/78.csv.zip && rm -rf $DATADIR/dpe/78/csv && rm -rf $DATADIR/dpe/78/doc

mkdir $DATADIR/dpe/79/
curl -L https://www.data.gouv.fr/fr/datasets/r/65cde3db-5f95-4222-b415-9eb46fc5e192 > $DATADIR/dpe/79/79.csv.zip
cd $DATADIR/dpe/79
unzip 79.csv.zip
cp $DATADIR/dpe/79/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/79/
cp $DATADIR/dpe/79/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/79/
rm -rf $DATADIR/dpe/79/79.csv.zip && rm -rf $DATADIR/dpe/79/csv && rm -rf $DATADIR/dpe/79/doc

mkdir $DATADIR/dpe/80/
curl -L https://www.data.gouv.fr/fr/datasets/r/91851f5e-ead8-43a5-b20b-4db7f7eb065a > $DATADIR/dpe/80/80.csv.zip
cd $DATADIR/dpe/80
unzip 80.csv.zip
cp $DATADIR/dpe/80/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/80/
cp $DATADIR/dpe/80/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/80/
rm -rf $DATADIR/dpe/80/80.csv.zip && rm -rf $DATADIR/dpe/80/csv && rm -rf $DATADIR/dpe/80/doc

mkdir $DATADIR/dpe/81/
curl -L https://www.data.gouv.fr/fr/datasets/r/de7ddbd4-d240-4dad-a179-648770d9449e > $DATADIR/dpe/81/81.csv.zip
cd $DATADIR/dpe/81
unzip 81.csv.zip
cp $DATADIR/dpe/81/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/81/
cp $DATADIR/dpe/81/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/81/
rm -rf $DATADIR/dpe/81/81.csv.zip && rm -rf $DATADIR/dpe/81/csv && rm -rf $DATADIR/dpe/81/doc

mkdir $DATADIR/dpe/82/
curl -L https://www.data.gouv.fr/fr/datasets/r/f6b87236-4b0f-406e-8545-64fc6072d52e > $DATADIR/dpe/82/82.csv.zip
cd $DATADIR/dpe/82
unzip 82.csv.zip
cp $DATADIR/dpe/82/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/82/
cp $DATADIR/dpe/82/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/82/
rm -rf $DATADIR/dpe/82/82.csv.zip && rm -rf $DATADIR/dpe/82/csv && rm -rf $DATADIR/dpe/82/doc

mkdir $DATADIR/dpe/83/
curl -L https://www.data.gouv.fr/fr/datasets/r/cc915c92-50d9-4f28-9b51-890b9e195b3f > $DATADIR/dpe/83/83.csv.zip
cd $DATADIR/dpe/83
unzip 83.csv.zip
cp $DATADIR/dpe/83/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/83/
cp $DATADIR/dpe/83/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/83/
rm -rf $DATADIR/dpe/83/83.csv.zip && rm -rf $DATADIR/dpe/83/csv && rm -rf $DATADIR/dpe/83/doc

mkdir $DATADIR/dpe/84/
curl -L https://www.data.gouv.fr/fr/datasets/r/6ba14520-d1ed-4e57-a1ea-cd27736a8a64 > $DATADIR/dpe/84/84.csv.zip
cd $DATADIR/dpe/84
unzip 84.csv.zip
cp $DATADIR/dpe/84/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/84/
cp $DATADIR/dpe/84/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/84/
rm -rf $DATADIR/dpe/84/84.csv.zip && rm -rf $DATADIR/dpe/84/csv && rm -rf $DATADIR/dpe/84/doc

mkdir $DATADIR/dpe/85/
curl -L https://www.data.gouv.fr/fr/datasets/r/8e6c77b4-57c3-4019-bd27-a5b5c0d4a0e0 > $DATADIR/dpe/85/85.csv.zip
cd $DATADIR/dpe/85
unzip 85.csv.zip
cp $DATADIR/dpe/85/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/85/
cp $DATADIR/dpe/85/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/85/
rm -rf $DATADIR/dpe/85/85.csv.zip && rm -rf $DATADIR/dpe/85/csv && rm -rf $DATADIR/dpe/85/doc

mkdir $DATADIR/dpe/86/
curl -L https://www.data.gouv.fr/fr/datasets/r/e9785283-131f-4f0d-a639-4e1f22140dfd > $DATADIR/dpe/86/86.csv.zip
cd $DATADIR/dpe/86
unzip 86.csv.zip
cp $DATADIR/dpe/86/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/86/
cp $DATADIR/dpe/86/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/86/
rm -rf $DATADIR/dpe/86/86.csv.zip && rm -rf $DATADIR/dpe/86/csv && rm -rf $DATADIR/dpe/86/doc

mkdir $DATADIR/dpe/87/
curl -L https://www.data.gouv.fr/fr/datasets/r/6889d56d-2378-4463-9e51-e00154f4894e > $DATADIR/dpe/87/87.csv.zip
cd $DATADIR/dpe/87
unzip 87.csv.zip
cp $DATADIR/dpe/87/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/87/
cp $DATADIR/dpe/87/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/87/
rm -rf $DATADIR/dpe/87/87.csv.zip && rm -rf $DATADIR/dpe/87/csv && rm -rf $DATADIR/dpe/87/doc

mkdir $DATADIR/dpe/88/
curl -L https://www.data.gouv.fr/fr/datasets/r/566b596e-a7eb-456e-be3d-58c3385756ba > $DATADIR/dpe/88/88.csv.zip
cd $DATADIR/dpe/88
unzip 88.csv.zip
cp $DATADIR/dpe/88/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/88/
cp $DATADIR/dpe/88/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/88/
rm -rf $DATADIR/dpe/88/88.csv.zip && rm -rf $DATADIR/dpe/88/csv && rm -rf $DATADIR/dpe/88/doc

mkdir $DATADIR/dpe/89/
curl -L https://www.data.gouv.fr/fr/datasets/r/d7054ff6-8c2a-42eb-b90e-88efa89d7784 > $DATADIR/dpe/89/89.csv.zip
cd $DATADIR/dpe/89
unzip 89.csv.zip
cp $DATADIR/dpe/89/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/89/
cp $DATADIR/dpe/89/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/89/
rm -rf $DATADIR/dpe/89/89.csv.zip && rm -rf $DATADIR/dpe/89/csv && rm -rf $DATADIR/dpe/89/doc

mkdir $DATADIR/dpe/90/
curl -L https://www.data.gouv.fr/fr/datasets/r/7ea1d2ac-eb63-4614-bbf5-0ffb32493516 > $DATADIR/dpe/90/90.csv.zip
cd $DATADIR/dpe/90
unzip 90.csv.zip
cp $DATADIR/dpe/90/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/90/
cp $DATADIR/dpe/90/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/90/
rm -rf $DATADIR/dpe/90/90.csv.zip && rm -rf $DATADIR/dpe/90/csv && rm -rf $DATADIR/dpe/90/doc

mkdir $DATADIR/dpe/91/
curl -L https://www.data.gouv.fr/fr/datasets/r/f33f4fca-0f3e-425b-a9d7-40798ca98e0c > $DATADIR/dpe/91/91.csv.zip
cd $DATADIR/dpe/91
unzip 91.csv.zip
cp $DATADIR/dpe/91/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/91/
cp $DATADIR/dpe/91/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/91/
rm -rf $DATADIR/dpe/91/91.csv.zip && rm -rf $DATADIR/dpe/91/csv && rm -rf $DATADIR/dpe/91/doc

mkdir $DATADIR/dpe/92/
curl -L https://www.data.gouv.fr/fr/datasets/r/580288da-b8db-4c51-8374-bb4d2ab42d09 > $DATADIR/dpe/92/92.csv.zip
cd $DATADIR/dpe/92
unzip 92.csv.zip
cp $DATADIR/dpe/92/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/92/
cp $DATADIR/dpe/92/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/92/
rm -rf $DATADIR/dpe/92/92.csv.zip && rm -rf $DATADIR/dpe/92/csv && rm -rf $DATADIR/dpe/92/doc

mkdir $DATADIR/dpe/93/
curl -L https://www.data.gouv.fr/fr/datasets/r/5ad27b23-5b97-4c4d-9cdc-e2e07ede2f14 > $DATADIR/dpe/93/93.csv.zip
cd $DATADIR/dpe/93
unzip 93.csv.zip
cp $DATADIR/dpe/93/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/93/
cp $DATADIR/dpe/93/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/93/
rm -rf $DATADIR/dpe/93/93.csv.zip && rm -rf $DATADIR/dpe/93/csv && rm -rf $DATADIR/dpe/93/doc

mkdir $DATADIR/dpe/94/
curl -L https://www.data.gouv.fr/fr/datasets/r/189a9069-2dcd-402c-ab27-628c3b4d99ab > $DATADIR/dpe/94/94.csv.zip
cd $DATADIR/dpe/94
unzip 94.csv.zip
cp $DATADIR/dpe/94/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/94/
cp $DATADIR/dpe/94/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/94/
rm -rf $DATADIR/dpe/94/94.csv.zip && rm -rf $DATADIR/dpe/94/csv && rm -rf $DATADIR/dpe/94/doc

mkdir $DATADIR/dpe/95/
curl -L https://www.data.gouv.fr/fr/datasets/r/338ac4f4-67ef-42b1-ab41-20175160f481 > $DATADIR/dpe/95/95.csv.zip
cd $DATADIR/dpe/95
unzip 95.csv.zip
cp $DATADIR/dpe/95/csv/rel_batiment_groupe_parcelle.csv $DATADIR/dpe/95/
cp $DATADIR/dpe/95/csv/batiment_groupe_dpe_representatif_logement.csv $DATADIR/dpe/95/
rm -rf $DATADIR/dpe/95/95.csv.zip && rm -rf $DATADIR/dpe/95/csv && rm -rf $DATADIR/dpe/95/doc
