DATADIR="$1"
rm -rf $DATADIR
mkdir -p $DATADIR

dictionary=(
  "2022_pres_t1|https://www.data.gouv.fr/fr/datasets/r/79b5cac4-4957-486b-bbda-322d80868224"
  "2022_pres_t2|https://www.data.gouv.fr/fr/datasets/r/4dfd05a9-094e-4043-8a19-43b6b6bbe086"
  "2022_legi_t1|https://www.data.gouv.fr/fr/datasets/r/a1f73b85-8194-44f4-a2b7-c343edb47d32"
  "2022_legi_t2|https://www.data.gouv.fr/fr/datasets/r/96ffddda-59b4-41b8-a6a3-dfe1adb7fa36"
  "2021_dpmt_t1|https://www.data.gouv.fr/fr/datasets/r/57370a9b-7fa1-465c-a051-c984fc21321f"
  "2021_dpmt_t2|https://www.data.gouv.fr/fr/datasets/r/b34b36d9-e416-4144-8384-d101b140afaf"
  "2020_muni_t2|https://www.data.gouv.fr/fr/datasets/r/7e641d2e-e017-43d4-9434-49d5acd44b4b"
  "2019_euro_t1|https://www.data.gouv.fr/fr/datasets/r/77c4450b-7fa7-425c-84da-4f7bf4b97820"
  "2017_pres_t1|https://www.data.gouv.fr/fr/datasets/r/8fdb0926-ea9d-4fb4-a136-7767cd97e30b"
  "2017_pres_t2|https://www.data.gouv.fr/fr/datasets/r/2e3e44de-e584-4aa2-8148-670daf5617e1"
  "2017_legi_t1|https://www.data.gouv.fr/fr/datasets/r/80cb1309-9147-4bae-b6e2-79877d549b50"
  "2017_legi_t2|https://www.data.gouv.fr/fr/datasets/r/8eb61f3e-dfdf-496e-85af-2859cd7383c3"
)

# Iterate over the dictionary
for item in "${dictionary[@]}"; do
  # Split the string into ID and URL using IFS
  IFS='|' read -ra parts <<< "$item"
  id="${parts[0]}"
  url="${parts[1]}"
  
  # Download the file using curl
  echo "Downloading $id..."
  curl -o "$DATADIR/$id.txt" "$url" -L
done

cd $DATADIR && ls