#!/bin/bash

# Common parameters
format="csv"
header="true"
size="10000"
DATADIR="$1"

# Define the datasets and their corresponding URLs
datasets=(
  "dpe-v2-logements-existants|https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines"
  "dpe-france|https://data.ademe.fr/data-fair/api/v1/datasets/dpe-france"
)

# Loop through the datasets
for dataset in "${datasets[@]}"; do
  # Extract the dataset name and URL
  dataset_name="${dataset%%|*}"
  dataset_url="${dataset#*|}"
  dataset_file="$DATADIR/$dataset_name.csv"
  
  # Loop to download all pages
  page_number=0
  while true; do
    # Construct the URL with pagination parameters
    url="${dataset_url}?size=${size}&format=${format}&after=$((page_number * size))&header=${header}"
    
    # Download the page and append it to the dataset file
    temp_file="$(mktemp)"
    curl -o "$temp_file" "$url"
    
    # Check if the page is empty (indicating the end of the dataset)
    if [ ! -s "$temp_file" ]; then
      echo "Download of $dataset_name complete."
      break
    fi
    
    # Append the page to the dataset file
    cat "$temp_file" >> "$dataset_file"
    
    # Cleanup the temporary file
    rm "$temp_file"
    
    # Increment the page number for the next iteration
    page_number=$((page_number + 1))
  done
done

cd $DATADIR && ls -lh
