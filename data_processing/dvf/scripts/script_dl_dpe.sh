size="10000"
DATADIR="$1"
content_size_threshold="100"  # Specify the threshold in bytes

# Define the datasets and their corresponding URLs
datasets=(
  "old_dpe|https://data.ademe.fr/data-fair/api/v1/datasets/dpe-france/lines"
  "new_dpe|https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines"
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
    if ((page_number == 0))
    then
      url="${dataset_url}?size=${size}&format=csv&after=$((page_number * size))&header=true"
    else
      url="${dataset_url}?size=${size}&format=csv&after=$((page_number * size))&header=false"
    fi

    # Download the page and store the content in a variable
    content=$(curl -s "$url")
    
    # Check the size of the downloaded content
    content_size=$(echo -n "$content" | wc -c)
    if ((content_size < content_size_threshold)); then
      echo "Download of $dataset_name complete."
      break
    fi
    
    # Print the download progress message
    echo "Downloading page $((page_number + 1)) for $dataset_name..."
    
    echo "$content" >> "$dataset_file"
    
    # Increment the page number for the next iteration
    ((page_number++))
  done
done

cd $DATADIR && ls -lh
