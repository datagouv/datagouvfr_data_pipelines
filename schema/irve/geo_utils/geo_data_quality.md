# Geo data quality checks in consolidation

A number of data quality improvement steps are now carried out on geographical data while consolidating datasets.

So far, these checks are only applied to consolidated files that comply with an IRVE schema.

Here are the steps we run:

- Fix [x,y] coordinates order to ensure longitude comes before latitude. This is done by checking if [y,x] is located in France. If so, the coordinates column is modified accordingly and the `consolidated_coordinates_reordered` column entry is `True`.
- `consolidated_longitude` and `consolidated_latitude` columns are created by parsing the coordinates column.
- The function `fix_code_insee` implements a number of steps to check if the code INSEE is correct or can be fixed and enrich the geographical data where possible with `consolidated_code_postal` and `consolidated_commune`. This function also creates other fields, namely:
	- `consolidated_is_code_insee_verified` which is `True` if the final code INSEE after fix matches the coordinates or has a postcode which is present in the address field.
	- `consolidated_is_lon_lat_correct` which is `True` if the code INSEE field matches either the code INSEE of the commune where the coordinates point or one of the postcodes of that commune.
- Export consolidated CSV file to GeoJSON format.