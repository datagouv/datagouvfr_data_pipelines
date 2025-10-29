import gzip
import logging

import duckdb
from geoparquet_io.core.add_bbox_metadata import add_bbox_metadata

from datagouvfr_data_pipelines.config import AIRFLOW_ENV


def csv_to_parquet(
    csv_file_path: str,
    dtype: dict | None = None,
    columns: list | None = None,
    output_name: str | None = None,
    output_path: str | None = None,
    sep: str = ";",
    compression: str = "zstd",
    **kwargs,
) -> str:
    """
    if dtype is not specified, columns are required to load everything as string (for safety)
    for allowed types see https://duckdb.org/docs/sql/data_types/overview.html
    """
    assert dtype is not None or columns is not None
    if output_name is None:
        output_name = csv_file_path.split("/")[-1].replace(".csv", ".parquet")
    if output_path is None:
        output_path = "/".join(csv_file_path.split("/")[:-1]) + "/"
    logging.info(f"Converting {csv_file_path}")
    db = duckdb.read_csv(
        csv_file_path,
        sep=sep,
        dtype=dtype or {c: "VARCHAR" for c in columns},
        **kwargs,
    )
    logging.info(f"to {output_path + output_name}")
    db.write_parquet(output_path + output_name, compression=compression)
    return output_path + output_name


def csv_to_csvgz(
    csv_file_path: str,
    output_name: str | None = None,
    output_path: str | None = None,
    chunk_size: int = 1024 * 1024,
) -> str:
    if output_name is None:
        output_name = csv_file_path.split("/")[-1].replace(".csv", ".csv.gz")
    if output_path is None:
        output_path = "/".join(csv_file_path.split("/")[:-1]) + "/"
    logging.info(f"Converting {csv_file_path}")
    logging.info(f"to {output_path + output_name}")
    with (
        open(csv_file_path, "r", newline="", encoding="utf-8") as csvfile,
        gzip.open(
            output_path + output_name, "wt", newline="", encoding="utf-8"
        ) as gzfile,
    ):
        while True:
            chunk = csvfile.read(chunk_size)
            if not chunk:
                break
            gzfile.write(chunk)
    return output_path + output_name


def csv_to_geoparquet(
    csv_file_path: str,
    dtype: dict[str, str],
    output_name: str | None = None,
    output_path: str | None = None,
    sep: str = ";",
    quote: str = '"',
    parquet_compression: str = "zstd",
    compression_level: int = 15,
    row_group_size: int = 20000,
    longitude_col: str = "longitude",
    latitude_col: str = "latitude",
) -> str:
    if output_name is None:
        output_name = csv_file_path.split("/")[-1].replace(".csv", ".parquet")
    if output_path is None:
        output_path = "/".join(csv_file_path.split("/")[:-1]) + "/"
    geoparquet_query = """COPY (
        WITH data AS (
            SELECT *, ST_Point({long}, {lat}) AS geometry, STRUCT_PACK(
                xmin := ST_XMin(ST_Point({long}, {lat})),
                ymin := ST_YMin(ST_Point({long}, {lat})),
                xmax := ST_XMax(ST_Point({long}, {lat})),
                ymax := ST_YMax(ST_Point({long}, {lat}))
            ) AS bbox
            FROM read_csv(
                '{csv_file_path}',
                delim = '{sep}',
                quote = '{quote}',
                header = true,
                columns = {dtype}
            )
        ),
        bbox AS (
            SELECT ST_Extent(ST_Extent_Agg(geometry))::BOX_2D AS b
            FROM data
        )
        SELECT t.*
        FROM data AS t
        CROSS JOIN bbox
        ORDER BY ST_Hilbert(t.geometry, bbox.b)
    ) TO '{output_name}'
    (FORMAT 'parquet', COMPRESSION '{compression}', COMPRESSION_LEVEL {compression_level}, ROW_GROUP_SIZE '{row_group_size}')"""
    query = geoparquet_query.format(
        **{
            "csv_file_path": csv_file_path,
            "dtype": dtype,
            "output_name": output_path + output_name,
            "sep": sep,
            "quote": quote,
            "compression": parquet_compression,
            "compression_level": compression_level,
            "row_group_size": row_group_size,
            "long": longitude_col,
            "lat": latitude_col,
        }
    )

    queries = [
        "INSTALL spatial;",
        "LOAD spatial;",
        "SET preserve_insertion_order=false;",
        "SET threads TO 2;",
        "SET memory_limit = '8GB';",
        "SET max_temp_directory_size = '125GB';",
        query,
    ]

    logging.info(f"Converting {csv_file_path}")
    logging.info(f"to {output_path + output_name}")
    with duckdb.connect() as con:
        for query in queries:
            logging.info(query)
            con.sql(query)

    add_bbox_metadata(output_path + output_name, verbose=False)
    return output_path + output_name


