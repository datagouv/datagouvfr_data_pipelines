import io
import logging
import gc

from time import monotonic
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from datagouvfr_data_pipelines.utils.s3 import S3Client


class S3RangeFile(io.RawIOBase):
    """Minimal seekable reader over an S3 object, so that pyarrow can read the parquet
    footer and then stream row groups with range requests.
    We don't use pyarrow's own S3FileSystem because its AWS SDK sends an
    `x-amz-checksum-mode` header that OVH rejects (AWS Error 134 on GetObject),
    so we go through the boto3 client held by S3Client."""

    def __init__(self, s3_client: S3Client, bucket: str, key: str):
        self._client = s3_client.client
        self._bucket = bucket
        self._key = key
        self.size = self._client.head_object(Bucket=bucket, Key=key)["ContentLength"]
        self._pos = 0

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self._pos = offset
        elif whence == io.SEEK_CUR:
            self._pos += offset
        else:
            self._pos = self.size + offset
        return self._pos

    def tell(self) -> int:
        return self._pos

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = self.size - self._pos
        if size <= 0:
            return b""
        end = min(self._pos + size, self.size) - 1
        body = self._client.get_object(
            Bucket=self._bucket, Key=self._key, Range=f"bytes={self._pos}-{end}"
        )["Body"].read()
        self._pos += len(body)
        return body


def match_year_to_snapshots(year, available_dates) -> list[str]:
    """Match year to available snapshots dates.
    Returns a list of ISO str dates ["YYYY-MM-DD", ...]"""
    restr_available_dates = [
        max(
            k for k in available_dates.keys() if k.startswith(f"{int(year) - 1}")
        )  # take last snapshot of the prior year, e.g. "2022-10-01"
    ] + sorted(
        [k for k in available_dates.keys() if k.startswith(year)]
    )  # all snapshots of the current year
    if restr_available_dates[-1] < f"{year}-12-31":
        restr_available_dates.append(
            f"{year}-12-31"
        )  # closing bound of the current year, not a snapshot
    return restr_available_dates


def enrich_parcelles_with_snapshot_coord(
    dvf_batch_df: pd.DataFrame,
    snapshot_file: str,
    s3_client: S3Client,
    bucket: str,
) -> pd.DataFrame:
    """Match parcelle's lat, long coordinates for a snapshot file to the DVF batch,
    relying on parcelle ID.
    Note: snapshot parquet files are produced by Thomas G.
    """
    logging.info(f"Opening {snapshot_file}...")
    parcelle_ids = pa.array(dvf_batch_df["id_parcelle"].unique(), type=pa.string())

    # Open just once the file to accelerate time
    coord_pf = pq.ParquetFile(
        S3RangeFile(s3_client, bucket, snapshot_file), pre_buffer=True
    )
    logging.info(
        f"> {coord_pf.metadata.num_rows:,} parcelles referenced in the current cadastre snapshot file, sliced in {coord_pf.metadata.num_row_groups} row groups"
    )
    coord_to_keep_chunks = []
    nb_wanted = len(parcelle_ids)  # distinct parcelles we need coordinates for
    nb_scanned = nb_found = 0
    started = monotonic()
    logging.info(
        f"> {nb_wanted} unique parcelles to look up for all mutations between the current cadastre snapshot dates"
    )
    logging.info(f"Starting lookup on {snapshot_file}")
    for i, coord_batch in enumerate(
        coord_pf.iter_batches(
            batch_size=1_000_000,
            columns=[
                "id",
                "longitude",
                "latitude",
            ],  # order matters: it is the output's
        )
    ):
        nb_scanned += coord_batch.num_rows
        # filtering in arrow: to_pandas() here would build a python str for each of the
        # file's ~94M ids, only to discard the ~99.5% we don't need
        coord_to_keep = coord_batch.filter(
            pc.is_in(coord_batch.column("id"), value_set=parcelle_ids)
        )
        if coord_to_keep.num_rows:
            coord_to_keep_chunks.append(coord_to_keep)
            nb_found += coord_to_keep.num_rows
        del coord_batch, coord_to_keep
        if i % 20 == 0:
            # the whole snapshot has to be scanned: its ids are not sorted, so no row group can be skipped
            eta = (monotonic() - started) * (
                coord_pf.metadata.num_rows / nb_scanned - 1
            )
            logging.info(
                f"> {nb_scanned / coord_pf.metadata.num_rows:.0%} of the snapshot scanned"
                f" - {nb_found:,}/{nb_wanted:,} parcelles found ({nb_found / nb_wanted:.0%})"
                f" - ~{eta / 60:.0f} min left"
            )

    del parcelle_ids
    if not coord_to_keep_chunks:
        raise ValueError(
            f"Lookup over but no matching parcelles ID between DVF batch and coordinates snapshot {snapshot_file}"
        )
    # only the matching rows are converted to pandas
    coord_to_keep_df = (
        pa.Table.from_batches(coord_to_keep_chunks)
        .to_pandas()
        .rename({"id": "id_parcelle"}, axis=1)
    )
    logging.info(f"Lookup over: {len(coord_to_keep_df)} unique parcelles found")
    assert (
        coord_to_keep_df["id_parcelle"].is_unique
    )  # if id is NOT unique in the parquet file, duplicates would appear in the join
    del coord_to_keep_chunks
    gc.collect()

    enriched_batch_df = pd.merge(
        dvf_batch_df, coord_to_keep_df, on="id_parcelle", how="left"
    )
    del coord_to_keep_df
    logging.info(
        f"{round(len(enriched_batch_df.loc[enriched_batch_df['latitude'].isna()]) / len(enriched_batch_df) * 100, 2)}% missing"
    )  # expecting around 1-2% missing coords
    return enriched_batch_df


def enrich_parcelles_with_coord(
    dvf_df: pd.DataFrame,
    year: str,
    available_dates: dict[str, str],
    s3_client: S3Client,
    bucket: str,
):
    """Enrich DVF dataframe with parcelle's lat, long coordinates.
    Those parcelle coordinates can be defined at a slightly different timeframe than the mutation,
    so looping through each available snapshot of parcelle coordinates.
    """
    restr_available_dates = match_year_to_snapshots(year, available_dates)
    logging.info(restr_available_dates)
    geoloced = []
    remainders = None
    for k in range(len(restr_available_dates) - 1):
        dmin, dmax = (
            restr_available_dates[k],
            restr_available_dates[k + 1],
        )  # lower and upper date bounds for current snapshot
        matching_mutations_df = dvf_df.loc[
            dvf_df["date_mutation"].between(
                dmin, dmax, inclusive="both" if dmax == f"{year}-12-31" else "left"
            )
        ]  # the rows between the snapshot date bounds to process
        dvf_df.drop(
            matching_mutations_df.index, inplace=True
        )  # dropping them from original df to keep RAM low
        logging.info(f"{len(matching_mutations_df)} rows between {dmin} and {dmax}")
        if remainders is not None and not remainders.empty:
            # for parcelles that didn't get geolocalized in the expected batch, we'll try again at each upcoming batch
            logging.info(f"- adding {len(remainders)} remainders")
            matching_mutations_df = pd.concat(
                [matching_mutations_df, remainders], ignore_index=True
            )
        if len(matching_mutations_df) == 0:
            logging.info("> skipping")
            continue
        # TODO: sorting below preserve row order from prior implem. Need to verify if necessary otherwise can be removed
        matching_mutations_df = matching_mutations_df.sort_values(
            by="id_parcelle", key=lambda s: s.str[:3], kind="stable", ignore_index=True
        )  # sort by 2 digits department + 1st digit of the commune code like "380"
        enriched = enrich_parcelles_with_snapshot_coord(
            matching_mutations_df, available_dates[dmin], s3_client, bucket
        )
        remainders = enriched.loc[enriched["longitude"].isna()][
            [c for c in enriched.columns if c not in ["latitude", "longitude"]]
        ]
        geoloced.append(enriched.dropna(subset="longitude"))
        del enriched
        gc.collect()
    logging.info("Done with geoloc, concatenating results...")
    if remainders is not None:
        geoloced.append(remainders)
    del remainders

    # using pd.concat on geoloced directly is too RAM heavy, so workaround
    enriched_df = pd.DataFrame()
    while geoloced:
        logging.info(f"> {len(geoloced)} dfs still to concatenate")
        enriched_df = pd.concat([enriched_df, geoloced[0]], ignore_index=True)
        del geoloced[0]
        gc.collect()
    del geoloced
    return enriched_df
