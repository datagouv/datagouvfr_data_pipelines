import logging
import gc
import re
import os
from time import sleep
from functools import lru_cache
from time import monotonic
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import shapely

from pyproj import Transformer

WGS84 = "EPSG:4326"
# how many ambiguous points to detail in the logs before only counting them
AMBIGUOUS_SAMPLE = 20
# INSEE COG, "commune depuis 1943". The numeric segment is the COG edition and changes with
# each yearly release: check https://www.insee.fr/fr/information/8377162 when it 404s.
INSEE_COMMUNES_URL = (
    "https://www.insee.fr/fr/statistiques/fichier/8377162/v_commune_depuis_1943.csv"
)
# Paris, Lyon and Marseille: DVF carries the arrondissement, so the parent commune must not
# shadow it in the lookup
PLM_PARENTS = ("75056", "69123", "13055")


@lru_cache(maxsize=None)
def _transformer(srid: int) -> Transformer:
    """WGS84 lon/lat -> the departement's own projection.

    always_xy=True is mandatory: EPSG:4326 declares latitude first, so without it pyproj
    reads our (longitude, latitude) as (latitude, longitude) and silently misplaces every
    single point. Cached because building a Transformer costs more than using it."""
    return Transformer.from_crs(WGS84, f"EPSG:{srid}", always_xy=True)


@lru_cache(maxsize=1)
def load_commune_names() -> dict[str, str]:
    """INSEE commune code -> the name that code carries today.

    Two passes over the COG "commune depuis 1943" file, as the rule goes:
    1. the row still in force for a code, i.e. the one with an empty DATE_FIN;
    2. for the codes that no longer have one, the row that ended most recently - the last
       name that code ever carried. A code can hold several rows (39209 went Épy ->
       Épy-Lanéria -> Val-d'Épy -> Val d'Épy -> Val-d'Épy), hence the sort.

    TYPECOM is restricted to COM and ARM to leave out the communes associées and déléguées,
    and the Paris/Lyon/Marseille parents are dropped so those cities resolve to their
    arrondissement, which is what DVF carries. Cached: the file is ~3.4 MB and the years
    are enriched in a loop."""
    referential = pd.read_csv(
        INSEE_COMMUNES_URL,
        dtype=str,
        usecols=["TYPECOM", "COM", "LIBELLE", "DATE_FIN"],
    )
    referential = referential[
        referential["TYPECOM"].isin(("COM", "ARM"))
        & ~referential["COM"].isin(PLM_PARENTS)
    ]
    current = referential[referential["DATE_FIN"].isna()]
    names = dict(zip(current["COM"], current["LIBELLE"]))
    # sorted ascending so that, for a code with several closed rows, the last one written
    # wins - the most recent DATE_FIN
    retired = referential[
        referential["DATE_FIN"].notna() & ~referential["COM"].isin(names)
    ].sort_values("DATE_FIN")
    names.update(zip(retired["COM"], retired["LIBELLE"]))
    logging.info(
        f"INSEE referential: {len(current):,} active commune codes"
        f" + {len(names) - len(current):,} retired ones resolved to their last name"
    )
    return names


def project_to_departement(
    longitude: np.ndarray,
    latitude: np.ndarray,
    srid: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Project WGS84 lon/lat arrays into the departement's CRS.

    We move the points rather than the parcelles: a mutation is one point where a parcelle
    is ~100 vertices, and only the DOM use a projection other than 2154, so the polygons can
    stay exactly as the cadastre published them."""
    return _transformer(srid).transform(longitude, latitude)


class ParcellesIndex:
    """R-tree over one departement's parcelles, held in that departement's own projection.

    Query points must be projected with `project()` before being matched against `tree`,
    which is what keeps the geometries untouched.

    Measured on dep 01 (1.4M parcelles, shapely 2.0.7): ~1.9 s to build, ~1.3 GB of RAM per
    million parcelles. Across the ~116M parcelles of the whole file that is ~4 min in total,
    so building one index per departement is cheap; holding several at once is what is not
    (the biggest departements - 29, 59, 33 - are ~1.9M parcelles, ~2.5 GB each)."""

    def __init__(
        self,
        departement: str,
        srid: int,
        tree: shapely.STRtree,
        parcelle_ids: np.ndarray,
        communes: np.ndarray,
    ):
        self.departement = departement
        self.srid = srid
        self.tree = tree
        self.parcelle_ids = parcelle_ids
        self.communes = communes

    def __len__(self) -> int:
        return len(self.parcelle_ids)

    @classmethod
    def for_departement(cls, departement: str, parcelles_file: str) -> "ParcellesIndex":
        """Build the index for one departement, reading only its rows from the file.

        The parcelles file keeps the source's ordering, so it is sorted by departement and
        this filter prunes to that departement's row groups instead of scanning ~14 GB."""
        started = monotonic()
        table = pq.read_table(
            parcelles_file,
            columns=["geom_srid", "commune", "parcelle_id", "geometry"],
            filters=[("departement", "==", departement)],
        )
        if not table.num_rows:
            raise ValueError(f"No parcelle found for departement {departement}")

        srids = table.column("geom_srid").unique().to_pylist()
        if len(srids) != 1:
            # one projection per departement is what lets us transform the points once per
            # departement instead of once per row
            raise ValueError(
                f"Expected a single geom_srid for {departement}, got {srids}"
            )

        # to_numpy rather than to_pylist: the latter builds a python bytes object per
        # parcelle, which cost ~0.7 s and ~0.6 GB per departement for nothing
        geometries = shapely.from_wkb(
            table.column("geometry").to_numpy(zero_copy_only=False)
        )
        index = cls(
            departement=departement,
            srid=srids[0],
            tree=shapely.STRtree(geometries),
            parcelle_ids=table.column("parcelle_id").to_numpy(zero_copy_only=False),
            communes=table.column("commune").to_numpy(zero_copy_only=False),
        )
        logging.info(
            f"> dep {departement}: indexed {len(index):,} parcelles"
            f" (EPSG:{index.srid}) in {monotonic() - started:.1f}s"
        )
        return index

    def project(
        self,
        longitude: np.ndarray,
        latitude: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Project WGS84 lon/lat into this departement's CRS, ready to query `tree`."""
        return project_to_departement(longitude, latitude, self.srid)

    def _resolve_ambiguous_matches(
        self,
        point_pos: np.ndarray,
        geom_pos: np.ndarray,
        hit: np.ndarray,
        usable_rows: np.ndarray,
        longitude: np.ndarray,
        latitude: np.ndarray,
        dvf_parcelle_ids: np.ndarray | None,
    ) -> None:
        """Settle the points that land in more than one parcelle, updating `hit` in place.

        A point on a boundary is claimed by both neighbours and the cadastre gives us no
        way to tell which mutation it belongs to. When DVF's own parcelle is one of the
        candidates we keep it: the alternative is to move the row to a neighbouring parcelle
        on the strength of a coordinate that sits exactly on the line between them, which
        would then be written to `ancien_id_parcelle` as if the parcelle had really changed.
        Otherwise we keep the first candidate the R-tree returned: an arbitrary pick, but a
        stable one for a given cadastre file, so a rerun gives the same answer.

        Also logs each case: where it is, which parcelles claim it, whether they agree on
        the commune, and which one was kept."""
        # STRtree.query returns its pairs ordered by point index, which is what lets us
        # slice each point's candidates out of geom_pos with searchsorted
        counts = np.bincount(point_pos, minlength=len(usable_rows))
        ambiguous = np.flatnonzero(counts > 1)
        if not len(ambiguous):
            return
        starts = np.searchsorted(point_pos, ambiguous, side="left")
        ends = np.searchsorted(point_pos, ambiguous, side="right")
        same_commune = kept_dvf = 0
        for rank, (pos, start, end) in enumerate(zip(ambiguous, starts, ends)):
            candidates = geom_pos[start:end]
            ids = self.parcelle_ids[candidates]
            communes = self.communes[candidates]
            row = usable_rows[pos]
            agree = len(set(communes)) == 1
            same_commune += agree
            dvf_id = None if dvf_parcelle_ids is None else dvf_parcelle_ids[row]
            keep = np.flatnonzero(ids == dvf_id) if dvf_id is not None else []
            if len(keep):
                hit[pos] = candidates[keep[0]]
                kept_dvf += 1
            if rank < AMBIGUOUS_SAMPLE:
                logging.info(
                    f">> ambiguous: dep {self.departement}"
                    f" ({longitude[row]:.6f}, {latitude[row]:.6f}) falls in {len(ids)}"
                    f" parcelles {list(ids)} communes {list(communes)}"
                    f" - same commune: {agree} - dvf id {dvf_id}"
                    f" {'IS' if len(keep) else 'is NOT'} among them"
                    f" - kept {self.parcelle_ids[hit[pos]]}"
                )
        logging.warning(
            f"> dep {self.departement}: {len(ambiguous):,} points fall in several parcelles"
            f" - {same_commune:,} stay within a single commune"
            f" - {kept_dvf:,} settled by keeping the DVF parcelle,"
            f" {len(ambiguous) - kept_dvf:,} fell back to the first candidate"
        )

    def match(
        self,
        longitude: np.ndarray,
        latitude: np.ndarray,
        dvf_parcelle_ids: np.ndarray | None = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        """For each WGS84 point, find the cadastre parcelle that contains it.

        Returns (parcelle_ids, communes)

        Exceptions :

        - Returns empty for a parcelle long/lat:
            - Are empty in DVF
            - Fall in no parcelle from the cadastre.
        - If a parcelle long/lat is on a shared boundary for two parcelles in the cadastre,
        `dvf_parcelle_ids` is used :
            - Current parcelle_id stays DVF's own parcelle when that parcelle is one of the candidates
            - Otherwise it falls back to the first candidate the R-tree returns. That order
              is arbitrary (it follows the tree traversal, not the parcelle ids), but it is
              stable for a given cadastre file, so a rerun gives the same answer."""
        longitude = np.asarray(longitude, dtype="float64")
        latitude = np.asarray(latitude, dtype="float64")
        parcelle_ids = np.full(len(longitude), "", dtype=object)
        communes = np.full(len(longitude), "", dtype=object)

        # enrich_year leaves 1-2% of rows without coordinates (its "No coords: X%" log),
        # and pyproj maps a NaN to an infinity that would query the tree for nothing
        usable = np.isfinite(longitude) & np.isfinite(latitude)
        if not usable.any():
            logging.warning(f"> dep {self.departement}: no row has coordinates")
            return parcelle_ids, communes

        x, y = self.project(longitude[usable], latitude[usable])
        # shapely's STRtree already IS the two-phase algorithm: the R-tree narrows to the
        # parcelles whose bbox covers the point (2.65 on average), then "within" runs the
        # exact point-in-polygon on just those
        point_pos, geom_pos = self.tree.query(shapely.points(x, y), predicate="within")
        usable_rows = np.flatnonzero(usable)

        # assigning in reverse lets the FIRST pair returned for a point win, which is the
        # default for the points claimed by several parcelles; _resolve_ambiguous_matches
        # then overrides the ones where DVF's own parcelle is among the candidates
        hit = np.full(len(usable_rows), -1, dtype=np.int64)
        hit[point_pos[::-1]] = geom_pos[::-1]
        self._resolve_ambiguous_matches(
            point_pos, geom_pos, hit, usable_rows, longitude, latitude, dvf_parcelle_ids
        )
        found = hit >= 0
        rows = usable_rows[found]
        parcelle_ids[rows] = self.parcelle_ids[hit[found]]
        communes[rows] = self.communes[hit[found]]
        logging.info(
            f"> dep {self.departement}: {found.sum():,}/{len(longitude):,} rows matched"
            f" a parcelle ({len(longitude) - usable.sum():,} without coordinates)"
        )
        return parcelle_ids, communes


def list_departements(parcelles_file: str) -> list[str]:
    """The departements present in the parcelles file, read from the row group statistics
    rather than by scanning the column."""
    metadata = pq.ParquetFile(parcelles_file).metadata
    column = [
        metadata.schema.column(i).name for i in range(metadata.num_columns)
    ].index("departement")
    departements = set()
    for group in range(metadata.num_row_groups):
        statistics = metadata.row_group(group).column(column).statistics
        departements.update([statistics.min, statistics.max])
    return sorted(departements)


def check_parcelles_indexes(departements: list[str], cadastre_file: str, **context):
    """Build every departement's index once, to prove the parcelles file is usable before
    the matching stage spends hours on it: every departement loads, holds a single
    projection, and yields WKB shapely can parse.

    One departement at a time: an index is ~1.3 GB of RAM per million parcelles, so the
    whole country at once would not fit, while building one costs only ~2s.
    The matching itself is done downstream, from the same ParcellesIndex."""
    total = 0
    for departement in departements:
        index = ParcellesIndex.for_departement(departement, cadastre_file)
        total += len(index)
        del index  # the tree holds the geometries: drop it before loading the next one
        gc.collect()
    logging.info(f"{len(departements)} departements indexed, {total:,} parcelles total")


def enrich_with_cadastre(file, cadastre_file, tmp_folder):
    if not (match := re.fullmatch(r"temp-(\d{4})\.parquet", file)):
        raise ValueError(f"Unexpected file name: {file}")
    year = match.group(1)
    if f"full-{year}.csv.gz" in os.listdir(tmp_folder):
        logging.info(f"Skipping {file} - already processed")  # In case of retry
        return
    df = pd.read_parquet(tmp_folder + file, dtype_backend="pyarrow")
    for column in ("cadastre_parcelle_id", "cadastre_commune"):
        df[column] = pd.Series(pd.NA, index=df.index, dtype="string[pyarrow]")

    logging.info("Start matching parcelles long/lat to cadastre...")
    started = monotonic()  # todo : finish setting monotonic clock and check
    for departement, rows in df.groupby("code_departement", sort=False):
        logging.info(f"> Create index for department n°{departement}")
        index = ParcellesIndex.for_departement(str(departement), cadastre_file)
        logging.info(f"> Matching {len(rows)} parcelles")
        parcelle_ids, communes = index.match(
            rows["longitude"].to_numpy(dtype="float64"),
            rows["latitude"].to_numpy(dtype="float64"),
            rows["id_parcelle"].to_numpy(),
        )
        del index  # the arrays below are plain numpy, they don't reference the tree
        gc.collect()
        # match() returns "" where the cadastre has nothing to say (parcelle gone to the
        # domaine public, or a geometry the cadastre published broken): those rows keep the
        # codes DVF built from the source text rather than being blanked
        found = parcelle_ids != ""
        logging.info(f"> Matching {len(rows)} parcelles")
        df.loc[rows.index[found], "cadastre_parcelle_id"] = parcelle_ids[found]
        df.loc[rows.index[found], "cadastre_commune"] = communes[found]
    # A row the cadastre had no answer for is neither a parcelle nor a commune change.
    # Not merely defensive: without it those rows are excluded only because a missing
    # cadastre_commune propagates NA through the comparisons, which holds on the arrow
    # backend read above and NOT on object dtypes, where "01004" != None is True and every
    # unmatched row would be recorded as having changed commune.
    matched = df["cadastre_parcelle_id"].notna()

    # Only parcelle_id has changed in new cadastre
    mask_only_parcelles_change = (
        matched
        & (df["id_parcelle"] != df["cadastre_parcelle_id"])
        & (df["code_commune"] == df["cadastre_commune"])
    )
    df.loc[mask_only_parcelles_change, "ancien_id_parcelle"] = df["id_parcelle"]
    df.loc[mask_only_parcelles_change, "id_parcelle"] = df["cadastre_parcelle_id"]
    # Commune has changed since and so parcelle id
    # (disjoint from the mask above, which requires the commune unchanged, so the
    # id_parcelle just written cannot leak into ancien_id_parcelle below)
    mask_commune_change = matched & (df["code_commune"] != df["cadastre_commune"])
    df.loc[mask_commune_change, "ancien_code_commune"] = df["code_commune"]
    df.loc[mask_commune_change, "ancien_nom_commune"] = df["nom_commune"]
    df.loc[mask_commune_change, "ancien_id_parcelle"] = df["id_parcelle"]
    df.loc[mask_commune_change, "code_commune"] = df["cadastre_commune"]
    df.loc[mask_commune_change, "id_parcelle"] = df["cadastre_parcelle_id"]
    # the name has to follow the code, and it is looked up on the NEW code, so this has to
    # come after code_commune was overwritten just above
    nom_commune = df.loc[mask_commune_change, "code_commune"].map(load_commune_names())
    unresolved = nom_commune.isna()
    if unresolved.any():
        missing = sorted(set(df.loc[mask_commune_change, "code_commune"][unresolved]))
        logging.error(
            f"{int(unresolved.sum()):,} rows ({len(missing)} distinct codes) have a commune"
            " code the cadastre gave but the INSEE referential does not know:"
            f" {missing[:AMBIGUOUS_SAMPLE]}. Their nom_commune keeps the previous commune's"
            " name, which the new code contradicts - check whether INSEE_COMMUNES_URL still"
            " points at the current COG edition."
        )
        # keeping the old name rather than blanking the column: a stale name is easier to
        # spot and to repair downstream than a hole
        nom_commune = nom_commune.fillna(df.loc[mask_commune_change, "nom_commune"])
    df.loc[mask_commune_change, "nom_commune"] = nom_commune
    logging.info(
        f"{int(mask_commune_change.sum()):,} rows changed commune,"
        f" {int(mask_only_parcelles_change.sum()):,} changed parcelle only,"
        f" {int((~matched).sum()):,} had no cadastre match"
    )

    # working columns, not part of the published schema
    df.drop(columns=["cadastre_parcelle_id", "cadastre_commune"], inplace=True)
    logging.info("Saving file...")
    df.to_csv(
        tmp_folder + f"full-{year}.csv.gz",
        index=False,
        compression="gzip",
    )
    del df
    # end-of-loop garbage management, giving time to reclaim memory
    gc.collect()
    sleep(5)
