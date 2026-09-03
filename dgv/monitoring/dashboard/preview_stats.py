import datetime
import json
import logging
import re
from io import StringIO
from urllib.parse import urlparse

import pandas as pd
from airflow.sdk import task
from datagouvfr_data_pipelines.config import AIRFLOW_DAG_TMP
from datagouvfr_data_pipelines.dgv.monitoring.dashboard.task_functions import DAG_NAME
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.s3 import S3Client

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/preview_stats/"
s3_destination_folder = "dashboard/"

# Full catalog exports published as data.gouv.fr dataset resources
EXPORT_RESOURCE_URL = (
    "https://www.data.gouv.fr/api/1/datasets/r/4babf5f2-6a9c-45b5-9144-ca5eae6a7a6d"
)
EXPORT_DATASET_URL = (
    "https://www.data.gouv.fr/api/1/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3"
)

# the aggregated stats keep a growing history at the top level.
# the per-resource detail is overwritten on each run.
# level each run (no "_latest" suffix, no dated folder for it).
history_file_name = "stats_catalog_preview.csv"
detail_file_name = "preview_catalog.csv.gz"

bucket = "dataeng-open"

MIN_FORMAT_COUNT = 10

FORMAT_FAMILIES = {
    "Tabulaire": ["csv", "xls", "xlsx", "ods", "parquet", "txt", "csv.gz"],
    "Document": ["pdf", "doc", "docx", "odt", "rtf", "odp"],
    "Données structurées": [
        "json",
        "xml",
        "geojson",
        "gpx",
        "kml",
        "kmz",
        "shp",
        "gpkg",
        "shp.zip",
        "mif",
        "tab",
        "gdb",
        "dbf",
        "nc",
        "h5",
        "gtfs",
        "gtfs-rt",
        "netex",
        "siri",
        "grib",
        "grib2",
        "geotiff",
        "dxf",
        "dwg",
        "sql",
        "xsd",
        "ttl",
        "n3",
        "rdf",
        "rdf+xml",
        "rdf/n3",
        "rdf/nt",
        "ld+json",
        "turtle",
        "pmtiles",
    ],
    "Image": ["png", "jpg", "jpeg", "svg", "tiff", "jp2", "ecw", "jpe"],
    "Archive": [
        "zip",
        "tar",
        "gz",
        "tgz",
        "7z",
        "rar",
        "bz2",
        "xz",
        "tar.gz",
        "tar.xz",
    ],
    "Liens": ["url", "doi", "atom", "rss"],
    "API": [
        "wms",
        "wfs",
        "wmts",
        "wcs",
        "arcgis-rest",
        "ogc-api-features",
        "ows-c",
        "odata",
        "siri",
    ],
    "Autre": ["html", "ics"],
}


def normalize_format(raw):
    raw = str(raw).strip().lower()
    if not raw or raw in ("inconnu", "unknown", "autre", "other", "information"):
        return "Autre"
    if raw.startswith("www:link") or raw in ("www:download",):
        return "url"
    if raw.startswith("www:download-"):
        return "url"
    if raw.startswith("www:download:"):
        raw = raw.removeprefix("www:download:")
        raw = re.sub(r"\s*\(.*", "", raw).strip()
        raw = raw.rsplit("/", 1)[-1]
    if raw.startswith("file://"):
        raw = raw.rsplit("/", 1)[-1]
    mime_map = {
        "vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
        "vnd.ms-excel": "xls",
        "vnd.oasis.opendocument.spreadsheet": "ods",
        "vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
        "msword": "doc",
        "vnd.google-earth.kml+xml": "kml",
        "x-gis/x-shapefile": "shp",
        "vnd.ogc.wms_xml": "wms",
        "comma separated value (csv)": "csv",
        "adobe portable document format (pdf)": "pdf",
        "application/rtf": "rtf",
        "image tiff (tif)": "tiff",
        "mapinfo interchange format (mif/mid)": "mif",
        "mapinfo tab": "tab",
        "esri shapefile (shp)": "shp",
        "esri shapefile": "shp",
        "shapefile (zip)": "shp.zip",
        "dbase database file (dbf)": "dbf",
        "geopackage - gpkg": "gpkg",
        "arcgis geoservices rest api": "arcgis-rest",
        "ogc api - features": "ogc-api-features",
        "ogc api features": "ogc-api-features",
        "site internet": "url",
        "page web": "url",
        "web page": "url",
        "hyperlink": "url",
        "excel non structuré": "xls",
        "microsoft excel": "xls",
        "plain": "txt",
        "csv/utf8": "csv",
        "csv lonlat/xy": "csv",
        "archive zip": "zip",
        "shapefile": "shp",
        "arcgis": "arcgis-rest",
    }
    if raw in mime_map:
        return mime_map[raw]
    if raw.startswith("http://") or raw.startswith("https://"):
        return "url"
    if raw.startswith("ogc:"):
        return raw.removeprefix("ogc:")
    known_compound = {
        "ld+json",
        "rdf+xml",
        "rdf/n3",
        "rdf/nt",
        "csv.gz",
        "shp.zip",
        "gpkg.zip",
        "tar.gz",
        "tar.xz",
        "gtfs-rt",
        "ogc-api-features",
    }
    if raw in known_compound:
        return raw
    raw = re.sub(r"\(.*?\)", "", raw).strip()
    raw = re.sub(r"[,;+].*$", "", raw).strip()
    raw = raw.split("/")[-1].split(" - ")[0].strip()
    if not raw:
        return "Autre"
    return raw


def format_family(fmt):
    fmt = str(fmt).strip().lower()
    for family, formats in FORMAT_FAMILIES.items():
        if fmt in formats or any(fmt.endswith("." + f) for f in formats):
            return family
    return "Autre"


def _as_int(value):
    """Robustly coerce a numeric (str/int/float) to int, or None."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def get_resource_previews(row: dict) -> tuple[list[str], list[str]]:
    """
    Returns (previews, errors).

    Only includes errors for preview types that are actually relevant
    to this resource (based on format, extras, etc.).

    Error reasons:
      - source_unreachable  : resource URL returned HTTP >= 400
      - parsing_error       : tabular analysis failed (csv_detective, timeout, …)
      - cors_blocked        : allow-origin header present but doesn't allow data.gouv.fr (or GET)
      - cors_missing        : resource was checked but server returned no allow-origin header (blocked in browser)
      - cors_unknown        : resource was never CORS-checked, cannot verify access
      - file_too_big        : file size exceeds the preview limit
      - unknown_size        : file size unknown, cannot verify eligibility
    """
    extras = json.loads(row.get("extras") or "{}")
    fmt = normalize_format(row.get("format"))
    url = (row.get("url") or "").lower()
    file_size = _as_int(row.get("filesize")) or _as_int(
        extras.get("analysis:content-length")
    )

    # --- CORS check (mirrors cdata's getResourceCorsStatus) ---
    # "check:cors:status" presence tells whether the resource was actually
    # CORS-checked. If checked but no "allow-origin" header was returned, the
    # browser blocks the cross-origin read -> blocked (missing header)
    allow_origin = extras.get("check:cors:allow-origin")
    cors_reason = None
    if allow_origin is not None:
        trusted_domains = ["data.gouv.fr"]
        has_public_cors = allow_origin == "*"
        has_specific_cors = False
        if not has_public_cors:
            try:
                hostname = urlparse(allow_origin).hostname or allow_origin
                if hostname:
                    has_specific_cors = any(
                        hostname == d or hostname.endswith(f".{d}")
                        for d in trusted_domains
                    )
            except Exception:
                pass
        raw_methods = extras.get("check:cors:allow-methods") or ""
        allowed_methods = [m.strip().upper() for m in raw_methods.split(",")]
        supports_get = len(allowed_methods) == 0 or "GET" in allowed_methods
        cors_allowed = (has_public_cors or has_specific_cors) and supports_get
    elif extras.get("check:cors:status") is not None:
        # checked, but server returned no allow-origin header -> blocked in browser
        cors_allowed = False
        cors_reason = "cors_missing"
    else:
        # never CORS-checked -> cannot verify access
        cors_allowed = None
        cors_reason = "cors_unknown"

    check_status = extras.get("check:status")
    timeout = extras.get("check:timeout") is True
    source_unreachable = timeout or (
        _as_int(check_status) is not None and _as_int(check_status) >= 400
    )

    previews = []
    errors = {}

    # --- Map ---
    # Mirrors cdata's detectOgcService: WMS via format or a GetCapabilities URL
    # carrying service=wms (normalize_format maps ogc:wms -> wms).
    has_pmtiles = extras.get("analysis:parsing:pmtiles_url") or fmt == "pmtiles"
    has_wms = fmt == "wms" or (
        "request=getcapabilities" in url and "service=wms" in url
    )
    if has_pmtiles or has_wms:
        if source_unreachable:
            errors["map"] = "source_unreachable"
        else:
            previews.append("map")

    # --- JSON / PDF / XML ---
    limits = {"json": 1_000_000, "pdf": 10_000_000, "xml": 100_000}
    for pt, max_size in limits.items():
        if fmt != pt:
            continue
        if source_unreachable:
            errors[pt] = "source_unreachable"
        elif cors_allowed is False:
            errors[pt] = cors_reason if cors_reason else "cors_blocked"
        elif cors_allowed is None:
            errors[pt] = "cors_unknown"
        elif file_size is None:
            errors[pt] = "unknown_size"
        elif file_size > max_size:
            errors[pt] = "file_too_big"
        else:
            previews.append(pt)

    # --- Tabular ---
    # Mirrors cdata's useHasTabularData + hasTabularParsingError: only parsing
    # errors in the tabular parsing itself are fatal; downstream geo/format export
    # failures (pmtiles_export / geojson_export) leave the table usable.
    NON_TABULAR_PARSING_ERROR_STEPS = {"pmtiles_export", "geojson_export"}
    is_tabular_format = fmt in {
        "csv",
        "xls",
        "xlsx",
        "ods",
        "parquet",
        "csv.gz",
        "tsv",
        "dta",
        "sas7bdat",
        "sav",
    }
    has_parsing_extras = bool(
        extras.get("analysis:parsing:parsing_table")
        or extras.get("analysis:parsing:error")
    )
    if is_tabular_format or has_parsing_extras:
        if source_unreachable:
            errors["tabular"] = "source_unreachable"
        elif extras.get("analysis:parsing:error"):
            error_step = (extras.get("analysis:parsing:error") or "").split(":", 1)[0]
            if error_step not in NON_TABULAR_PARSING_ERROR_STEPS:
                errors["tabular"] = "parsing_error"
            elif extras.get("analysis:parsing:parsing_table"):
                previews.append("tabular")
        elif extras.get("analysis:parsing:parsing_table"):
            previews.append("tabular")

    # --- Datafair / OpenAPI ---
    if extras.get("datafairEmbed"):
        previews.append("datafair")
    if extras.get("apidocUrl"):
        previews.append("openapi")

    # --- Analysis reported the file too large to download ---
    # Mirrors cdata's ResourceExplorerViewer which shows the "trop volumineux"
    # message when extras["analysis:error"] == "File too large to download".
    # Only applies when there is no other preview (cdata ignores this field when
    # parsing_table produced a table, e.g. useHasTabularData).
    if not previews and extras.get("analysis:error") == "File too large to download":
        if "file_too_big" not in errors.values():
            errors["analysis"] = "file_too_big"

    return previews, list(errors.values())


def _tabular_delay(tab_date, mod):
    if not tab_date or not mod or not isinstance(mod, str):
        return None
    try:
        return (
            datetime.datetime.fromisoformat(tab_date).replace(tzinfo=None)
            - datetime.datetime.fromisoformat(mod).replace(tzinfo=None)
        ).days
    except ValueError:
        return None


def build_resource_info_table(row, dataset_dict):
    extras = json.loads(row.get("extras") or "{}")
    detected_size = extras.get("check:headers:content-length") or extras.get(
        "analysis:content-length"
    )
    detected_format = extras.get("check:headers:content-type") or extras.get(
        "analysis:content-type"
    )
    url = row.get("url") or ""
    if url.startswith("https://static.data.gouv.fr/resources/"):
        source = "static"
    else:
        harvest = dataset_dict.get(row.get("dataset.id"), {}).get("harvest", False)
        source = "harvest" if harvest else "remote"
    previews, errors = get_resource_previews(row)

    tabular_preview_last_update = extras.get("analysis:parsing:finished_at")
    normalized = normalize_format(row.get("format"))
    return {
        "id": row.get("id"),
        "titre": row.get("title"),
        "url": url,
        "source": source,
        "format déclaré": row.get("format"),
        "format détecté": detected_format,
        "format normalisé": normalized,
        "famille": format_family(normalized),
        "taille déclarée": row.get("filesize"),
        "taille détectée": detected_size,
        "téléchargements": row.get("downloads"),
        "dernière modification": row.get("modified"),
        "dernière mise à jour tabular": tabular_preview_last_update,
        "délai tabular (jours)": _tabular_delay(
            extras.get("analysis:parsing:finished_at"), row.get("modified")
        ),
        "aperçus actifs": ",".join(previews),
        "a un aperçu": bool(previews),
        "a une erreur": any(
            v in {"source_unreachable", "parsing_error", "cors_blocked", "cors_missing"}
            for v in errors
        ),
        "aperçu manquant": not previews and not any(v is not None for v in errors),
        "erreur source inaccessible": "source_unreachable" in set(errors),
        "erreur analyse": "parsing_error" in set(errors),
        "erreur cors bloqué": "cors_blocked" in set(errors),
        "erreur cors header manquant": "cors_missing" in set(errors),
        "erreur cors inconnu": "cors_unknown" in set(errors),
        "erreur fichier trop volumineux": "file_too_big" in set(errors),
        "erreur taille inconnue": "unknown_size" in set(errors),
    }


@task()
def download_exports() -> None:
    """Download the resources and datasets catalog export files."""
    from datagouvfr_data_pipelines.utils.download import download_files

    download_files(
        [
            File(
                url=EXPORT_RESOURCE_URL,
                dest_path=TMP_FOLDER,
                dest_name="export_resource.csv",
            ),
            File(
                url=EXPORT_DATASET_URL,
                dest_path=TMP_FOLDER,
                dest_name="export_dataset.csv",
            ),
        ]
    )


def _read_export(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, delimiter=";", dtype="string", low_memory=False)
    return df.fillna(value="")


def build_stats(df_res: pd.DataFrame) -> pd.DataFrame:
    """Aggregate the per-resource detail rows into the monthly format stats.

    Columns of ``df_res`` are expected to come from ``build_resource_info_table``
    (famille, format normalisé, a un aperçu, a une erreur,
    erreur fichier trop volumineux, aperçu manquant, id).
    """
    total = len(df_res)
    stats = (
        df_res.groupby(["famille", "format normalisé"])
        .agg(
            nombre=("id", "count"),
            prévisualisable=("a un aperçu", "sum"),
            erreur=("a une erreur", "sum"),
            trop_volumineux=("erreur fichier trop volumineux", "sum"),
            manquant=("aperçu manquant", "sum"),
        )
        .reset_index()
    )
    # Collapse formats with very few resources (mostly junk values)
    # into the "Autre" row so the stats are not cluttered by a long tail.
    stats.loc[stats["nombre"] < MIN_FORMAT_COUNT, "format normalisé"] = "Autre"
    stats.loc[stats["nombre"] < MIN_FORMAT_COUNT, "famille"] = "Autre"
    stats = (
        stats.groupby(["famille", "format normalisé"])
        .agg(
            nombre=("nombre", "sum"),
            prévisualisable=("prévisualisable", "sum"),
            erreur=("erreur", "sum"),
            trop_volumineux=("trop_volumineux", "sum"),
            manquant=("manquant", "sum"),
        )
        .reset_index()
    )
    stats["% catalogue"] = (stats["nombre"] / total * 100).round(1)
    stats["% prévisualisable"] = (
        stats["prévisualisable"] / stats["nombre"] * 100
    ).round(1)
    stats["% erreur"] = (stats["erreur"] / stats["nombre"] * 100).round(1)
    stats["% trop volumineux"] = (
        stats["trop_volumineux"] / stats["nombre"] * 100
    ).round(1)
    stats["% prévisualisation manquante"] = (
        stats["manquant"] / stats["nombre"] * 100
    ).round(1)
    stats = stats.drop(columns=["erreur", "trop_volumineux", "manquant"])
    stats["mois"] = datetime.datetime.today().strftime("%Y-%m")
    stats = stats.sort_values(["famille", "nombre"], ascending=[True, False])
    return stats


def compute_stats(resource_df: pd.DataFrame, dataset_df: pd.DataFrame) -> tuple:
    """Run the full per-resource -> stats computation from export DataFrames.

    Excludes resources belonging to archived datasets ("dataset.archived" is
    "false" when not archived and an ISO date when archived), builds the detail
    table, then aggregates the monthly format stats.
    """
    archived = (
        resource_df["dataset.archived"].fillna("").astype(str).str.strip().str.lower()
    )
    resource_df = resource_df[archived.eq("false")]

    dataset_dict = {
        row["id"]: {"harvest": bool(row.get("harvest.backend"))}
        for _, row in dataset_df.iterrows()
    }

    df_res = pd.DataFrame.from_dict(
        [
            build_resource_info_table(row, dataset_dict)
            for _, row in resource_df.iterrows()
        ]
    )

    return df_res, build_stats(df_res)


@task()
def get_preview_stats() -> None:
    df = _read_export(f"{TMP_FOLDER}export_resource.csv")
    df_dataset = _read_export(f"{TMP_FOLDER}export_dataset.csv")

    df_res, stats = compute_stats(df, df_dataset)

    stats.to_csv(TMP_FOLDER + "stats_current.csv", index=False)
    df_res.to_csv(TMP_FOLDER + detail_file_name, index=False, compression="gzip")


@task()
def upload_preview_stats() -> None:
    s3 = S3Client(conn_name="S3_OVH_RBX", bucket=bucket)

    current = pd.read_csv(TMP_FOLDER + "stats_current.csv", dtype="string")
    key_cols = ["famille", "format normalisé", "mois"]

    history_key = s3_destination_folder + history_file_name
    if s3.does_file_exist_in_bucket(history_key):
        logging.info(f"Existing history found, appending new rows")
        history = pd.read_csv(
            StringIO(s3.get_file_content(history_key)),
            dtype="string",
        )
        history = pd.concat([history, current]).drop_duplicates(key_cols, keep="last")
    else:
        logging.warning("No existing history found, starting fresh")
        history = current

    history.to_csv(TMP_FOLDER + history_file_name, index=False)

    s3.send_files(
        list_files=[
            File(
                source_path=TMP_FOLDER,
                source_name=history_file_name,
                dest_path=s3_destination_folder,
                dest_name=history_file_name,
            ),
            File(
                source_path=TMP_FOLDER,
                source_name=detail_file_name,
                dest_path=s3_destination_folder,
                dest_name=detail_file_name,
            ),
        ],
        ignore_airflow_env=True,
        is_public=True,
    )
