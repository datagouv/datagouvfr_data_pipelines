import gzip
import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from io import BytesIO
from urllib.parse import quote_plus, urlencode

import pandas as pd
import requests
from airflow.sdk import task
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.tchap import send_message

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}geozones/"
# same dataset id on demo (demo.data.gouv.fr) and prod (www.data.gouv.fr): only the
# client switches between the two instances, so the id is not env-dependent
dataset_id = "554210a9c751df2666a7b26c"
dataset = local_client.dataset(dataset_id, fetch=False)
geozones_file = File(
    source_path=TMP_FOLDER,
    source_name="export_geozones.json",
    remote_source=True,  # not remote but not created yet
)
countries_file = File(
    source_path=TMP_FOLDER,
    source_name="export_countries.json",
    remote_source=True,  # not remote but not created yet
)
levels_file = File(
    source_path=TMP_FOLDER,
    source_name="export_levels.json",
    remote_source=True,  # not remote but not created yet
)


def query_insee_sparql(query: str) -> bytes:
    """Run a SPARQL query against the INSEE endpoint and return the CSV payload."""
    endpoint = "https://rdf.insee.fr/sparql?query="
    params = {"format": "application/csv;grid=true"}
    url = endpoint + quote_plus(query, safe="*") + "&" + urlencode(params)
    logging.info("Querying INSEE SPARQL endpoint: %s", url)
    response = requests.get(url)
    response.raise_for_status()
    return response.content


def build_geozones_hierarchy(map_type: dict, exported_ids: set) -> tuple[dict, dict]:
    """
    Reconstruct, for every French zone, its parents and its full set of ancestors,
    from the INSEE "subdivisionDirecteDe" relations.

    Returns two dicts keyed by geozone id (e.g. "fr:commune:75056"):
      - parents: the closest parent ids that are present in the export (e.g.
        department + EPCI of a commune, or its commune for a delegated commune).
        Intermediate zones filtered out of the export (typically départemental
        arrondissements) are skipped, so a commune still exposes its department
        even when INSEE only links it to the department through such an
        arrondissement.
      - ancestors: every ancestor id, transitively.

    Why both, even though parents is always a subset of ancestors? They answer
    different questions, and we denormalize on purpose because this export is a
    reference dataset read far more often than it is regenerated (~once a year):
      - parents is the minimal navigation edge (breadcrumb, "one level up"). It is
        also the only field that keeps the *direct* relation and a zone's several
        distinct parent branches (a commune belongs both to a department and to an
        EPCI); ancestors flattens those together and loses that distinction.
      - ancestors is the flattened transitive closure, so a consumer can answer
        "is X inside Y?" with a single O(1) membership test (``Y in X.ancestors``)
        instead of walking the graph. Crucially, it is what lets descendants be
        derived downstream by a reverse lookup ("all communes of region 27" =
        every zone whose ancestors contains "fr:region:27"), which is why we do
        not compute descendants here.

    Both lists are restricted to zones actually present in the export
    (``exported_ids``) so we never reference a filtered-out zone, and "country:fr"
    is added as a top-level ancestor of every French zone. Statistical zonings
    (unité urbaine, aire d'attraction...) and suppressed (historical) zones are
    excluded directly in the SPARQL query.
    """
    child_types = ", ".join(
        f"igeo:{insee_type}" for insee_type in map_type if insee_type != "Etat"
    )
    query = f"""PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX igeo:<http://rdf.insee.fr/def/geo#>

    SELECT ?childType ?childCode ?parentType ?parentCode
    WHERE {{
        ?child igeo:subdivisionDirecteDe ?parent .
        ?child rdf:type ?ct ; igeo:codeINSEE ?childCode .
        ?parent rdf:type ?pt ; igeo:codeINSEE ?parentCode .
        BIND(REPLACE(STR(?ct), "http://rdf.insee.fr/def/geo#", "") AS ?childType)
        BIND(REPLACE(STR(?pt), "http://rdf.insee.fr/def/geo#", "") AS ?parentType)
        FILTER(?ct IN ({child_types}))
        FILTER(?pt IN (igeo:Region, igeo:Departement, igeo:CollectiviteDOutreMer,
                       igeo:Intercommunalite, igeo:Arrondissement, igeo:Commune))
        FILTER NOT EXISTS {{ ?evt_parent igeo:suppression ?parent }}
        FILTER NOT EXISTS {{ ?evt_child igeo:suppression ?child }}
    }}"""
    edges = pd.read_csv(BytesIO(query_insee_sparql(query)), dtype=str)

    expected_columns = {"childType", "childCode", "parentType", "parentCode"}
    missing_columns = expected_columns - set(edges.columns)
    if missing_columns:
        raise ValueError(
            f"Unexpected INSEE SPARQL response, missing columns: {sorted(missing_columns)}"
        )
    if edges.empty:
        raise ValueError(
            "INSEE SPARQL returned no subdivision relation; aborting so we never "
            "publish a geozones export without any hierarchy."
        )

    def to_geoid(insee_type, code):
        level = map_type.get(insee_type)
        if level is None or not isinstance(code, str):
            return None
        return f"{level}:{code}"

    direct_parents = defaultdict(set)
    for child_type, child_code, parent_type, parent_code in zip(
        edges["childType"], edges["childCode"], edges["parentType"], edges["parentCode"]
    ):
        child = to_geoid(child_type, child_code)
        parent = to_geoid(parent_type, parent_code)
        if child and parent and child != parent:
            direct_parents[child].add(parent)

    ancestors_cache: dict = {}

    def ancestors_of(geoid, visiting):
        if geoid in ancestors_cache:
            return ancestors_cache[geoid]
        result = set()
        for parent in direct_parents.get(geoid, ()):
            if parent in visiting:
                continue  # skip back-edge: a zone can never be its own ancestor
            result.add(parent)
            result |= ancestors_of(parent, visiting | {geoid})
        ancestors_cache[geoid] = result
        return result

    parents_cache: dict = {}

    def closest_exported_parents(geoid, visiting):
        """Direct parents present in the export, climbing over filtered-out ones."""
        if geoid in parents_cache:
            return parents_cache[geoid]
        result = set()
        for parent in direct_parents.get(geoid, ()):
            if parent in visiting:
                continue
            if parent in exported_ids:
                result.add(parent)
            else:
                result |= closest_exported_parents(parent, visiting | {geoid})
        parents_cache[geoid] = result
        return result

    has_country = "country:fr" in exported_ids
    parents = {}
    ancestors = {}
    for geoid in exported_ids:
        if not geoid or not geoid.startswith("fr:"):
            continue
        zone_ancestors = ancestors_of(geoid, frozenset()) & exported_ids
        if has_country:
            zone_ancestors.add("country:fr")
        ancestors[geoid] = sorted(zone_ancestors)
        parents[geoid] = sorted(closest_exported_parents(geoid, frozenset()))
    return parents, ancestors


# IGN administrative contours (the source geo.api.gouv.fr itself is built on),
# published per level and resolution. 100m is a good size/precision balance.
# Communes also need the associated/delegated file, otherwise merged (déléguées)
# communes get no geometry. Municipal arrondissements (Paris/Lyon/Marseille) and
# countries are not published here, so those zones keep a null geometry.
GEOMETRY_ENDPOINT = "https://contours-administratifs.s3.rbx.io.cloud.ovh.net"
GEOMETRY_SOURCES = {
    "fr:commune": ["communes-100m", "communes-associees-deleguees-100m"],
    "fr:epci": ["epci-100m"],
    "fr:departement": ["departements-100m"],
    "fr:region": ["regions-100m"],
}


def resolve_geometry_year() -> int:
    """Most recent millésime available in the contours bucket (files lag the year)."""
    current = datetime.now().year
    for year in range(current, current - 3, -1):
        probe = f"{GEOMETRY_ENDPOINT}/{year}/geojson/regions-1000m.geojson.gz"
        if requests.head(probe).status_code == 200:
            return year
    raise ValueError("No contours-administratifs millésime found in the last 3 years")


def fetch_geozones_geometries() -> dict:
    """
    Download IGN administrative contours and return, per level, a mapping from
    INSEE code to its GeoJSON geometry, e.g. {"fr:commune": {"21231": {...}}}.

    Geometry is an enrichment, not critical data: a source that fails to download
    is logged and skipped rather than aborting the whole geozones export.
    """
    year = resolve_geometry_year()
    logging.info("Fetching IGN geometries (millésime %s)", year)
    geometries: dict = defaultdict(dict)
    for level, names in GEOMETRY_SOURCES.items():
        for name in names:
            url = f"{GEOMETRY_ENDPOINT}/{year}/geojson/{name}.geojson.gz"
            try:
                response = requests.get(url)
                response.raise_for_status()
                features = json.loads(gzip.decompress(response.content))["features"]
            except (requests.RequestException, ValueError, KeyError) as e:
                logging.warning("Skipping geometry source %s: %s", url, e)
                continue
            for feature in features:
                code = feature["properties"].get("code")
                if code:
                    # first wins: current communes take precedence over delegated ones
                    geometries[level].setdefault(code, feature["geometry"])
    return geometries


POPULATION_ENDPOINT = "https://geo.api.gouv.fr"


def fetch_geozones_populations() -> dict:
    """
    Return the legal population per zone as {level: {code: population}}.

    Source is geo.api.gouv.fr (INSEE census, recent millésime): communes and
    EPCI expose it directly, department and region populations are summed from
    their communes since geo.api does not serve them. The INSEE SPARQL endpoint
    also carries population but is frozen at 2016, so it is not used.

    Best-effort: a geo.api failure is logged and yields no population rather than
    aborting the export.
    """
    populations: dict = {
        "fr:commune": {},
        "fr:epci": {},
        "fr:departement": defaultdict(int),
        "fr:region": defaultdict(int),
    }
    try:
        communes = requests.get(
            f"{POPULATION_ENDPOINT}/communes",
            params={"fields": "code,population,codeDepartement,codeRegion"},
        )
        communes.raise_for_status()
        epcis = requests.get(
            f"{POPULATION_ENDPOINT}/epcis", params={"fields": "code,population"}
        )
        epcis.raise_for_status()
    except requests.RequestException as e:
        logging.warning("Could not fetch populations from geo.api.gouv.fr: %s", e)
        return populations

    for epci in epcis.json():
        if epci.get("population"):
            populations["fr:epci"][epci["code"]] = epci["population"]
    for commune in communes.json():
        pop = commune.get("population")
        if pop is None:
            continue
        populations["fr:commune"][commune["code"]] = pop
        if commune.get("codeDepartement"):
            populations["fr:departement"][commune["codeDepartement"]] += pop
        if commune.get("codeRegion"):
            populations["fr:region"][commune["codeRegion"]] += pop
    return populations


@task()
def download_and_process_geozones():
    query = """PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfschema:<http://www.w3.org/2000/01/rdf-schema#>
    PREFIX igeo:<http://rdf.insee.fr/def/geo#>

    SELECT *
    WHERE {
        ?zone rdf:type ?territory .
        ?zone igeo:nom ?nom .
        ?zone igeo:codeINSEE ?codeINSEE .
        # Each attribute is fetched in its own OPTIONAL: grouping them means the
        # whole block only binds when *all* match, which (a) dropped nomSansArticle
        # /codeArticle for every non-suppressed zone and (b) missed the suppression
        # of zones lacking those attributes, mislabelling them as not deleted.
        OPTIONAL { ?zone igeo:nomSansArticle ?nomSansArticle . }
        OPTIONAL { ?zone igeo:codeArticle ?codeArticle . }
        OPTIONAL { ?suppression_evt igeo:suppression ?zone . }
    }"""
    df = pd.read_csv(BytesIO(query_insee_sparql(query)))
    logging.info("INSEE zones fetched: %s rows", len(df))
    df["type"] = df["territory"].apply(lambda x: x.split("#")[1])
    df["is_deleted"] = df["suppression_evt"].apply(lambda s: isinstance(s, str))
    for c in df.columns:
        if c != "is_deleted":
            df[c] = df[c].apply(str)
    map_type = {
        "Etat": "country",
        "Region": "fr:region",
        "Departement": "fr:departement",
        "CollectiviteDOutreMer": "fr:departement",
        "Intercommunalite": "fr:epci",
        "Arrondissement": "fr:arrondissement",
        "ArrondissementMunicipal": "fr:arrondissement",
        "Commune": "fr:commune",
        "CommuneDeleguee": "fr:commune",
        "CommuneAssociee": "fr:commune",
    }
    df = df.loc[df["type"].isin(map_type)]
    df["level"] = df["type"].apply(lambda x: map_type.get(x, x))
    df["_id"] = df["level"] + ":" + df["codeINSEE"]
    df = df.rename({"zone": "uri"}, axis=1)
    df = df.drop(["territory", "suppression_evt"], axis=1)
    df = df.loc[
        (df["type"] != "Arrondissement")
        | (
            (df["type"] == "Arrondissement")
            & (df["nom"].str.contains("|".join(["Paris", "Lyon", "Marseille"])))
        )
    ]
    df = df.loc[df["level"] != "country"]

    # get countries (with ISO alpha 2 code) from another source
    # keep_default_na=False: Namibia's ISO alpha-2 code is "NA", which pandas would
    # otherwise read as NaN, leaving the country without a codeINSEE/_id.
    countries = pd.read_csv(
        "https://www.data.gouv.fr/api/1/datasets/r/2b38f28d-15e7-4f0c-b61d-6ca1d9b1cfa2",
        sep=";",
        encoding="cp1252",
        dtype=str,
        keep_default_na=False,
        na_values=[""],
    )
    countries = countries.loc[countries["SOUVERAIN"] == "O"]
    countries["uri"] = countries["CODE_COG"].apply(
        lambda x: "http://id.insee.fr/geo/etat/" + x if isinstance(x, str) else x
    )
    countries.rename(
        {
            "NOM_COURT": "nom",
        },
        axis=1,
        inplace=True,
    )
    countries["nomSansArticle"] = countries["nom"]
    countries["codeArticle"] = None
    countries["type"] = "country"
    countries["is_deleted"] = False
    countries["level"] = "country"
    countries["codeINSEE"] = countries["ISO_alpha2"].apply(
        lambda x: x.lower() if isinstance(x, str) else x
    )
    countries["_id"] = countries["ISO_alpha2"].apply(
        lambda x: "country:" + x.lower() if isinstance(x, str) else x
    )
    countries = countries[
        [
            "uri",
            "nom",
            "codeINSEE",
            "nomSansArticle",
            "codeArticle",
            "type",
            "is_deleted",
            "level",
            "_id",
        ]
    ]
    countries_json = json.loads(countries.to_json(orient="records"))

    export = json.loads(df.to_json(orient="records"))
    export.extend(
        [
            {
                "uri": "http://id.insee.fr/geo/world",
                "nom": "Monde",
                "codeINSEE": "world",
                "nomSansArticle": "Monde",
                "codeArticle": None,
                "type": "country-group",
                "is_deleted": False,
                "level": "country-group",
                "_id": "country-group:world",
            },
            {
                "uri": "http://id.insee.fr/geo/europe",
                "nom": "Union Européenne",
                "codeINSEE": "ue",
                "nomSansArticle": "Union Européenne",
                "codeArticle": None,
                "type": "country-group",
                "is_deleted": False,
                "level": "country-group",
                "_id": "country-group:ue",
            },
            {
                "uri": None,
                "nom": "DROM",
                "codeINSEE": "fr:drom",
                "nomSansArticle": "DROM",
                "codeArticle": None,
                "type": "country-subset",
                "is_deleted": False,
                "level": "country-subset",
                "_id": "country-subset:fr:drom",
            },
            {
                "uri": None,
                "nom": "DROM-COM",
                "codeINSEE": "fr:dromcom",
                "nomSansArticle": "DROM-COM",
                "codeArticle": None,
                "type": "country-subset",
                "is_deleted": False,
                "level": "country-subset",
                "_id": "country-subset:fr:dromcom",
            },
            {
                "uri": None,
                "nom": "France métropolitaine",
                "codeINSEE": "fr:metro",
                "nomSansArticle": "France métropolitaine",
                "codeArticle": None,
                "type": "country-subset",
                "is_deleted": False,
                "level": "country-subset",
                "_id": "country-subset:fr:metro",
            },
        ]
    )
    export.extend(countries_json)
    for geoz in export:
        for c in geoz.keys():
            if geoz[c] == "nan":
                geoz[c] = None

    # Enrich each French zone with its hierarchy (direct parents + full ancestors),
    # rebuilt from INSEE subdivision relations. Only zones kept in the export are
    # referenced, so we never point to a filtered-out zone.
    exported_ids = {geoz["_id"] for geoz in export}
    parents_by_id, ancestors_by_id = build_geozones_hierarchy(map_type, exported_ids)
    for geoz in export:
        geoz["parents"] = parents_by_id.get(geoz["_id"], [])
        geoz["ancestors"] = ancestors_by_id.get(geoz["_id"], [])
    logging.info("Hierarchy computed for %s French zones", len(ancestors_by_id))

    # Enrich each zone with its IGN administrative contour, joined on the INSEE
    # code. Zones with no published geometry (countries, municipal arrondissements)
    # keep a null geom so every record exposes the key.
    geometries = fetch_geozones_geometries()
    for geoz in export:
        geoz["geom"] = geometries.get(geoz["level"], {}).get(geoz["codeINSEE"])
    logging.info(
        "Geometry attached to %s zones", sum(1 for z in export if z.get("geom"))
    )

    # Enrich with the legal population (geo.api.gouv.fr), joined on the INSEE code.
    populations = fetch_geozones_populations()
    for geoz in export:
        geoz["population"] = populations.get(geoz["level"], {}).get(geoz["codeINSEE"])
    logging.info(
        "Population attached to %s zones",
        sum(1 for z in export if z.get("population") is not None),
    )

    os.makedirs(TMP_FOLDER, exist_ok=True)
    with open(geozones_file.full_source_path, "w", encoding="utf8") as f:
        json.dump(export, f, ensure_ascii=False)
    logging.info(
        "Wrote %s zones to %s (%.0f MB)",
        len(export),
        geozones_file.full_source_path,
        os.path.getsize(geozones_file.full_source_path) / 1e6,
    )

    with open(countries_file.full_source_path, "w", encoding="utf8") as f:
        json.dump(countries_json, f, ensure_ascii=False, indent=4)

    levels = [
        {
            "id": "country-group",
            "label": "Country group",
            "admin_level": 10,
            "parents": [],
        },
        {
            "id": "country",
            "label": "Country",
            "admin_level": 20,
            "parents": ["country-group"],
        },
        {
            "id": "country-subset",
            "label": "Country subset",
            "admin_level": 30,
            "parents": ["country"],
        },
        {
            "id": "fr:region",
            "label": "French region",
            "admin_level": 40,
            "parents": ["country"],
        },
        {
            "id": "fr:departement",
            "label": "French county",
            "admin_level": 60,
            "parents": ["fr:region"],
        },
        {
            "id": "fr:arrondissement",
            "label": "French arrondissement",
            "admin_level": 70,
            "parents": ["fr:departement"],
        },
        {
            "id": "fr:commune",
            "label": "French town",
            "admin_level": 80,
            "parents": ["fr:arrondissement", "fr:epci"],
        },
        {
            "id": "fr:iris",
            "label": "Iris (Insee districts)",
            "admin_level": 98,
            "parents": ["fr:commune"],
        },
        {
            "id": "fr:canton",
            "label": "French canton",
            "admin_level": 98,
            "parents": ["fr:departement"],
        },
        {
            "id": "fr:collectivite",
            "label": "French overseas collectivities",
            "admin_level": 60,
            "parents": ["fr:region"],
        },
        {
            "id": "fr:epci",
            "label": "French intermunicipal (EPCI)",
            "admin_level": 68,
            "parents": ["country"],
        },
    ]
    with open(levels_file.full_source_path, "w", encoding="utf8") as f:
        json.dump(levels, f, ensure_ascii=False, indent=4)


@task()
def post_geozones():
    year = datetime.now().strftime("%Y")
    dataset.create_static(
        file_to_upload=geozones_file.full_source_path,
        payload={
            "description": (
                "Géozones créées à partir du [fichier de l'INSEE]"
                "(https://rdf.insee.fr/geo/index.html)"
            ),
            "filesize": os.path.getsize(geozones_file.full_source_path),
            "mime": "application/json",
            "title": f"Zones {year} (json)",
            "type": "main",
        },
    )

    dataset.create_static(
        file_to_upload=countries_file.full_source_path,
        payload={
            "description": (
                "Géozones (pays uniquement) créées à partir du [Référentiel des pays et des territoires]"
                "(https://www.data.gouv.fr/datasets/64959ecae2bdc5448631a59c/)"
            ),
            "filesize": os.path.getsize(countries_file.full_source_path),
            "mime": "application/json",
            "title": f"Zones pays uniquement {year} (json)",
            "type": "main",
        },
    )

    dataset.create_static(
        file_to_upload=levels_file.full_source_path,
        payload={
            "filesize": os.path.getsize(levels_file.full_source_path),
            "mime": "application/json",
            "title": f"Niveaux {year} (json)",
            "type": "main",
        },
    )


@task()
def notification():
    send_message(
        "Données Géozones mises à jours [ici]"
        f"({local_client.base_url}/datasets/{dataset_id})"
    )
