import gzip
import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from io import BytesIO
from typing import NamedTuple
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
from shapely.geometry import shape

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


# Zones that INSEE does not describe, defined once here and used in three steps:
# 1. definition: each one is a record of the export (download_and_process_geozones)
# 2. position: it is put under its parents (build_geozones_hierarchy)
# 3. bbox: it is derived from the bboxes of its descendants (add_geozones_bboxes)
# The countries, France included, come from another source and are all under the world.
WORLD = "country-group:world"
FRANCE = "country:fr"
METRO = "country-subset:fr:metro"
DROM = "country-subset:fr:drom"
COM = "country-subset:fr:com"
DROMCOM = "country-subset:fr:dromcom"
METRODROM = "country-subset:fr:metrodrom"


class HandMadeZone(NamedTuple):
    id: str
    nom: str
    code_insee: str
    uri: str | None
    parents: tuple[str, ...]


HAND_MADE_ZONES = [
    HandMadeZone(WORLD, "Monde", "world", "http://id.insee.fr/geo/world", ()),
    HandMadeZone(
        "country-group:ue",
        "Union Européenne",
        "ue",
        "http://id.insee.fr/geo/europe",
        (),
    ),
    HandMadeZone(DROM, "DROM", "fr:drom", None, (DROMCOM, METRODROM)),
    HandMadeZone(COM, "COM", "fr:com", None, (DROMCOM,)),
    HandMadeZone(METRO, "France métropolitaine", "fr:metro", None, (FRANCE, METRODROM)),
    HandMadeZone(
        METRODROM, "France métropolitaine et DROM", "fr:metrodrom", None, (FRANCE,)
    ),
    HandMadeZone(DROMCOM, "DROM-COM", "fr:dromcom", None, (FRANCE,)),
]


def add_geozones_bboxes(export: list[dict]) -> None:
    # Zones with a geometry (active ones only): bbox is the tight envelope of all their
    # polygons, [minx, miny, maxx, maxy] in WGS84.
    for zone in export:
        zone["bbox"] = list(shape(zone["geom"]).bounds) if zone.get("geom") else None

    # The hand-made subsets and France have no geometry but descendants in the
    # hierarchy (so this runs after it): their bbox is the union of the bboxes of every
    # zone with a geometry that has them as ancestor. It contains the bbox of each of
    # their descendants, and as no derived bbox feeds another the order doesn't matter.
    # Deleted zones have no geometry, so they never count, and the derived ids are
    # never shared with a deleted zone. Every other geometry-less zone (foreign
    # countries, EU, arrondissements...) stays None.
    derived = (METRO, DROM, COM, METRODROM, DROMCOM, FRANCE)
    boxes = defaultdict(list)
    for zone in export:
        if zone["bbox"]:
            for ancestor in zone["ancestors"]:
                if ancestor in derived:
                    boxes[ancestor].append(zone["bbox"])
    by_id = {z["_id"]: z for z in export}
    for zone_id in derived:
        if not boxes[zone_id]:
            raise ValueError(
                f"No descendant with a bbox to derive the bbox of {zone_id}"
            )
        by_id[zone_id]["bbox"] = [
            f(b[i] for b in boxes[zone_id]) for i, f in enumerate((min, min, max, max))
        ]
    # The world is the whole globe.
    by_id[WORLD]["bbox"] = [-180, -90, 180, 90]


def query_insee_sparql(query: str) -> bytes:
    """Run a SPARQL query against the INSEE endpoint and return the CSV payload."""
    endpoint = "https://rdf.insee.fr/sparql?query="
    params = {"format": "application/csv;grid=true"}
    url = endpoint + quote_plus(query, safe="*") + "&" + urlencode(params)
    logging.info("Querying INSEE SPARQL endpoint: %s", url)
    response = requests.get(url)
    response.raise_for_status()
    return response.content


def build_geozones_hierarchy(
    map_type: dict, exported_ids: set, com_ids: set
) -> tuple[dict, dict]:
    """Reconstruct, for every French zone, its parents and its full set of ancestors.

    Starting from the INSEE "subdivisionDirecteDe" relations, and put the
    countries and the hand-made subsets in the tree.

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
    (``exported_ids``) so we never reference a filtered-out zone. The top of the tree
    is not in the INSEE relations and is added by hand, and descendants inherit it:
      country-group:world > every country
      country:fr > country-subset:fr:metro > métropolitan régions
      country:fr > country-subset:fr:dromcom > country-subset:fr:drom > DROM régions
                                             > country-subset:fr:com > COM
      country:fr > country-subset:fr:metrodrom > metro and drom (a second parent)
    EPCI have no parent in the INSEE relations either: each is put under the smallest
    zone containing all its member communes (département, else région, else metro or
    drom). "country:fr" and the world are ancestors of every French zone. Statistical
    zonings (unité urbaine, aire d'attraction...) and suppressed (historical) zones
    are excluded directly in the SPARQL query.
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

    # The top of the tree is not in the INSEE relations, which exclude countries:
    # every country is under the world, and each hand-made zone under its parent.
    for geoid in exported_ids:
        if geoid.startswith("country:"):
            direct_parents[geoid].add(WORLD)
    for zone in HAND_MADE_ZONES:
        direct_parents[zone.id].update(zone.parents)

    # Put each French zone under its subset:
    # - the COM (com_ids, typed as such by INSEE) go under com
    # - the régions go under metro or drom depending on the code of their
    #   départements: 2 chars (2A/2B included) is métropolitan, 3 chars is a DROM.
    #   Only active départements have a région in the INSEE relations, and the COM
    #   have none, so they are left out naturally.
    # Communes and other descendants inherit it through the ancestors closure.
    for geoid in com_ids:
        direct_parents[geoid].add(COM)
    for geoid, regions in list(direct_parents.items()):
        if not geoid.startswith("fr:departement:"):
            continue
        subset = METRO if len(geoid.split(":")[-1]) == 2 else DROM
        for region in regions:
            if region.startswith("fr:region:"):
                direct_parents[region].add(subset)

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

    # EPCI have no parent in the INSEE relations: each is put under the smallest zone
    # containing all its member communes. That is its département when they all share
    # one, else its région when they all share one, else metro or DROM (none mixes
    # metro and overseas, and the COM have no EPCI). A zone covering only part of the
    # EPCI is never a parent, so "Y in X.ancestors" still means X is inside Y. Needs
    # the closure above to know where the communes are (some reach their département
    # only through an arrondissement), then the cache is reset as EPCI ancestors
    # changed.
    epci_members_ancestors = defaultdict(list)
    for child, child_parents in list(direct_parents.items()):
        if not child.startswith("fr:commune:"):
            continue
        for parent in child_parents:
            if parent.startswith("fr:epci:"):
                epci_members_ancestors[parent].append(ancestors_of(child, frozenset()))
    for epci, members_ancestors in epci_members_ancestors.items():
        for prefix in ("fr:departement:", "fr:region:", "country-subset:"):
            shared = set.intersection(
                *(
                    {a for a in ancestors if a.startswith(prefix)}
                    for ancestors in members_ancestors
                )
            )
            if prefix == "country-subset:":
                shared &= {METRO, DROM}
            if len(shared) == 1:
                direct_parents[epci].update(shared)
                break
    ancestors_cache.clear()

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

    # Every French zone has France and its ancestors (the world) as ancestors,
    # including those with no edge above, such as deleted zones. The countries and
    # the subsets get their own chain from the edges above.
    france_chain = {FRANCE} | ancestors_of(FRANCE, frozenset())
    parents = {}
    ancestors = {}
    for geoid in exported_ids:
        if not geoid or not geoid.startswith(("fr:", "country:", "country-subset:")):
            continue
        zone_ancestors = ancestors_of(geoid, frozenset()) & exported_ids
        if geoid.startswith("fr:"):
            zone_ancestors |= france_chain
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
        if requests.head(probe, timeout=120).status_code == 200:
            return year
    raise ValueError("No contours-administratifs millésime found in the last 3 years")


def fetch_geozones_geometries() -> dict:
    """Download IGN administrative contours and return a per-level mapping.

    Each level maps INSEE codes to their GeoJSON geometry, e.g.
    {"fr:commune": {"21231": {...}}}.

    Any source that fails to download aborts the export: the geometries and the
    bboxes derived from them are needed to match datasets to zones, and publishing a
    whole level without them would wipe the ones of the previous export.
    """
    year = resolve_geometry_year()
    logging.info("Fetching IGN geometries (millésime %s)", year)
    geometries: dict = defaultdict(dict)
    for level, names in GEOMETRY_SOURCES.items():
        for name in names:
            url = f"{GEOMETRY_ENDPOINT}/{year}/geojson/{name}.geojson.gz"
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            features = json.loads(gzip.decompress(response.content))["features"]
            for feature in features:
                code = feature["properties"].get("code")
                if code:
                    # first wins: current communes take precedence over delegated ones
                    geometries[level].setdefault(code, feature["geometry"])
    return geometries


POPULATION_ENDPOINT = "https://geo.api.gouv.fr"


def fetch_geozones_populations() -> dict:
    """Return the legal population per zone as {level: {code: population}}.

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
    # The hand-made zones, see HAND_MADE_ZONES.
    export.extend(
        {
            "uri": zone.uri,
            "nom": zone.nom,
            "codeINSEE": zone.code_insee,
            "nomSansArticle": zone.nom,
            "codeArticle": None,
            "type": zone.id.split(":")[0],
            "is_deleted": False,
            "level": zone.id.split(":")[0],
            "_id": zone.id,
        }
        for zone in HAND_MADE_ZONES
    )
    export.extend(countries_json)
    for geoz in export:
        for c in geoz.keys():
            if geoz[c] == "nan":
                geoz[c] = None

    # Enrich each zone with its hierarchy (direct parents + full ancestors), rebuilt
    # from INSEE subdivision relations plus the hand-made top of the tree. Only zones
    # kept in the export are referenced, so we never point to a filtered-out zone.
    # Must run before the bboxes, which are derived from the ancestors.
    exported_ids = {geoz["_id"] for geoz in export}
    # INSEE also types the historical colonies as COM, and some deleted COM share
    # their id with an active zone (976, Mayotte): only the active ones are the COM.
    com_ids = {
        geoz["_id"]
        for geoz in export
        if geoz["type"] == "CollectiviteDOutreMer" and not geoz["is_deleted"]
    }
    parents_by_id, ancestors_by_id = build_geozones_hierarchy(
        map_type, exported_ids, com_ids
    )
    for geoz in export:
        geoz["parents"] = parents_by_id.get(geoz["_id"], [])
        geoz["ancestors"] = ancestors_by_id.get(geoz["_id"], [])
    logging.info("Hierarchy computed for %s zones", len(ancestors_by_id))

    # Enrich each zone with its IGN administrative contour, joined on the INSEE
    # code. Zones with no published geometry (countries, municipal arrondissements)
    # keep a null geom so every record exposes the key. Deleted zones too: IGN only
    # publishes today's contours, and a deleted zone sharing its code with an active
    # one (976, a commune merged into a commune nouvelle) would get the current shape.
    geometries = fetch_geozones_geometries()
    for geoz in export:
        geoz["geom"] = (
            None
            if geoz["is_deleted"]
            else geometries.get(geoz["level"], {}).get(geoz["codeINSEE"])
        )
    logging.info(
        "Geometry attached to %s zones", sum(1 for z in export if z.get("geom"))
    )
    add_geozones_bboxes(export)
    logging.info("Bbox attached to %s zones", sum(1 for z in export if z.get("bbox")))

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
