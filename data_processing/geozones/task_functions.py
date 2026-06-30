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
    AIRFLOW_ENV,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.tchap import send_message

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}geozones/"
dataset_id = (
    "554210a9c751df2666a7b26c" if AIRFLOW_ENV == "prod" else "64bfc429d6e029048e577d3e"
)
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

    Both lists are restricted to zones actually present in the export
    (``exported_ids``) so we never reference a filtered-out zone, and "country:fr"
    is added as a top-level ancestor of every French zone. Statistical zonings
    (unité urbaine, aire d'attraction...) and suppressed (historical) zones are
    excluded directly in the SPARQL query.

    Descendants are intentionally not computed here: they are obtained downstream
    by a reverse lookup on ``ancestors``.
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

    ancestors_cache = {}

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

    parents_cache = {}

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

    os.mkdir(TMP_FOLDER)
    with open(geozones_file.full_source_path, "w", encoding="utf8") as f:
        json.dump(export, f, ensure_ascii=False, indent=4)

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
        file_to_upload=countries_file,
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
        file_to_upload=levels_file,
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
