"""Build the standardized files of the senatorial elections (general renewals only,
partial elections are excluded), upload them to our S3 and register them in
sources.json.

Requires xlrd for the 1992-2011 .xls files. Usage, from dags/:
    uv run --with xlrd python -m datagouvfr_data_pipelines.data_processing.elections.aggregation.scripts.build_senatoriales [--year 2026] [--dry-run]
"""

import argparse
import logging
import re
import tempfile
from pathlib import Path

import pandas as pd

from datagouvfr_data_pipelines.data_processing.elections.aggregation.schema import (
    dtypes,
)
from datagouvfr_data_pipelines.data_processing.elections.aggregation.scripts.common import (
    download_file,
    get_dataset,
    get_s3_bucket,
    load_sources,
    register_source,
    save_sources,
    source_key,
    upload_file,
)

# for each year, the source dataset and the tables (Excel sheets, or CSV resources
# titles) of each round; majoritarian and proportional departments are disjoint
SENATORIALES = {
    1992: ("536993a9a3a729239d204262", {"t1": ["Maj T1", "Prop"], "t2": ["Maj T2"]}),
    1995: ("536993aaa3a729239d204263", {"t1": ["Maj T1", "Prop"], "t2": ["Maj T2"]}),
    1998: ("536993aaa3a729239d204264", {"t1": ["Maj T1", "Prop"], "t2": ["Maj T2"]}),
    2001: ("536993aba3a729239d204265", {"t1": ["Maj T1", "Prop"], "t2": ["Maj T2"]}),
    2004: ("536993aba3a729239d204266", {"t1": ["Maj T1", "Prop"], "t2": ["Maj T2"]}),
    2008: ("536993aba3a729239d204267", {"t1": ["Maj T1", "Prop"], "t2": ["Maj T2"]}),
    2011: ("536993aca3a729239d204268", {"t1": ["Maj T1", "Prop"], "t2": ["Maj T2"]}),
    2014: ("5436344388ee3842773199b2", {"t1": ["Maj T1", "Prop"], "t2": ["Maj T2"]}),
    2017: ("59c8af30c751df49b73b9a3b", {"t1": ["MAJ T1", "PROP"], "t2": ["MAJ T2"]}),
    2020: ("5f72f4db8c0ebf04393a7971", {"t1": ["MAJ 1", "PROP"], "t2": ["MAJ 2"]}),
    2023: (
        "651559bbf0ed2c8d9e50db43",
        {"t1": ["MAJ - T1", "PROP"], "t2": ["MAJ - T2"]},
    ),
    2026: (
        "6ab9abea8ebfc5bc472f3924",
        {
            "t1": [
                "Sénatoriales 2026 - Résultats T1 Scrutin majoritaire et scrutin proportionnel.csv"
            ],
            "t2": ["Sénatoriales 2026 - Résultats T2 scrutin majoritaire.csv"],
        },
    ),
}

# source headers (trailing block numbers removed) -> our columns
GENERAL_ALIASES = {
    "Code du niveau": "code_departement",
    "Code du département": "code_departement",
    "Code département": "code_departement",
    "Libellé du niveau": "libelle_departement",
    "Libellé du département": "libelle_departement",
    "Libellé département": "libelle_departement",
    "Inscrits": "inscrits",
    "Abstentions": "abstentions",
    "Votants": "votants",
    "Blancs": "blancs",
    "Nuls": "nuls",
    # blank and null votes are merged before 2014, as for 2008_muni
    "Blancs et nuls": "nuls",
    "Exprimés": "exprimes",
}
GENERAL_IGNORED = {
    "Type de scrutin",
    "Type scrutin",
    "Date de l'export",
    "N° Tour",
    "Code localisation",
    "Libellé localisation",
    # 2023 locates the vote at the prefecture commune and a nominal polling station,
    # which would wrongly assign all the electors to that commune
    "Code commune",
    "Libellé commune",
    "Code BV",
}
# 2023 and 2026 spell the sex out, the other elections use M / F
SEXES = {"MASCULIN": "M", "FEMININ": "F", "FÉMININ": "F"}
BLOCK_ALIASES = {
    "Sexe": "sexe",
    "Sexe candidat": "sexe",
    "Nom": "nom",
    "Nom candidat": "nom",
    "Prénom": "prenom",
    "Prénom candidat": "prenom",
    "Nuance": "nuance",
    "Nuance candidat": "nuance",
    "Nuance Liste": "nuance_liste",
    "Nuance liste": "nuance_liste",
    "Libellé Abrégé Liste": "libelle_abrege_liste",
    "Libellé abrégé de liste": "libelle_abrege_liste",
    "Libellé de la Liste": "libelle_etendu_liste",
    "Libellé Liste": "libelle_etendu_liste",
    "Libellé de liste": "libelle_etendu_liste",
    "Nom Tête de Liste": "nom_tete_liste",
    "Voix": "voix",
}
BLOCK_IGNORED = {
    "Résultat candidat",
    "Elu",
    "Sièges",
    "N°Dépôt",
    "N° Dépôt",
    "Code Dépôt",
}


def normalize(header: object) -> str:
    # "Voix 12" -> "Voix", as CSV and recent files number the repeated blocks
    return re.sub(r"\s+\d+$", "", str(header).strip())


def is_ignored(name: str) -> bool:
    # percentages are recomputed from the counts, and trailing empty headers dropped
    return name.startswith("%") or name in ("nan", "") or name in GENERAL_IGNORED


def read_tables(dataset: dict, tmp_dir: Path) -> dict[str, pd.DataFrame]:
    tables = {}
    for resource in dataset["resources"]:
        local_path = tmp_dir / resource["id"]
        download_file(resource["url"], local_path)
        if resource["format"] == "csv":
            tables[resource["title"]] = pd.read_csv(
                local_path, sep=";", header=None, dtype=str
            )
        else:
            tables |= pd.read_excel(local_path, sheet_name=None, header=None, dtype=str)
    return tables


def to_int(values: pd.Series) -> pd.Series:
    cleaned = values.str.replace(r"\s", "", regex=True).str.replace(",", ".")
    return pd.to_numeric(cleaned).round().astype("Int64")


def normalize_departement(code: str) -> str:
    # the source codes are kept as is (ZA, ZZ, 971...), as in the other elections
    code = code.strip()
    return code.zfill(2) if code.isdigit() and len(code) == 1 else code


def parse_table(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    header_row = next(k for k in range(len(raw)) if raw.iloc[k].notna().sum() >= 4)
    names = [normalize(h) for h in raw.iloc[header_row]]
    body = raw.iloc[header_row + 1 :].reset_index(drop=True)
    body = body[body.notna().sum(axis=1) >= 4]
    duplicated = body.duplicated()
    if duplicated.any():
        # e.g. 2014 Maj T1 lists ZT, ZW and ZY twice
        logging.warning(f"Dropping {duplicated.sum()} duplicated rows")
        body = body[~duplicated]

    general = pd.DataFrame(index=body.index)
    block_start = None
    for position, name in enumerate(names):
        if name in GENERAL_ALIASES:
            general[GENERAL_ALIASES[name]] = body.iloc[:, position]
        elif not is_ignored(name):
            block_start = position
            break
    if block_start is None:
        raise ValueError(f"No candidate block found in {names}")

    first_block_name = names[block_start]
    if first_block_name in names[block_start + 1 :]:
        block_size = names.index(first_block_name, block_start + 1) - block_start
    else:
        # 2020: only the first block has headers, the next ones are unnamed
        block_size = names.index("nan", block_start) - block_start
    block_names = names[block_start : block_start + block_size]
    candidats = []
    for start in range(block_start, len(names) - block_size + 1, block_size):
        block = pd.DataFrame(index=body.index)
        for offset, name in enumerate(block_names):
            if name in BLOCK_ALIASES:
                block[BLOCK_ALIASES[name]] = body.iloc[:, start + offset]
            elif not (is_ignored(name) or name in BLOCK_IGNORED):
                raise ValueError(f"Unknown column {name!r}")
        block["code_departement"] = general["code_departement"]
        candidats.append(block[block["voix"].notna()])
    return general, pd.concat(candidats, ignore_index=True)


def ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return (100 * numerator / denominator).round(2)


def build_round(
    tables: dict[str, pd.DataFrame], names: list[str], id_election: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    parsed = [parse_table(tables[name]) for name in names]
    general = pd.concat([g for g, _ in parsed], ignore_index=True)
    candidats = pd.concat([c for _, c in parsed], ignore_index=True)
    for df in (general, candidats):
        df["code_departement"] = df["code_departement"].map(normalize_departement)
        df["id_election"] = id_election
        df["id_brut_miom"] = df["code_departement"]
    duplicates = general["code_departement"][general["code_departement"].duplicated()]
    if not duplicates.empty:
        raise ValueError(
            f"{id_election}: departments in several tables {list(duplicates)}"
        )

    for column in ["inscrits", "abstentions", "votants", "blancs", "nuls", "exprimes"]:
        if column in general:
            general[column] = to_int(general[column])
    if "abstentions" not in general:
        general["abstentions"] = general["inscrits"] - general["votants"]
    for numerator in ["abstentions", "blancs", "nuls", "exprimes"]:
        if numerator in general:
            general[f"ratio_{numerator}_inscrits"] = ratio(
                general[numerator], general["inscrits"]
            )
    general["ratio_votants_inscrits"] = ratio(general["votants"], general["inscrits"])
    for numerator in ["blancs", "nuls", "exprimes"]:
        if numerator in general:
            general[f"ratio_{numerator}_votants"] = ratio(
                general[numerator], general["votants"]
            )

    # majoritarian blocks have a candidate nuance, proportional ones a list nuance
    if "nuance_liste" in candidats:
        candidats["nuance"] = candidats.get("nuance", pd.Series(dtype=str)).fillna(
            candidats.pop("nuance_liste")
        )
    if "sexe" in candidats:
        candidats["sexe"] = candidats["sexe"].replace(SEXES)
    if "nom_tete_liste" in candidats:
        # 2020 prefixes the head of list with a title (M. / Mme)
        candidats["nom_tete_liste"] = candidats["nom_tete_liste"].str.replace(
            r"^(M\.|Mme)\s+", "", regex=True
        )
    candidats["voix"] = to_int(candidats["voix"])
    totals = general.set_index("code_departement")
    candidats["ratio_voix_inscrits"] = ratio(
        candidats["voix"], candidats["code_departement"].map(totals["inscrits"])
    )
    candidats["ratio_voix_exprimes"] = ratio(
        candidats["voix"], candidats["code_departement"].map(totals["exprimes"])
    )
    # in plurinominal majoritarian departments each elector votes for several
    # candidates, so the sum of voix only equals exprimes for proportional lists
    is_list = candidats["nom"].isna()
    check = pd.DataFrame(
        {"voix": candidats[is_list].groupby("code_departement")["voix"].sum()}
    ).join(totals["exprimes"])
    mismatches = check[check["voix"].ne(check["exprimes"]).fillna(True)]
    if not mismatches.empty:
        logging.warning(f"{id_election}: sum of list voix != exprimes\n{mismatches}")
    too_many = candidats[
        ~is_list
        & candidats["voix"].gt(candidats["code_departement"].map(totals["exprimes"]))
    ]
    if not too_many.empty:
        logging.warning(f"{id_election}: candidates with voix > exprimes\n{too_many}")
    return general, candidats


def to_schema(df: pd.DataFrame, scope: str) -> pd.DataFrame:
    unknown = set(df.columns) - set(dtypes[scope])
    if unknown:
        raise ValueError(f"Columns not in the {scope} schema: {unknown}")
    return df.reindex(columns=list(dtypes[scope]))


def build_year(year: int, output_dir: Path, bucket, sources: dict | None) -> None:
    dataset_id, rounds = SENATORIALES[year]
    dataset = get_dataset(dataset_id)
    key = f"{year}_sena"
    with tempfile.TemporaryDirectory() as tmp_dir:
        tables = read_tables(dataset, Path(tmp_dir))
    results = {"general": [], "candidats": []}
    for tour, names in rounds.items():
        general, candidats = build_round(tables, names, f"{key}_{tour}")
        results["general"].append(general)
        results["candidats"].append(candidats)
    for scope, frames in results.items():
        local_path = output_dir / f"{key}_{scope}-results.csv"
        to_schema(pd.concat(frames, ignore_index=True), scope).to_csv(
            local_path, sep=";", index=False
        )
        logging.info(f"{key} {scope} > {local_path}")
        if bucket is not None:
            upload_file(bucket, local_path, source_key(key, scope))
    if sources is not None:
        register_source(
            sources,
            key,
            [f"{key}_{tour}" for tour in rounds],
            dataset_id,
            dataset["last_update"],
        )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, choices=sorted(SENATORIALES))
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="only write the files locally, no upload nor sources.json update",
    )
    parser.add_argument("--output-dir", type=Path, default=Path(tempfile.gettempdir()))
    args = parser.parse_args()
    bucket = None if args.dry_run else get_s3_bucket()
    sources = None if args.dry_run else load_sources()
    for year in [args.year] if args.year else sorted(SENATORIALES):
        build_year(year, args.output_dir, bucket, sources)
    if sources is not None:
        save_sources(sources)


if __name__ == "__main__":
    main()
