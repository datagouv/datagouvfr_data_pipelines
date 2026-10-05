"""Correspondence table between the communes of each election and the current
commune geography, built from the INSEE annual correspondence table.

Pure functions (no Airflow), called by the build_correspondence_table task.
"""

import io
import re
import unicodedata
import zipfile

import pandas as pd
import requests

INSEE_PAGE_URL = "https://www.insee.fr/fr/information/7671867"
INSEE_FILE_PATTERN = (
    r"/fr/statistiques/fichier/7671867/table_passage_annuelle_(\d{4})\.zip"
)
FIRST_MILLESIME = 2003
# codes outside the INSEE commune geography: French people abroad, overseas
# collectivities (Polynesia, New Caledonia...), Saint-Pierre-et-Miquelon,
# Saint-Barthélemy and Saint-Martin (also under their pre-2007 Guadeloupe codes)
OUT_OF_SCOPE_PREFIXES = ("ZZ", "98", "975", "977", "978", "97123", "97127")
# a neighbouring millesime matching better by this many points signals that the
# ministry used another geography than the one of the election year
MILLESIME_TOLERANCE = 0.5
OUTPUT_COLUMNS = [
    "id_election",
    "code_departement",
    "code_commune",
    "libelle_commune",
    "code_commune_actuel",
    "libelle_commune_actuel",
    "nb_communes_actuelles",
    "methode_rapprochement",
    "millesime_cog_election",
]


def get_latest_annual_table() -> tuple[str, int]:
    # the INSEE page lists one annual table per year, we take the most recent one
    response = requests.get(INSEE_PAGE_URL, timeout=60)
    response.raise_for_status()
    years = [int(y) for y in re.findall(INSEE_FILE_PATTERN, response.text)]
    if not years:
        raise ValueError(f"No annual correspondence table found on {INSEE_PAGE_URL}")
    year = max(years)
    return (
        f"https://www.insee.fr/fr/statistiques/fichier/7671867/table_passage_annuelle_{year}.zip",
        year,
    )


def load_annual_table(url: str) -> tuple[pd.DataFrame, str | None]:
    """Returns the table and its publication date as stated by INSEE (dd/mm/yyyy)."""
    # the zip holds a single xlsx, whose COM sheet is the correspondence table
    response = requests.get(url, timeout=300)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        name = next(n for n in archive.namelist() if n.endswith(".xlsx"))
        raw = pd.read_excel(
            archive.open(name), sheet_name="COM", header=None, dtype=str
        )
    # the header row (NIVGEO, CODGEO_2003...) comes after a few lines of titles,
    # one of which reads "Mise en ligne le 19/03/2026"
    header_row = raw.index[raw.iloc[:, 0] == "NIVGEO"][0]
    titles = " ".join(raw.iloc[:header_row].stack().astype(str))
    published = re.search(r"Mise en ligne le (\d{2}/\d{2}/\d{4})", titles)
    # one row per current commune, with its code and label for every year since 2003
    table = raw.iloc[header_row + 1 :].set_axis(raw.iloc[header_row], axis=1)
    # communes and the municipal arrondissements of Paris, Lyon and Marseille
    return (
        table[table["NIVGEO"].isin(["COM", "ARM"])].reset_index(drop=True),
        published.group(1) if published else None,
    )


def election_millesime(id_election: str, latest_year: int) -> int:
    # "2014_euro_t1" -> 2014; elections before 2003 use the oldest available geography
    return min(max(int(id_election[:4]), FIRST_MILLESIME), latest_year)


def millesime_mapping(
    table: pd.DataFrame, millesime: int, latest_year: int
) -> pd.DataFrame:
    """Each code of the millesime -> one or several current communes (splits)."""
    # a code shared by several rows (several current communes) is a split commune
    return (
        table[[f"CODGEO_{millesime}", f"CODGEO_{latest_year}", f"LIBGEO_{latest_year}"]]
        .dropna(subset=[f"CODGEO_{millesime}"])
        .drop_duplicates()
        .set_axis(["code", "code_commune_actuel", "libelle_commune_actuel"], axis=1)
    )


def find_fallback(
    code: str, label: str, millesime: int, table: pd.DataFrame, latest_year: int
) -> tuple[str, int, str] | None:
    """For a code unknown in its millesime: (code to use, millesime to use, method)."""
    # no current commune to look for (abroad, overseas collectivities...)
    if code.startswith(OUT_OF_SCOPE_PREFIXES):
        return None
    # 2. a unique commune with the same label in the millesime, e.g. communes nouvelles
    # spanning two departments that the ministry codes in the other department
    label = normalize_label(label)
    if label:
        same_label = (
            table.loc[
                table[f"LIBGEO_{millesime}"].map(normalize_label) == label,
                f"CODGEO_{millesime}",
            ]
            .dropna()
            .unique()
        )
        # only when unambiguous, homonyms (several "Saint-Martin"...) are skipped
        if len(same_label) == 1:
            return same_label[0], millesime, "libelle"
    # 3. the closest millesime where the code exists, e.g. an old code still in use
    # search outwards: millesime -1, +1, -2, +2...
    for distance in range(1, latest_year - FIRST_MILLESIME + 1):
        for other in (millesime - distance, millesime + distance):
            if (
                FIRST_MILLESIME <= other <= latest_year
                and (table[f"CODGEO_{other}"] == code).any()
            ):
                return code, other, "millesime_voisin"
    # nothing found: the commune stays without a current commune
    return None


def build_table_passage(
    communes: pd.DataFrame, table: pd.DataFrame, latest_year: int
) -> pd.DataFrame:
    """communes: one row per (id_election, code_departement, code_commune,
    libelle_commune); returns one row per (id_election, code_commune,
    code_commune_actuel)."""
    # which INSEE year to read the code in, for each election
    communes = communes.assign(
        millesime=communes["id_election"].map(
            lambda i: election_millesime(i, latest_year)
        )
    )
    parts = []
    # all the elections of a same year share the same lookup
    for millesime, group in communes.groupby("millesime"):
        mapping = millesime_mapping(table, millesime, latest_year)
        # 1. the code in the millesime of the election year
        # inner join: a split commune gets one row per current commune
        found = group.merge(mapping, left_on="code_commune", right_on="code")
        found["methode_rapprochement"] = "code"
        # the INSEE millesime whose codes were used to map the election commune
        found["millesime_cog_election"] = millesime
        parts.append(found)
        # 2. and 3. the few codes unknown that year, handled one by one
        missing = group[~group["code_commune"].isin(mapping["code"])]
        for row in missing.itertuples(index=False):
            fallback = find_fallback(
                row.code_commune, row.libelle_commune, millesime, table, latest_year
            )
            row_df = pd.DataFrame([row._asdict()])
            if fallback is None:
                # kept with empty current commune columns
                parts.append(
                    row_df.assign(
                        methode_rapprochement=None, millesime_cog_election=None
                    )
                )
                continue
            # map the code found by the fallback, in the millesime it was found in
            code, other_millesime, method = fallback
            other = millesime_mapping(table, other_millesime, latest_year)
            parts.append(
                row_df.assign(code=code)
                .merge(other, on="code")
                .assign(
                    methode_rapprochement=method, millesime_cog_election=other_millesime
                )
            )
    result = pd.concat(parts, ignore_index=True)
    # number of rows of each election commune: the coefficient to divide its counts by
    result["nb_communes_actuelles"] = result.groupby(["id_election", "code_commune"])[
        "code_commune"
    ].transform("size")
    # nullable integer, so that the CSV reads 2014 and not 2014.0
    result["millesime_cog_election"] = result["millesime_cog_election"].astype("Int64")
    return result[OUTPUT_COLUMNS].sort_values(
        ["id_election", "code_commune", "code_commune_actuel"], ignore_index=True
    )


def normalize_label(label: object) -> str | None:
    if not isinstance(label, str):
        return None
    # old files put the article at the end: "Oudon (L')" -> "L'Oudon"
    label = re.sub(r"^(.*?)\s*\((L'|Le|La|Les)\)\s*$", r"\2 \1", label.strip())
    # "Saint-André-d'Huiriat" and "ST ANDRE D HUIRIAT" -> "saintandredhuiriat"
    ascii_label = (
        unicodedata.normalize("NFKD", label).encode("ascii", "ignore").decode()
    )
    ascii_label = re.sub(
        r"\bste\b", "sainte", re.sub(r"\bst\b", "saint", ascii_label.lower())
    )
    return re.sub(r"[^a-z0-9]", "", ascii_label)


def label_agreement(
    communes: pd.DataFrame, table: pd.DataFrame, millesime: int
) -> float:
    """Share (%) of the election communes whose label matches the millesime label."""
    # code -> normalized INSEE label of that year
    labels = dict(
        zip(
            table[f"CODGEO_{millesime}"],
            table[f"LIBGEO_{millesime}"].map(normalize_label),
        )
    )
    ours = communes["libelle_commune"].map(normalize_label)
    theirs = communes["code_commune"].map(labels)
    return round(100 * ((ours == theirs) & ours.notna()).sum() / len(communes), 2)


def check_millesimes(
    communes: pd.DataFrame, table: pd.DataFrame, latest_year: int
) -> list[str]:
    """Alerts on elections whose labels match a neighbouring millesime better than the
    one of their year, i.e. the ministry used another geography."""
    alerts = []
    for id_election, group in communes.groupby("id_election"):
        millesime = election_millesime(id_election, latest_year)
        expected = label_agreement(group, table, millesime)
        # the year before and after should match worse than the election year
        for neighbour in (millesime - 1, millesime + 1):
            if FIRST_MILLESIME <= neighbour <= latest_year:
                other = label_agreement(group, table, neighbour)
                if other > expected + MILLESIME_TOLERANCE:
                    alerts.append(
                        f"{id_election} : les libellés collent mieux au COG {neighbour}"
                        f" ({other} %) qu'au COG {millesime} ({expected} %)"
                    )
    return alerts


def check_unmapped(result: pd.DataFrame) -> list[str]:
    """Alerts on the communes left without a current commune, besides the codes
    known to be outside the INSEE geography and the elections before the first
    millesime, whose communes merged before 2003 cannot be mapped."""
    # rows without a current commune that we could have expected to map
    unmapped = result[
        result["code_commune_actuel"].isna()
        & ~result["code_commune"].str.startswith(OUT_OF_SCOPE_PREFIXES)
        & (result["id_election"].str[:4].astype(int) >= FIRST_MILLESIME)
    ]
    return [
        f"{id_election} : {len(group)} communes sans correspondance"
        f" ({', '.join(group['code_commune'] + ' ' + group['libelle_commune'].fillna(''))})"
        for id_election, group in unmapped.groupby("id_election")
    ]
