from datetime import datetime
from dateutil.parser import parse
import json
import logging

import pandas as pd
import requests

from datagouvfr_data_pipelines.utils.mattermost import send_message

max_csvgz_size = 104857600


def better_parse(date:str) -> datetime | None:
    try:
        return parse(date)
    except Exception:
        return None


def get_duration_between(extras: dict, start_key: str, end_key: str):
    if not (start_key in extras and end_key in extras):
        return None
    start = better_parse(extras[start_key])
    end = better_parse(extras[end_key])
    if start is None or end is None:
        return None
    return (
        duration
        # considering that > ~2 weeks is abnormal for stats
        if 0 < (duration := (end - start).total_seconds()) < 1e6
        else None
    )


def score(num: int, denom: int) -> float:
    return round(num / denom * 100, 1)


def resource_url(row: pd.Series):
    return f"https://www.data.gouv.fr/datasets/{row['dataset.id']}/#/resources/{row['id']}"


def process_catalog():
    logging.info("Loading catalog...")
    full_catalog = pd.read_csv(
        "https://www.data.gouv.fr/api/1/datasets/r/4babf5f2-6a9c-45b5-9144-ca5eae6a7a6d",
        sep=";",
        dtype=str,
        usecols=[
            "id",
            "dataset.id",
            "filesize",
            "format",
            "type",
            "preview_url",
            "title",
            "extras",
        ],
    )
    logging.info("Processing catalog...")
    full_catalog["has_preview"] = pd.notna(full_catalog["preview_url"])
    tabular_formats = ["csv", "xlsx", "xls"]
    catalog = full_catalog.loc[full_catalog["format"].isin(tabular_formats)]
    catalog["extras"] = catalog["extras"].apply(json.loads)
    catalog["filesize"] = catalog["filesize"].apply(
        lambda fz: int(fz) if not pd.isna(fz) else 0
    )
    nb_tabular_files = len(catalog)
    catalog = catalog.loc[catalog["extras"].apply(
        lambda extras: extras.get("check:available", False)
    )]
    nb_available = len(catalog)
    should_be_previz = len(catalog.loc[catalog["filesize"] <= max_csvgz_size])
    preview_per_format = catalog.groupby(
        ["format", "has_preview"]
    ).size().reset_index(name="count")
    no_preview_no_error = catalog.loc[
        (~catalog["has_preview"])
        & (~catalog["extras"].apply(
            lambda extras: (
                bool(extras.get("analysis:parsing:error"))
                | bool(extras.get("analysis:error"))
            )
        ))
    ]
    catalog["analysis_error"] = catalog["extras"].apply(
        lambda extras: extras.get("analysis:error", pd.NA)
    )
    catalog["parsing_error"] = catalog["extras"].apply(
        lambda extras: extras.get("analysis:parsing:error", pd.NA)
    )
    catalog["delay_to_analysis"] = catalog["extras"].apply(
        lambda extras: get_duration_between(
            extras,
            "analysis:last-modified-at",
            "analysis:parsing:started_at",
        )
    )
    catalog["analysis_duration"] = catalog["extras"].apply(
        lambda extras: get_duration_between(
            extras,
            "analysis:parsing:started_at",
            "analysis:parsing:finished_at",
        )
    )

    logging.info("Getting hydra exceptions...")
    hydra_exceptions = requests.get(
        "https://crawler.data.gouv.fr/api/resources-exceptions/"
    ).json()

    logging.info("Building message...")
    message = "#### Monitoring de la prévisualisation :hydra:\n"
    message += f"{nb_tabular_files} fichiers tabulaires\n"
    message += f"dont {nb_available} accessibles ({score(nb_available, nb_tabular_files)}%)\n"
    message += f"dont {should_be_previz} sous la limite de taille ({score(should_be_previz, nb_tabular_files)}%)\n"
    have_previz = sum(catalog['has_preview'])
    message += f"**{have_previz} fichiers prévisualisables ({score(have_previz, nb_tabular_files)}%)** :\n"
    for _format in tabular_formats:
        row = preview_per_format.loc[
            (preview_per_format["format"] == _format)
            & preview_per_format["has_preview"]
        ].iloc[0]
        prop = row['count'] / preview_per_format.loc[
            (preview_per_format['format'] == _format), 'count'
        ].sum()
        message += f"- {row['count']} {_format} ({round(prop * 100, 1)}% des {_format})\n"
    message += "\n"

    if len(no_preview_no_error):
        message += f"{len(no_preview_no_error)} sans previz mais sans erreur ([exemple]({resource_url(no_preview_no_error.iloc[0])}))\n"
    message += "##### Délai entre maj et analyse :\n"
    message += f"- moyenne : {round(catalog['delay_to_analysis'].mean(), 1)}s\n"
    message += f"- médiane : {round(catalog['delay_to_analysis'].median(), 1)}s\n"
    message += "##### Durée d'analyse des fichiers :\n"
    message += f"- moyenne : {round(catalog['analysis_duration'].mean(), 1)}s\n"
    message += f"- médiane : {round(catalog['analysis_duration'].median(), 1)}s\n"

    errors = {
        "analysis_error": "de l'analyse de la ressource",
        "parsing_error": "du traitement de la ressource",
    }
    for error_type, label in errors.items():
        _errors = catalog.dropna(subset=error_type)
        if _errors[error_type].nunique():
            message += f"##### Erreurs lors {label} :\n"
            for error in _errors[error_type].unique():
                restr = _errors.loc[_errors[error_type] == error]
                if len(restr) < 5:
                    continue
                message += f"- {len(restr)} `{error}` ([exemple]({resource_url(restr.iloc[0])}))\n"
        del _errors

    exceptions_alert = ""
    for exception in hydra_exceptions:
        row = full_catalog.loc[full_catalog["id"] == exception["resource_id"]]
        if len(row) == 0:
            exceptions_alert += f"- [{exception['comment']}](https://www.data.gouv.fr/api/2/datasets/resources/{exception['resource_id']}/) not in catalog anymore\n"
            continue
        row = row.iloc[0]
        if not row["has_preview"] and "analysis:parsing:pmtiles_url" not in row["extras"]:
            if not exceptions_alert:
                exceptions_alert = "\n##### Certaines exceptions ne sont plus prévisualisables :alert:\n"
            exceptions_alert += f"- [{exception['comment']}](https://www.data.gouv.fr/api/2/datasets/resources/{exception['resource_id']}/)\n"

    send_message(message + exceptions_alert)
