"""Description of the dataset: the manual text of description.yaml followed by the list
of the sources, generated from sources.json (no Airflow, called by the
publish_description task)."""

DATASET_URL = "https://www.data.gouv.fr/datasets/{}"


def sort_key(key: str, source: dict) -> tuple:
    # by date of the (first) round; same day, e.g. régionales and départementales 2021:
    # by key
    if "date" not in source["description"]:
        raise ValueError(f"{key}: no description.date in sources.json")
    return (source["description"]["date"], key)


def source_line(source: dict) -> str:
    description = source["description"]
    dataset_id = (
        source["resultats"]["source_dataset_id"]
        if "resultats" in source
        else description["source_dataset_id"]
    )
    link = f"[{description['libelle']}]({DATASET_URL.format(dataset_id)})"
    # the elections that are not integrated are struck through
    line = f"- {link}" if "resultats" in source else f"- ~~{link}~~"
    if description.get("commentaire"):
        line += f" ({description['commentaire']})"
    return line


def build_description(texts: dict, sources: dict) -> str:
    # most recent first
    keys = sorted(sources, key=lambda key: sort_key(key, sources[key]), reverse=True)
    lines = [source_line(sources[key]) for key in keys]
    description = (
        texts["introduction"].rstrip()
        + "\n\n"
        + texts["titre_sources"]
        + "\n"
        + "\n".join(lines)
        + "\n"
    )
    # manual history of the important changes, after the list of the sources
    if texts.get("historique"):
        description += "\n" + texts["historique"].rstrip() + "\n"
    return description
