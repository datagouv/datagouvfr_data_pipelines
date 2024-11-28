import json
from datetime import datetime
from typing import Dict, List


def load_json(file_path: str):
    with open(file_path, "r") as f:
        return json.load(f)


def get_missing_geo_files(dict_to_compare: Dict, dict_baseline: Dict) -> List[str]:
    """
    Returns:
        List[str]: list of all the files missing from the dict_to_compare dictionary
    """
    return [
        geo_file_path.split("/")[-1]
        for geo_file_path in set(dict_baseline.keys() - set(dict_to_compare.keys()))
    ]


def get_previous_year_month_string() -> str:
    """
    Returns:
        str: the previous month with the format YYYY-MM
    """
    current_date = datetime.now()
    previous_month = current_date.month - 1
    previous_year = current_date.year
    if previous_month == 0:
        previous_month = 12
        previous_year -= 1
    return f"{previous_year}-{previous_month:02d}"


def compare_stats(
    stats_prev_json, stats_next_json, thresholds: Dict[str, int]
) -> List[str]:
    """
    Compare stats between two JSON files.
    The json looks like:
    [{
        "action": "final",
        "count": 102317,
        "efficacite": 80.94,
        "fichier": "/srv/sirene/data-sirene/data/dep_70.csv",
        "geocode_cache": 96207,
        "geocode_count": 13425,
        "geocode_score_avg": 0.9073401380827498,
        "geocode_score_variance": 0.013120856986200129,
        "housenumber": 62967,
        "interpolation": 478,
        "locality": 939,
        "municipality": 15,
        "poi": 1345,
        "street": 17073,
        "townhall": 0,
        "vide": 0
    }, {...}]

    Args:
        json1 (list): Parsed content of the first JSON file.
        json2 (list): Parsed content of the second JSON file.
        thresholds (dict): Thresholds for acceptable percentage change.

    Returns:
        list: Issues found during comparison.
    """

    stats_prev = {item["fichier"]: item for item in stats_prev_json}
    stats_next = {item["fichier"]: item for item in stats_next_json}

    results = []

    common_files = set(stats_prev.keys()).intersection(stats_next.keys())
    for geo_file_path in common_files:
        geo_file = geo_file_path.split("/")[-1]
        for key in thresholds.keys():
            if key in stats_prev[geo_file_path] and key in stats_next[geo_file_path]:
                value_prev = stats_prev[geo_file_path][key]
                value_next = stats_next[geo_file_path][key]
                if value_prev == 0:  # No division by 0
                    results.append(
                        f"[OK]\t{geo_file}: '{key}' went from {value_prev} to {value_next}"
                    )
                    continue
                delta = (value_next - value_prev) / value_prev * 100
                threshold = thresholds.get(key, 100)
                # Example: a delta of -30% for a threshold of -20% raise an error
                if threshold > delta:
                    results.append(
                        f"[ERROR]\t{geo_file}: '{key}' went from {value_prev} to {value_next} "
                        f"= {delta:.2f}% (threshold: {threshold}%)"
                    )
                else:
                    results.append(
                        f"[OK]\t{geo_file}: '{key}' went from {value_prev} to {value_next} "
                        f"= {delta:.2f}% (threshold: {threshold}%)"
                    )

    # Find missing "fichiers" keys
    missing_in_json_prev = get_missing_geo_files(stats_prev, stats_next)
    if missing_in_json_prev:
        results.append(
            f"[ERROR] Geo files missing in the old stats: {', '.join(missing_in_json_prev)}"
        )

    missing_in_json_next = get_missing_geo_files(stats_next, stats_prev)
    if missing_in_json_next:
        results.append(
            f"[ERROR] Geo files missing in the new stats: {', '.join(missing_in_json_next)}"
        )

    return results


def check_stats_coherence(file_prev: str, file_next: str) -> None:
    file_prev = load_json(file_prev)
    file_next = load_json(file_next)

    thresholds = {
        "count": -5,  # if the new count is under -5% of the previous one then raises an error
        "locality": -10,
        "efficacite": -10,  # efficacite = ratio of successful geocodage
    }

    results = compare_stats(file_prev, file_next, thresholds)

    error_code = 0
    for result in results:
        print(result)
        if result.startswith("[ERROR]"):
            error_code = 1

    if error_code:
        print("Stats are not coherent.")
    else:
        print("Stats are coherent.")

    # TODO: After running at least once in production
    # Change the print code to:
    # exit(error_code)
    print(f"Error Code: {error_code}")


if __name__ == "__main__":
    previous_month_string = get_previous_year_month_string()
    file_prev = f"/srv/sirene/data-sirene/{previous_month_string}/stats.json"
    file_next = "/srv/sirene/data-sirene/stats.json"
    check_stats_coherence(file_prev, file_next)
