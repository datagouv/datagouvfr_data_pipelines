import gzip
from datetime import date, datetime
import logging

import duckdb
from dateutil.relativedelta import relativedelta

MOIS_FR = {
    "01": "janvier",
    "02": "février",
    "03": "mars",
    "04": "avril",
    "05": "mai",
    "06": "juin",
    "07": "juillet",
    "08": "août",
    "09": "septembre",
    "10": "octobre",
    "11": "novembre",
    "12": "décembre",
}


def check_if_monday():
    return date.today().weekday() == 0


def check_if_first_day_of_month():
    return date.today().day == 1


def check_if_first_day_of_year():
    return date.today().day == 1 and date.today().month == 1


def get_fiscal_year(date):
    # Get the fiscal year based on the month of the date
    return date.year if date.month >= 7 else date.year - 1


def csv_to_parquet(
    csv_file_path: str,
    dtype: dict | None = None,
    columns: list | None = None,
    output_name: str | None = None,
    output_path: str | None = None,
    sep: str = ";",
    compression: str = "zstd",
    **kwargs,
):
    """
    if dtype is not specified, columns are required to load everything as string (for safety)
    for allowed types see https://duckdb.org/docs/sql/data_types/overview.html
    """
    assert dtype is not None or columns is not None
    if output_name is None:
        output_name = csv_file_path.split("/")[-1].replace(".csv", ".parquet")
    if output_path is None:
        output_path = "/".join(csv_file_path.split("/")[:-1]) + "/"
    logging.info(f"Converting {csv_file_path}")
    db = duckdb.read_csv(
        csv_file_path,
        sep=sep,
        dtype=dtype or {c: "VARCHAR" for c in columns},
        **kwargs,
    )
    logging.info(f"to {output_path + output_name}")
    db.write_parquet(output_path + output_name, compression=compression)


def csv_to_csvgz(
    csv_file_path: str,
    output_name: str | None = None,
    output_path: str | None = None,
    chunk_size: int = 1024 * 1024,
):
    if output_name is None:
        output_name = csv_file_path.split("/")[-1].replace(".csv", ".csv.gz")
    if output_path is None:
        output_path = "/".join(csv_file_path.split("/")[:-1]) + "/"
    logging.info(f"Converting {csv_file_path}")
    logging.info(f"to {output_path + output_name}")
    with (
        open(csv_file_path, "r", newline="", encoding="utf-8") as csvfile,
        gzip.open(output_path + output_name, "wt", newline="", encoding="utf-8") as gzfile,
    ):
        while True:
            chunk = csvfile.read(chunk_size)
            if not chunk:
                break
            gzfile.write(chunk)


def time_is_between(time1, time2):
    # no date involved here
    if time1 > time2:
        time1, time2 = time2, time1
    return time1 <= datetime.now().time() <= time2


def get_unique_list(*lists: list[str]) -> list[str]:
    """
    Returns a list of unique string elements from multiple input lists.

    Args:
        *lists (List[str]): An arbitrary number of string lists of elements.

    Returns:
        list[str]: The list with unique elements in no particular order.
    """
    unique_elements = set()
    for lst in lists:
        unique_elements.update(lst)
    return list(unique_elements)


def list_months_between(start_date: datetime, end_date: datetime) -> list[str]:
    """
    Generate a list of month strings between two dates.
    Args:
        start_date (datetime): The start date.
        end_date (datetime): The end date.
    Returns:
        list[str]: A list of strings representing each month between the start and end dates included in the format 'YYYY-MM'.
    """

    start_date = start_date.replace(day=1).date()
    end_date = end_date.replace(day=1).date()

    months = []
    current_date = start_date

    while current_date <= end_date:
        months.append(current_date.strftime("%Y-%m"))
        current_date += relativedelta(months=1)

    return months
