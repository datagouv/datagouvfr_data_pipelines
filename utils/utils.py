from datetime import date, datetime
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
