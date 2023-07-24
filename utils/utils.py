from datetime import date


def check_if_monday():
    if date.today().weekday() == 0:
        return True
    else:
        return False


def check_if_first_day_of_month():
    if date.today().day == 1:
        return True
    else:
        return False


def get_fiscal_year(date):
    # Get the fiscal year based on the month of the date
    return date.year if date.month >= 7 else date.year - 1
