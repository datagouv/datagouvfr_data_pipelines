from datetime import date
import duckdb


def check_if_monday():
    return date.today().weekday() == 0


def check_if_first_day_of_month():
    return date.today().day == 1


def check_if_first_day_of_year():
    return date.today().day == 1 and date.today().month == 1


def get_fiscal_year(date):
    # Get the fiscal year based on the month of the date
    return date.year if date.month >= 7 else date.year - 1


def month_year_iter(start_month, start_year, end_month, end_year):
    # includes both start and end months
    ym_start = 12 * start_year + start_month - 1
    ym_end = 12 * end_year + end_month - 1
    for ym in range(ym_start, ym_end + 1):
        y, m = divmod(ym, 12)
        yield y, m + 1


def csv_to_parquet(
    csv_file_path,
    columns,
    output_name=None,
    output_path=None,
    sep=";",
):
    """
    columns are required to load everything as string (for safety)
    """
    if output_name is None:
        output_name = csv_file_path.split('/')[-1].replace('.csv', '.parquet')
    if output_path is None:
        output_path = '/'.join(csv_file_path.split('/')[:-1]) + '/'
    print(f"Saving {csv_file_path}")
    print(f"to {output_path + output_name}")
    db = duckdb.read_csv(
        csv_file_path,
        sep=sep,
        dtype={c: 'VARCHAR' for c in columns},
    )
    db.write_parquet(output_path + output_name)
