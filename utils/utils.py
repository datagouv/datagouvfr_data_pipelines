import duckdb
from datetime import date, datetime


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
    dtype=None,
    columns=None,
    output_name=None,
    output_path=None,
    sep=";",
    compression="snappy",
):
    """
    if dtype is not specified, columns are required to load everything as string (for safety)
    for now keeping snappy compression as more efficient ones are less widespread
    (cf https://github.com/apache/parquet-format/blob/54e6133e887a6ea90501ddd72fff5312b7038a7c/src/main/thrift/parquet.thrift#L457)
    """
    assert dtype is not None or columns is not None
    if output_name is None:
        output_name = csv_file_path.split('/')[-1].replace('.csv', '.parquet')
    if output_path is None:
        output_path = '/'.join(csv_file_path.split('/')[:-1]) + '/'
    print(f"Saving {csv_file_path}")
    print(f"to {output_path + output_name}")
    db = duckdb.read_csv(
        csv_file_path,
        sep=sep,
        dtype=dtype or {c: 'VARCHAR' for c in columns},
    )
    db.write_parquet(output_path + output_name, compression=compression)


def time_is_between(time1, time2):
    # no date involved here
    if time1 > time2:
        time1, time2 = time2, time1
    return time1 <= datetime.now().time() <= time2
