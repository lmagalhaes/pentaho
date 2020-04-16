import decimal
import glob
import gzip
import os
from datetime import datetime
from multiprocessing.pool import Pool
from time import time

import click
from dateutil.rrule import rrule, MONTHLY


ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
ATTRIBUTE_SEPARATOR = "|"

ORIGINALS_PATH = os.path.join(ROOT_DIR, 'originals')
ZIPPED_PATH = os.path.join(ROOT_DIR, 'zipped')
PARSED_PATH = os.path.join(ROOT_DIR, 'parsed')
# for is a good number because it reduces the file size by more than 60% while keeps the compressing time to almost 1/2
GZIP_COMPRESSION_LEVEL = 3
NUMBER_OF_WORKERS = os.cpu_count()
DATE_FORMAT = '%Y%m'

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def extract_file_name(file_path):
    return os.path.basename(file_path)


def parse_file(filename, workers):
    with gzip.GzipFile(filename, 'r') as file_:
        return Pool(processes=workers).map(parse_line, file_.readlines())


def parse_line(line):
    return '{}{}'.format(
        ATTRIBUTE_SEPARATOR.join(list(map(
            lambda attr: str(round_score(attr, 10)) if is_number(attr) else attr,
            line.decode().strip().split(ATTRIBUTE_SEPARATOR)
        ))),
        os.linesep).encode()


def round_score(score, decimal_points=8):
    decimal.getcontext().rounding = decimal.ROUND_DOWN
    score = decimal.Decimal(score)

    return float(round(score, decimal_points))


def get_months_to_import(dates, date_format):

    def get_dates(initial_date, final_date):
        return [
            date_.strftime(date_format) for date_ in rrule(MONTHLY, dtstart=initial_date, until=final_date)
        ]

    if dates == 'all':
        initial_date = datetime.strptime('201805', date_format)
        final_date = datetime.now()

        return get_dates(initial_date, final_date)

    all_dates = dates.split(',')
    months_to_import = list(filter(lambda date: '-' not in date, all_dates))
    range_dates = filter(lambda date: '-' in date, all_dates)
    for date_range in range_dates:
        lower_range, upper_range = date_range.split('-')
        initial_date = datetime.strptime(lower_range, date_format)
        final_date = datetime.strptime(upper_range, date_format)
        months_to_import += get_dates(initial_date, final_date)

    return months_to_import


@click.group('process-cli')
def process_cli():
    pass


@process_cli.command('process')
@click.option('--workers', '-w', type=int, default=NUMBER_OF_WORKERS, help=f'Number of works to process file. Defaults: {NUMBER_OF_WORKERS} - total of CPUS on the machine')
@click.option('--compression-level', '-c', type=int, default=GZIP_COMPRESSION_LEVEL, help=f'Gzip compression level (0-9) defaults: {GZIP_COMPRESSION_LEVEL}')
@click.option('--dates', default='all', type=str, help='Date to migrate in the "YYYYMM" format. Multiple date separated by "," or range of dates separated by "-"')
@click.option('--date-format', default=DATE_FORMAT, type=str, help=f'Format in which dates will be input and searched for. Should be compatible with Python datetime format. Default {DATE_FORMAT}. ')
def process(workers, compression_level, dates, date_format):
    """
    Process original files downloaded using the upload command.
    """
    files = []
    months_to_import = get_months_to_import(dates=dates, date_format=date_format)
    for month in months_to_import:
        pattern = os.path.join(ORIGINALS_PATH, 'configs', month, '*.csv.gz')
        files += glob.glob(pattern)

    required_folders = [
        ORIGINALS_PATH, ZIPPED_PATH, PARSED_PATH
    ]

    for folder in required_folders:
        if not os.path.isdir(folder):
            os.makedirs(folder)

    t1 = time()
    for config_file in files:
        print(f'Parsing file: {config_file}')
        filename = extract_file_name(config_file)

        parsed_file_path = os.path.join(PARSED_PATH, filename)
        p1 = time()
        with gzip.GzipFile(parsed_file_path, 'w', compresslevel=compression_level) as parsed_file:
            parsed_lines = parse_file(config_file, workers=workers)
            parsed_file.writelines(parsed_lines)
        p2 = time()
        print(f'Parse complete in {p2 - p1:.2f}s.')

    print(f'Total files parsed: {len(files)}')
    print(f'Elapsed time: {time()-t1:.2f}s')


if __name__ == '__main__':
    process_cli()
