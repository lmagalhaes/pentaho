import os
from glob import glob
import decimal
import gzip
from multiprocessing.pool import Pool
from time import time


ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
ATTRIBUTE_SEPARATOR = "|"

ORIGINALS_PATH = os.path.join(ROOT_DIR, 'originals')
ZIPPED_PATH = os.path.join(ROOT_DIR, 'zipped')
PARSED_PATH = os.path.join(ROOT_DIR, 'parsed')
# for is a good number because it reduces the file size by more than 60% while keeps the compressing time to almost 1/2
GZIP_COMPRESSION_LEVEL = 4
NUMBER_OF_WORKERS = os.cpu_count()


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def extract_file_name(file_path):
    return os.path.basename(file_path)


def parse_file(filename):
    with gzip.GzipFile(filename, 'r') as file_:
        return Pool(processes=NUMBER_OF_WORKERS).map(parse_line, file_.readlines())


def parse_line(line):
    return ATTRIBUTE_SEPARATOR.join(list(map(
        lambda attr: str(round_score(attr, 10)) if is_number(attr) else attr,
        line.decode().strip().split(ATTRIBUTE_SEPARATOR)
    ))).encode()


def round_score(score, decimal_points=8):
    decimal.getcontext().rounding = decimal.ROUND_DOWN
    score = decimal.Decimal(score)

    return float(round(score, decimal_points))


if __name__ == '__main__':

    required_folders = [
        ORIGINALS_PATH, ZIPPED_PATH, PARSED_PATH
    ]

    for folder in required_folders:
        if not os.path.isdir(folder):
            os.makedirs(folder)

    files = glob(os.path.join(ORIGINALS_PATH, 'configs', '201805', '*.csv.gz'))
    t1 = time()
    for config_file in files:
        print(f'Parsing file: {config_file}')
        filename = extract_file_name(config_file)

        parsed_file_path = os.path.join(PARSED_PATH, filename)
        p1 = time()
        with gzip.GzipFile(parsed_file_path, 'w', compresslevel=GZIP_COMPRESSION_LEVEL) as parsed_file:
            parsed_lines = parse_file(config_file)
            parsed_file.writelines(parsed_lines)
        p2 = time()
        print(f'Parse complete in {p2 - p1:.2f}s.')

    print(f'Total files parsed: {len(files)}')
    print(f'Elapsed time: {time()-t1:.2f}s')
