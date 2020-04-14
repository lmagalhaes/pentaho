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
        return Pool(os.cpu_count()).map(parse_line, file_.readlines())


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

    for config_file in files:
        print('Parsing file: {}'.format(config_file))
        filename = extract_file_name(config_file)

        parsed_file_path = os.path.join(PARSED_PATH, filename)
        t1 = time()
        with gzip.GzipFile(parsed_file_path, 'w', compresslevel=GZIP_COMPRESSION_LEVEL) as parsed_file:
            parsed_lines = parse_file(config_file)
            parsed_file.writelines(parsed_lines)
        t2 = time()
        print("Parse complete in {:.2f}s.".format(t2 - t1))
