import os
from glob import glob
import decimal
import shutil
import gzip


ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
ATTRIBUTE_SEPARATOR = "|"

ORIGINALS_PATH = os.path.join(ROOT_DIR, 'originals')
ZIPPED_PATH = os.path.join(ROOT_DIR, 'zipped')
PARSED_PATH = os.path.join(ROOT_DIR, 'parsed')


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def extract_file_name(file_path):
    return os.path.basename(file_path)


def compress_file(original):
    gzip_filename = '{}.gz'.format(
        extract_file_name(original)
    )
    print('Compressing file {}'.format(gzip_filename))
    gzip_file_path = os.path.join(ZIPPED_PATH, gzip_filename)
    with gzip.open(gzip_file_path, 'w', compresslevel=9) as gzip_file:
        with open(original, 'rb') as original_file:
            shutil.copyfileobj(original_file, gzip_file)


def round_score(score, decimal_points=8):
    decimal.getcontext().rounding = decimal.ROUND_DOWN
    score = decimal.Decimal(score)

    return float(round(score, decimal_points))


if __name__ == '__main__':

    required_folders = [
        ORIGINALS_PATH, ZIPPED_PATH, PARSED_PATH
    ]

    files = glob(os.path.join(ORIGINALS_PATH, '*.csv'))

    for config_file in files:
        filename = extract_file_name(config_file)

        parsed_file_path = os.path.join(PARSED_PATH, filename)

        with open(config_file, 'r') as config:
            print('Parsing file {}'.format(config_file))
            with open(parsed_file_path, 'w') as parsed_file:
                for line in config.readlines():

                    line = ATTRIBUTE_SEPARATOR.join(list(map(
                        lambda attr: str(round_score(attr, 10)) if is_number(attr) else attr,
                        line.strip().split(ATTRIBUTE_SEPARATOR)
                    )))

                    parsed_file.write('{}{}'.format(line, os.linesep))
            print("Parse complete.")
        compress_file(parsed_file_path)
