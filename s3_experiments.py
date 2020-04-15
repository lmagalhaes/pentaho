import os
from multiprocessing.dummy import Pool as ThreadPool
from time import time

from boto3 import Session

from convert_file import ORIGINALS_PATH, PARSED_PATH, DATE_FORMAT, extract_file_name, get_months_to_import

import glob

import click

aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
region_name = os.getenv('REGION_NAME')
profile_name = os.getenv('AWS_PROFILE_NAME')
sk_bucket_name = os.getenv('SK_BUCKET_NAME')
sk_destination_bucket = '3ebdg67-dwh'

session = Session(
    # aws_secret_access_key=aws_secret_access_key,
    # aws_access_key_id=aws_access_key_id,
    profile_name=profile_name,
    region_name=region_name
)


def download_file(item, destination_folder, s3_client):
    key = item['Key']
    filename = key.split(os.path.sep)[-1]

    try:
        destination_file = os.path.join(destination_folder, filename)
        # print('Downloading {} to {}'.format(key, destination_file))
        s3_client.download_file(Bucket=sk_bucket_name, Key=key, Filename=destination_file)
    except Exception as e:
        print('Failed to download file: {}'.format(key))
        print(str(e))
    # print('finilising processing {}:'.format(key))


def upload_file(file_path, s3_client):
    destination_bucket = '3ebdg67-dwh'
    filename = extract_file_name(file_path)

    prefix = 'search_keeper_dev/configs/'
    key = '{}{}'.format(prefix, filename)
    print(f'Uploading {key}')
    s3_client.upload_file(Filename=file_path, Bucket=destination_bucket, Key=key)
    print(f'{key} uploaded successfully')


@click.group(name='s3')
def s3_manager():
    pass


@click.command(name='upload')
@click.option('--dates', default='all', type=str, help='Date to migrate in the "YYYYMM" format. Multiple date separated by "," or range of dates separated by "-"')
def push_to_s3(dates):
    click.echo("Entering upload to s3")
    months_to_import = get_months_to_import(dates, DATE_FORMAT)

    files = []
    for month in months_to_import:
        pattern = f'{PARSED_PATH}/*{month}*'
        files += glob.glob(pattern)

    print(f'Uploading {len(files)} files for range {dates}')
    inputs = [[file_, client] for file_ in files]
    t1 = time()
    pool = ThreadPool()
    pool.starmap(upload_file, inputs)
    t2 = time()
    print(f'Uploaded finished in {t2-t1:.2f}s')


@click.command(name='download')
@click.option('--dates', default='all', type=str, help='Date to migrate in the "YYYYMM" format. Multiple date separated by "," or range of dates separated by "-"')
def download_from_s3(dates: str):
    click.echo("Entering download from s3")
    click.echo(f'Downloading "{dates}" files')

    months_to_import = get_months_to_import(dates, DATE_FORMAT)
    total_files = 0
    initial_time = time()
    for month in months_to_import:
        prefix = 'search_keeper/configs/{}'.format(month)
        items = client.list_objects_v2(Bucket=sk_bucket_name, Prefix=prefix)

        splitted_key = prefix.split(os.path.sep)[1:]
        destination_folder = os.path.join(ORIGINALS_PATH, *splitted_key)

        if not os.path.isdir(destination_folder):
            print("Creating destination folder: {}".format(destination_folder))
            os.makedirs(destination_folder)
        total_files_per_month = len(items['Contents'])
        total_files += total_files_per_month
        inputs = [[content, destination_folder, client] for content in items['Contents']]

        print(f'Downloading {total_files_per_month} files for {month}')
        total_processes = 4
        t1 = time()
        pool = ThreadPool(total_processes)
        pool.starmap(download_file, inputs)
        pool.close()
        t2 = time()

        print("Items downladed in {:.2f} s".format(t2 - t1))
    final_time = time()
    print('A total of {} items downloaded in {:.2f} s'.format(total_files, (final_time - initial_time)))


if __name__ == '__main__':
    client = session.client('s3')

    s3_manager.add_command(push_to_s3)
    s3_manager.add_command(download_from_s3)

    s3_manager()

