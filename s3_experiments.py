import os
from multiprocessing.dummy import Pool as ThreadPool
from time import time
from concurrent import futures
from boto3 import Session

from convert_file import ORIGINALS_PATH


aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
region_name = os.getenv('REGION_NAME')
profile_name = os.getenv('AWS_PROFILE_NAME')
sk_bucket_name = os.getenv('SK_BUCKET_NAME')

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
        print('Downloading {} to {}'.format(key, destination_file))
        s3_client.download_file(Bucket=sk_bucket_name, Key=key, Filename=destination_file)
    except Exception as e:
        print('Failed to download file: {}'.format(key))
        print(str(e))
    print('finilising processing {}:'.format(key))


if __name__ == '__main__':
    client = session.client('s3')

    export_date = '201805'
    prefix = 'search_keeper/configs/{}'.format(export_date)
    items = client.list_objects_v2(Bucket=sk_bucket_name, Prefix=prefix)

    splitted_key = prefix.split(os.path.sep)[1:]

    destination_folder = os.path.join(ORIGINALS_PATH, *splitted_key)

    if not os.path.isdir(destination_folder):
        print("Creating destination folder: {}".format(destination_folder))
        os.makedirs(destination_folder)

    inputs = [[content, destination_folder, client] for content in items['Contents'][1]]
    total_processes = 4
    t1 = time()
    pool = ThreadPool(total_processes)
    results = pool.starmap(download_file, inputs)
    pool.close()
    t2 = time()

    print("Items downladed: {}".format(len(items['Contents'])))
    print("Elapsed time: {:.3f} s".format((t2 - t1)))
