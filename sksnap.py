import click
from convert_file import process_cli
from s3_experiments import s3_cli


cli = click.CommandCollection(sources=[process_cli, s3_cli])
cli()
