import click
from process_cli import process_cli
from s3_cli import s3_cli


cli = click.CommandCollection(sources=[process_cli, s3_cli])
cli()
