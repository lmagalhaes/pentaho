import click
from convert_file import process_cli


cli = click.CommandCollection(sources=[process_cli])
cli()
