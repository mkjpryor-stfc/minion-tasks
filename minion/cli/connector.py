"""
Command-line interface for managing Minion connectors.
"""

import pathlib

import click
import yaml
from tabulate import tabulate

from ..core import Connector


class ConnectorLoader:
    """
    Loader for connectors that looks for ``connectors.yaml`` in each of the
    given directories and merges the results.

    Each ``connectors.yaml`` can provide many connectors. If a connector with
    the same name is given in multiple files, the one from the highest
    precedence directory is used.
    """
    def __init__(self, *directories):
        self.directories = tuple(map(pathlib.Path, directories))

    def find_all(self):
        """
        Returns a dictionary of the available connectors indexed by name.
        """
        connectors = {}
        # Start with the lowest-precedence directory and override as we go
        for directory in reversed(self.directories):
            path = directory / "connectors.yaml"
            if not path.exists():
                continue
            with path.open() as f:
                configs = yaml.safe_load(f)
            for name, config in configs.items():
                connectors[name] = Connector.from_config(name, config)
        return connectors


@click.group()
def connector():
    """
    Commands for managing connectors.
    """


@connector.command(name = "list")
@click.pass_context
def list_connectors(ctx):
    """
    List the available connectors.
    """
    connectors = ctx.obj['connectors'].find_all()
    if connectors:
        click.echo(tabulate(
            [
                (
                    c.name,
                    '{}.{}'.format(type(c).__module__, type(c).__qualname__)
                )
                # Sort the connectors by name
                for c in sorted(connectors.values(), key = lambda c: c.name)
            ],
            headers = ('Name', 'Connector'),
            tablefmt = 'psql'
        ))
    else:
        click.echo('No connectors available.')
