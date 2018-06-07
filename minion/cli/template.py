"""
Command-line interface for running Minion jobs.
"""

import click
import yaml
from tabulate import tabulate

from ..core import Template
from .loader import HierarchicalDirectoryLoader


class TemplateLoader(HierarchicalDirectoryLoader):
    """
    Loader for templates that searches the given directories for YAML files.
    """
    not_found_message = "Could not find template '{}'"

    def _from_path(self, path):
        with path.open() as f:
            spec = yaml.safe_load(f)
        return Template(path.stem, spec.get('description', '-'), spec['spec'])


@click.group()
def template():
    """
    Commands for managing templates.
    """


@template.command(name = "list")
@click.pass_context
def list_templates(ctx):
    """
    List the available templates.
    """
    templates = list(ctx.obj['templates'].list())
    if templates:
        click.echo(tabulate(
            [(t.name, t.description) for t in templates],
            headers = ('Name', 'Description'),
            tablefmt = 'psql'
        ))
    else:
        click.echo("No templates available.")
