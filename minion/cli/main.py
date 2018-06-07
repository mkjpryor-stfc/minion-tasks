"""
Command-line interface for Minion.
"""

import os
import logging

import click
from appdirs import user_config_dir, site_config_dir, user_data_dir

from .connector import connector, ConnectorLoader
from .template import template, TemplateLoader
from .job import job, JobLoader


@click.group()
@click.option(
    '--debug/--no-debug',
    default = False,
    help = "Enable debug logging."
)
@click.option(
    '--config-dir',
    envvar = 'MINION_CONFIG_DIRS',
    multiple = True,
    type = click.Path(exists = True, file_okay = False, resolve_path = True),
    help = 'Additional configuration directory (multiple permitted).'
)
@click.option(
    '--data-dir',
    envvar = 'MINION_DATA_DIRS',
    multiple = True,
    type = click.Path(
        exists = True,
        writable = True,
        file_okay = False,
        resolve_path = True
    ),
    help = 'Additional data directory (multiple permitted).'
)
@click.pass_context
def main(ctx, debug, config_dir, data_dir):
    """
    Minion workflow manager.
    """
    logging.basicConfig(
        format = "[%(levelname)s] [%(name)s] %(message)s",
        level = logging.DEBUG if debug else logging.INFO
    )
    ctx.obj = {}
    ctx.obj['config_dirs'] = config_dirs = [
        *config_dir,
        user_config_dir("minion"),
        site_config_dir("minion")
    ]
    ctx.obj['data_dirs'] = data_dirs = [
        *data_dir,
        user_data_dir("minion")
    ]
    # Construct all the required loaders
    ctx.obj['connectors'] = ConnectorLoader(*config_dirs)
    ctx.obj['templates'] = templates = TemplateLoader(
        *[os.path.join(d, "templates") for d in config_dirs]
    )
    ctx.obj['jobs'] = JobLoader(
        templates,
        *[os.path.join(d, "jobs") for d in data_dirs]
    )


@main.command(name = 'config-dirs')
@click.pass_context
def config_dirs(ctx):
    """
    Show the configuration directories in use.
    """
    click.echo(':'.join(ctx.obj['config_dirs']))


@main.command(name = 'data-dirs')
@click.pass_context
def data_dirs(ctx):
    """
    Show the data directories in use.
    """
    click.echo(':'.join(ctx.obj['data_dirs']))


main.add_command(connector)
main.add_command(template)
main.add_command(job)
