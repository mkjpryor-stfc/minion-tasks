"""
Functions for running Minion pipelines from the command line.
"""

import importlib
import pathlib

import click
import yaml
from appdirs import user_config_dir, site_config_dir
from tabulate import tabulate

from .core import Job, MinionFunction


def minion_function_constructor(loader, node):
    """
    YAML constructor for the "minion/function" tag.
    """
    if isinstance(node, yaml.MappingNode):
        mapping = loader.construct_mapping(node, deep = True)
        name = mapping['name']
        args = mapping.get('args', [])
        kwargs = mapping.get('kwargs', {})
    elif isinstance(node, yaml.ScalarNode):
        name = loader.construct_scalar(node)
        args = []
        kwargs = {}
    else:
        raise yaml.constructor.ConstructorError(
            None,
            None,
            'Invalid usage of minion/component',
            node.start_mark
        )
    # Re-raise any errors during import/exec as ConstructorErrors
    # That way, the user gets a place in the YAML file where things went wrong
    try:
        # Load the name as a dotted path
        module, name = name.rsplit(".", maxsplit=1)
        func = getattr(importlib.import_module(module), name)
        # The func must be a MinionFunction
        if not isinstance(func, MinionFunction):
            raise yaml.constructor.ConstructorError(
                None,
                None,
                "'{}.{}' is not a Minion function".format(
                    func.__module__,
                    func.__qualname__
                ),
                node.start_mark
            )
        return func(*args, **kwargs) if args or kwargs else func
    except (ImportError, TypeError, AttributeError) as exc:
        raise yaml.constructor.ConstructorError(
            None,
            None,
            str(exc),
            node.start_mark
        ) from exc

# Register the constructor with the safe loader
yaml.add_constructor(
    'tag:yaml.org,2002:minion/function',
    minion_function_constructor,
    yaml.SafeLoader
)


class Loader:
    """
    Class that is responsible for loading jobs.
    """
    def __init__(self, *directories):
        self.directories = tuple(map(pathlib.Path, directories))

    def list(self):
        """
        Returns an iterable of the available jobs.
        """
        # First, traverse the directories and work out what files to read
        # For any given stem, only read the file from the highest priority directory
        files = {}
        for directory in reversed(self.directories):
            for path in directory.glob("jobs/*.yaml"):
                files[path.stem] = path
        # Sort the jobs by stem
        # Ignore any files that yield TypeErrors or YAML parsing errors
        for (name, path) in sorted(files.items(), key = lambda x: x[0]):
            try:
                with path.open() as f:
                    yield Job(name, **yaml.safe_load(f))
            except (TypeError, yaml.YAMLError):
                pass

    def find(self, name):
        """
        Returns the job with the specified name or ``None``.
        """
        for directory in self.directories:
            path = directory / "jobs" / "{}.yaml".format(name)
            if path.exists():
                with path.open() as f:
                    return Job(name, **yaml.safe_load(f))
        return None


@click.group()
@click.option('-c', '--config-dir', envvar = 'MINION_CONFIG_DIR',
              type = click.Path(exists = True, file_okay = False, resolve_path = True),
              help = 'Additional configuration directory.')
@click.pass_context
def main(ctx, config_dir):
    """
    Minion task importer.
    """
    # Pass the minion loader as the click context object
    ctx.obj = Loader(
        *(config_dir, ) if config_dir else (),
        user_config_dir("minion"),
        site_config_dir("minion")
    )


@main.command(name = 'config-sources')
@click.pass_context
def config_sources(ctx):
    """
    Print the configuration sources being used in order of precedence.
    """
    # Just print the directories we are using
    for directory in ctx.obj.directories:
        click.echo(directory)


@main.command(name = "list")
@click.option('-q', '--quiet', is_flag = True, default = False,
              help = 'Print the job name only, one per line.')
@click.pass_context
def list_jobs(ctx, quiet):
    jobs = list(ctx.obj.list())
    if quiet:
        for job in jobs:
            click.echo(job.name)
    elif jobs:
        click.echo(tabulate(
            [(j.name, j.description) for j in jobs],
            headers = ('Name', 'Description'),
            tablefmt = 'psql'
        ))
    else:
        click.echo("No jobs available.")


@main.command(name = "run")
#@click.option("-f", "--params-file", multiple = True)
@click.argument("job_name")
@click.pass_context
def run_job(ctx, job_name):
    """
    Runs the specified job.
    """
    job = ctx.obj.find(job_name)
    if job is None:
        click.echo("Could not find job '{}'.".format(job_name), err = True)
        raise SystemExit(1)
    job()
