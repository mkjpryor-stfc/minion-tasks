"""
Command-line interface for running Minion jobs.
"""

import importlib
import pathlib
import logging

import click
import yaml
from appdirs import user_config_dir, site_config_dir
from tabulate import tabulate

from .core import Job, MinionFunction


class MinionYamlLoader(yaml.SafeLoader):
    """
    YAML loader that can be used to load Minion jobs which interprets the
    ``!!minion/function`` tags to load Python code.
    """
    params = {}

    def construct_minion_function(self, tag_suffix, node):
        if isinstance(node, yaml.MappingNode):
            kwargs = self.construct_mapping(node, deep = True)
        elif isinstance(node, yaml.ScalarNode):
            # A scalar node just means no kwargs
            kwargs = {}
        else:
            raise yaml.constructor.ConstructorError(
                None,
                None,
                'Invalid usage of minion/function',
                node.start_mark
            )
        # Re-raise any errors during import/exec as ConstructorErrors
        # That way, the user gets a place in the YAML file where things went wrong
        try:
            # Load the name as a dotted path
            module, name = tag_suffix.rsplit(".", maxsplit=1)
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
            return func(**kwargs)
        except (ImportError, TypeError, AttributeError) as exc:
            raise yaml.constructor.ConstructorError(
                None,
                None,
                str(exc),
                node.start_mark
            ) from exc

    def construct_minion_parameter(self, node):
        if not isinstance(node, yaml.ScalarNode):
            raise yaml.constructor.ConstructorError(
                None,
                None,
                'Invalid usage of minion/parameter',
                node.start_mark
            )
        # The scalar value is the parameter name
        param_name = self.construct_scalar(node)
        # It might be a dotted name that we need to traverse
        parts = param_name.split(".")
        value = self.params
        while parts:
            try:
                value = value[parts.pop(0)]
            except KeyError:
                raise yaml.constructor.ConstructorError(
                    None,
                    None,
                    f"Could not find parameter '{param_name}'",
                    node.start_mark
                )
        return value

    @classmethod
    def with_params(cls, params):
        """
        Returns a new subclass of this loader with the given parameters.
        """
        loader = type('LocalLoader', (cls, ), dict(params = params))
        loader.add_multi_constructor(
            'tag:yaml.org,2002:minion/function:',
            loader.construct_minion_function
        )
        loader.add_constructor(
            'tag:yaml.org,2002:minion/parameter',
            loader.construct_minion_parameter
        )
        return loader


class MinionIgnoreYamlLoader(yaml.SafeLoader):
    """
    YAML loader that allows a YAML file containing Minion tags to be loaded as
    if those tags were not present, but still safely.
    """
    def construct_minion_function(self, tag_suffix, node):
        if isinstance(node, yaml.MappingNode):
            return self.construct_mapping(node, deep = True)
        elif isinstance(node, yaml.ScalarNode):
            return self.construct_scalar(node)
        else:
            raise yaml.constructor.ConstructorError(
                None,
                None,
                'Invalid usage of minion/function',
                node.start_mark
            )

    def construct_minion_parameter(self, node):
        if isinstance(node, yaml.ScalarNode):
            return self.construct_scalar(node)
        else:
            raise yaml.constructor.ConstructorError(
                None,
                None,
                'Invalid usage of minion/parameter',
                node.start_mark
            )

MinionIgnoreYamlLoader.add_multi_constructor(
    'tag:yaml.org,2002:minion/function:',
    MinionIgnoreYamlLoader.construct_minion_function
)
MinionIgnoreYamlLoader.add_constructor(
    'tag:yaml.org,2002:minion/parameter',
    MinionIgnoreYamlLoader.construct_minion_parameter
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
        files = {}
        for directory in reversed(self.directories):
            for path in directory.glob("*.yaml"):
                files[path.stem] = path
        for name, path in sorted(files.items(), key = lambda x: x[0]):
            # We only want the description, so open with a loader that ignores
            # Minion syntax rather than bailing
            with path.open() as f:
                job_spec = yaml.load(f, Loader = MinionIgnoreYamlLoader)
            yield Job(name, job_spec['description'], lambda: None)

    def find(self, name, params):
        """
        Returns the job with the specified name or ``None``.
        """
        loader = MinionYamlLoader.with_params(params)
        for directory in self.directories:
            path = directory / f"{name}.yaml"
            if not path.exists():
                continue
            # Use a loader that understands the Minion tags
            with path.open() as f:
                job_spec = yaml.load(f, Loader = loader)
            return Job(name, job_spec['description'], job_spec['spec'])
        return None


@click.group()
@click.option('--debug/--no-debug', default = False, help = "Enable debug logging.")
@click.option('-d', '--jobs-dir', envvar = 'MINION_JOBS_DIR',
              type = click.Path(exists = True, file_okay = False, resolve_path = True),
              help = 'Additional directory to search for jobs.')
@click.pass_context
def main(ctx, debug, jobs_dir):
    """
    Minion task importer.
    """
    logging.basicConfig(
        format = "[%(levelname)s] [%(name)s] %(message)s",
        level = logging.DEBUG if debug else logging.INFO
    )
    # Pass the minion loader as the click context object
    ctx.obj = Loader(
        *(jobs_dir, ) if jobs_dir else (),
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


def merge(destination, values):
    for key, value in values.items():
        if isinstance(value, dict):
            merge(destination.setdefault(key, {}), value)
        elif isinstance(value, list):
            destination.setdefault(key, []).extend(value)
        elif isinstance(value, set):
            destination.setdefault(key, set()).update(value)
        else:
            destination[key] = value


@main.command(name = "run")
@click.option("-f", "--params-file", type = click.File(), multiple = True,
              help = "Files containing parameter values for the job.")
@click.argument("job_name")
@click.pass_context
def run_job(ctx, params_file, job_name):
    """
    Runs the specified job.
    """
    # Merge the parameter files in the order they were given
    # Values from later files take precedence
    params = {}
    for f in params_file:
        merge(params, yaml.safe_load(f))
    job = ctx.obj.find(job_name, params)
    if job is None:
        click.echo("Could not find job '{}'.".format(job_name), err = True)
        raise SystemExit(1)
    job()
