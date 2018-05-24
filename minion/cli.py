"""
Command-line interface for running Minion jobs.
"""

import os
import importlib
import pathlib
import logging
import collections

import click
import yaml
from appdirs import user_config_dir, site_config_dir
from tabulate import tabulate

from .core import Job, MinionFunction
from .connectors.base import Provider


class OrderedLoader(yaml.SafeLoader):
    def construct_ordered_mapping(self, node):
        self.flatten_mapping(node)
        # Force a depth-first construction
        return collections.OrderedDict(self.construct_pairs(node, deep = True))

OrderedLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    OrderedLoader.construct_ordered_mapping
)


class MinionYamlLoader(OrderedLoader):
    """
    YAML loader that can be used to load Minion jobs which interprets the
    ``!!minion/function`` tags to load Python code.
    """
    params = {}

    def __init__(self, *args, **kwargs):
        self.providers = []
        super().__init__(*args, **kwargs)

    def construct_minion_object(self, tag_name,
                                      check_type,
                                      type_human_name,
                                      tag_suffix,
                                      node):
        if isinstance(node, yaml.MappingNode):
            kwargs = self.construct_mapping(node, deep = True)
        elif isinstance(node, yaml.ScalarNode):
            # A scalar node just means no kwargs
            kwargs = {}
        else:
            raise yaml.constructor.ConstructorError(
                None,
                None,
                f'Invalid usage of {tag_name}',
                node.start_mark
            )
        # Re-raise any errors during import/exec as ConstructorErrors
        # That way, the user gets a place in the YAML file where things went wrong
        try:
            # Load the name as a dotted path
            module, name = tag_suffix.rsplit(".", maxsplit = 1)
            factory = getattr(importlib.import_module(module), name)
            # Verify the type is correct
            if not check_type(factory):
                raise yaml.constructor.ConstructorError(
                    None,
                    None,
                    "'{}.{}' is not a {}".format(
                        factory.__module__,
                        factory.__qualname__,
                        type_human_name
                    ),
                    node.start_mark
                )
            return factory(**kwargs)
        except (ImportError, TypeError, AttributeError) as exc:
            raise yaml.constructor.ConstructorError(
                None,
                None,
                str(exc),
                node.start_mark
            ) from exc

    def construct_minion_function(self, tag_suffix, node):
        return self.construct_minion_object(
            'minion/function',
            lambda x: isinstance(x, MinionFunction),
            'Minion function',
            tag_suffix,
            node
        )

    def construct_minion_provider(self, tag_suffix, node):
        provider = self.construct_minion_object(
            'minion/provider',
            lambda x: issubclass(x, Provider),
            'Minion provider',
            tag_suffix,
            node
        )
        # Store the provider for later
        self.providers.append(provider)
        return provider

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

    def construct_minion_get_provider(self, node):
        if not isinstance(node, yaml.ScalarNode):
            raise yaml.constructor.ConstructorError(
                None,
                None,
                'Invalid usage of minion/get_provider',
                node.start_mark
            )
        # The scalar value is the name of the provider
        provider_name = self.construct_scalar(node)
        # Try to find the named provider in the list of providers
        try:
            return next(p for p in self.providers if p.name == provider_name)
        except StopIteration:
            raise yaml.constructor.ConstructorError(
                None,
                None,
                f"Could not find provider '{provider_name}'",
                node.start_mark
            )

MinionYamlLoader.add_multi_constructor(
    'tag:yaml.org,2002:minion/function:',
    MinionYamlLoader.construct_minion_function
)
MinionYamlLoader.add_multi_constructor(
    'tag:yaml.org,2002:minion/provider:',
    MinionYamlLoader.construct_minion_provider
)
MinionYamlLoader.add_constructor(
    'tag:yaml.org,2002:minion/parameter',
    MinionYamlLoader.construct_minion_parameter
)
MinionYamlLoader.add_constructor(
    'tag:yaml.org,2002:minion/get_provider',
    MinionYamlLoader.construct_minion_get_provider
)


class MinionIgnoreYamlLoader(OrderedLoader):
    """
    YAML loader that allows a YAML file containing Minion tags to be loaded as
    if those tags were not present, but still safely.
    """
    @staticmethod
    def minion_object_constructor(tag_name):
        def constructor(loader, tag_suffix, node):
            if isinstance(node, yaml.MappingNode):
                return loader.construct_mapping(node, deep = True)
            elif isinstance(node, yaml.ScalarNode):
                return loader.construct_scalar(node)
            else:
                raise yaml.constructor.ConstructorError(
                    None,
                    None,
                    f'Invalid usage of {tag_name}',
                    node.start_mark
                )
        return constructor

    @staticmethod
    def minion_scalar_constructor(tag_name):
        def constructor(loader, node):
            if isinstance(node, yaml.ScalarNode):
                return loader.construct_scalar(node)
            else:
                raise yaml.constructor.ConstructorError(
                    None,
                    None,
                    f'Invalid usage of {tag_name}',
                    node.start_mark
                )
        return constructor

MinionIgnoreYamlLoader.add_multi_constructor(
    'tag:yaml.org,2002:minion/function:',
    MinionIgnoreYamlLoader.minion_object_constructor('minion/function')
)
MinionIgnoreYamlLoader.add_multi_constructor(
    'tag:yaml.org,2002:minion/provider:',
    MinionIgnoreYamlLoader.minion_object_constructor('minion/provider')
)
MinionIgnoreYamlLoader.add_constructor(
    'tag:yaml.org,2002:minion/parameter',
    MinionIgnoreYamlLoader.minion_scalar_constructor('minion/parameter')
)
MinionIgnoreYamlLoader.add_constructor(
    'tag:yaml.org,2002:minion/get_provider',
    MinionIgnoreYamlLoader.minion_scalar_constructor('minion/get_provider')
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
        # Generate a loader with the parameters bound
        loader = type('Loader', (MinionYamlLoader, ), dict(params = params))
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
@click.option('-d', '--job-dir', envvar = 'MINION_JOB_DIRS', multiple = True,
              type = click.Path(exists = True, file_okay = False, resolve_path = True),
              help = 'Additional directory to search for jobs (multiple permitted).')
@click.pass_context
def main(ctx, debug, job_dir):
    """
    Minion task importer.
    """
    logging.basicConfig(
        format = "[%(levelname)s] [%(name)s] %(message)s",
        level = logging.DEBUG if debug else logging.INFO
    )
    # Pass the minion loader as the click context object
    ctx.obj = Loader(
        *job_dir,
        os.path.join(user_config_dir("minion"), "jobs"),
        os.path.join(site_config_dir("minion"), "jobs")
    )


@main.command(name = 'job-sources')
@click.pass_context
def config_sources(ctx):
    """
    Print the directories that will be searched for jobs.
    """
    # Just print the directories we are using
    for directory in ctx.obj.directories:
        click.echo(directory)


@main.command(name = "list")
@click.option('-q', '--quiet', is_flag = True, default = False,
              help = 'Print the job name only, one per line.')
@click.pass_context
def list_jobs(ctx, quiet):
    """
    Lists the available jobs.
    """
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
@click.option("-f", "--params-file", envvar = "MINION_PARAMS_FILES",
              type = click.File(), multiple = True,
              help = "File containing parameter values (multiple allowed).")
@click.option("-p", "--params", type = str, help = "Parameter values as a YAML string.")
@click.argument("job_name")
@click.pass_context
def run_job(ctx, params_file, params, job_name):
    """
    Runs the specified job.
    """
    # Merge the parameter files in the order they were given
    # Values from later files take precedence
    merged = {}
    for f in params_file:
        merge(merged, yaml.safe_load(f))
    # Apply any overrides from the command line
    if params:
        merge(merged, yaml.safe_load(params))
    job = ctx.obj.find(job_name, merged)
    if job is None:
        click.echo("Could not find job '{}'.".format(job_name), err = True)
        raise SystemExit(1)
    job()
