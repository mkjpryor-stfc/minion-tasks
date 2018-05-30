"""
Command-line interface for running Minion jobs.
"""

import os
import pathlib
import logging
import textwrap

import click
import yaml
from appdirs import user_config_dir, site_config_dir, user_data_dir
from tabulate import tabulate
import coolname

from .core import Job, Template, Connector


class HierarchicalDirectoryLoader:
    """
    Base class for loaders that implements the directory-searching functionality.
    """
    not_found_message = "Could not find object '{}'"

    def __init__(self, *directories):
        self.directories = tuple(map(pathlib.Path, directories))

    def _from_path(self, path):
        raise NotImplementedError

    def list(self):
        """
        Returns an iterable of the available objects.
        """
        files = {}
        for directory in reversed(self.directories):
            for path in directory.glob("*.yaml"):
                files[path.stem] = path
        for name, path in sorted(files.items(), key = lambda x: x[0]):
            yield self._from_path(path)

    def find(self, name):
        """
        Returns the object with the specified name.
        """
        # Generate a loader with the parameters bound
        for directory in self.directories:
            path = directory / f"{name}.yaml"
            if not path.exists():
                continue
            return self._from_path(path)
        else:
            raise LookupError(self.not_found_message.format(name))


class TemplateLoader(HierarchicalDirectoryLoader):
    """
    Loader for templates that searches the given directories for YAML files.
    """
    not_found_message = "Could not find template '{}'"

    def _from_path(self, path):
        with path.open() as f:
            spec = yaml.safe_load(f)
        return Template(path.stem, spec.get('description', '-'), spec['spec'])


class JobLoader(HierarchicalDirectoryLoader):
    """
    Loader for jobs that searches the given directories for job definitions.
    """
    not_found_message = "Could not find job '{}'"

    def __init__(self, template_loader, *directories):
        self.template_loader = template_loader
        super().__init__(*directories)

    def _from_path(self, path):
        with path.open() as f:
            spec = yaml.safe_load(f)
        return Job(
            path.stem,
            spec.get('description', '-'),
            self.template_loader.find(spec['template']),
            spec.get('values', {})
        )

    def save(self, job):
        """
        Saves the given job in the directory with the highest precedence.
        """
        directory = self.directories[0]
        # Before attempting to write, ensure the directory exists
        directory.mkdir(parents = True, exist_ok = True)
        dest = directory / "{}.yaml".format(job.name)
        with dest.open('w') as f:
            yaml.dump(
                dict(
                    description = job.description or '',
                    template = job.template.name,
                    values = job.values
                ),
                f
            )

    def delete(self, name):
        """
        Removes jobs with the given name from any directory in which they exist.
        """
        for directory in self.directories:
            path = directory / f"{name}.yaml"
            if path.exists():
                path.unlink()


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
@click.option(
    '--debug/--no-debug',
    default = False,
    help = "Enable debug logging."
)
@click.option(
    '-c',
    '--config-dir',
    envvar = 'MINION_CONFIG_DIRS',
    multiple = True,
    type = click.Path(exists = True, file_okay = False, resolve_path = True),
    help = 'Additional configuration directory (multiple permitted).'
)
@click.option(
    '-d',
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


@main.group()
def connector():
    """
    Commands for managing connectors.
    """


@main.group()
def template():
    """
    Commands for managing templates.
    """


@main.group()
def job():
    """
    Commands for managing jobs.
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


@template.command(name = "show")
@click.argument('name')
@click.pass_context
def show_template(ctx, name):
    """
    Show details for the specified template.
    """
    try:
        template = ctx.obj['templates'].find(name)
    except LookupError as exc:
        click.secho(str(exc), err = True, fg = 'red', bold = True)
        raise SystemExit(1)
    click.echo("Name:")
    click.echo(textwrap.indent(template.name, '  '))
    click.echo("Description:")
    click.echo(
        textwrap.indent(
            os.linesep.join(textwrap.wrap(template.description)),
            '  '
        )
    )
    click.echo("Parameters:")
    for param in sorted(template.parameters, key = lambda p: p.name):
        click.echo(
            textwrap.indent(
                "{} ({})".format(
                    param.name,
                    "required" if param.default is Template.Parameter.NO_DEFAULT
                        else "default: {}".format(param.default)
                ),
                "  "
            )
        )


@job.command(name = "list")
@click.pass_context
def list_templates(ctx):
    """
    List the available jobs.
    """
    jobs = list(ctx.obj['jobs'].list())
    if jobs:
        click.echo(tabulate(
            [(j.name, j.description or '-', j.template.name) for j in jobs],
            headers = ('Name', 'Description', 'Template'),
            tablefmt = 'psql'
        ))
    else:
        click.echo("No jobs available.")


def _merge(destination, values):
    for key, value in values.items():
        if isinstance(value, dict):
            _merge(destination.setdefault(key, {}), value)
        elif isinstance(value, list):
            destination.setdefault(key, []).extend(value)
        elif isinstance(value, set):
            destination.setdefault(key, set()).update(value)
        else:
            destination[key] = value


@job.command(name = "create")
@click.option("-f", "--values-file", type = click.File(), multiple = True,
              help = "File containing parameter values (multiple allowed).")
@click.option("--values", "values_str", type = str,
              help = "Parameter values as a YAML string.")
@click.option("-n", "--name", type = str,
              default = lambda: '_'.join(coolname.generate(2)),
              help = "A name for the job. "
                     "If not given, a random name will be generated.")
@click.option("--desc", default = "", help = "A short description of the job.")
@click.argument("template_name")
@click.pass_context
def create_job(ctx, values_file, values_str, name, desc, template_name):
    """
    Create a job.
    """
    # Check if a job with the given name already exists
    try:
        _ = ctx.obj['jobs'].find(name)
    except LookupError:
        pass
    else:
        click.secho(
            f"Job '{name}' already exists",
            err = True, fg = "red", bold = True
        )
        raise SystemExit(1)
    # Find the template
    try:
        template = ctx.obj['templates'].find(template_name)
    except LookupError as exc:
        click.secho(str(exc), err = True, fg = "red", bold = True)
        raise SystemExit(1)
    # Merge the parameter files in the order they were given
    # Values from later files take precedence
    values = {}
    for f in values_file:
        _merge(values, yaml.safe_load(f))
    # Apply any overrides from the command line
    if values_str:
        _merge(values, yaml.safe_load(values_str))
    try:
        template.check_values(values)
    except ValueError as exc:
        click.secho(str(exc), err = True, fg = "red", bold = True)
        raise SystemExit(1)
    # Create the new job and save it
    ctx.obj['jobs'].save(Job(name, desc, template, values))
    click.echo(f"Created job '{name}'")


@job.command(name = "run")
@click.argument('name')
@click.pass_context
def run_job(ctx, name):
    """
    Run a job.
    """
    try:
        job = ctx.obj['jobs'].find(name)
    except LookupError as exc:
        click.secho(str(exc), err = True, fg = "red", bold = True)
        raise SystemExit(1)
    # Load the connectors
    connectors = ctx.obj['connectors'].find_all()
    # Run the job with the connectors
    job.run(connectors)


@job.command(name = "delete")
@click.option("-f", "--force", is_flag = True, default = False,
              help = "Suppress confirmation.")
@click.argument('name')
@click.pass_context
def delete_job(ctx, force, name):
    """
    Delete a job.
    """
    if force or click.confirm("Are you sure?"):
        ctx.obj['jobs'].delete(name)
