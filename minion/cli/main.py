"""
Command-line interface for Minion.
"""

import sys
import re
import logging
import functools
import textwrap

import click
from tabulate import tabulate
import coolname
import yaml

from ..core import Parameter
from . import context


@click.group()
@click.option(
    '--debug/--no-debug',
    default = False,
    help = "Enable debug logging."
)
@click.option(
    '-c',
    '--config-dir',
    envvar = 'MINION_CONFIG_DIR',
    type = click.Path(exists = True, file_okay = False, resolve_path = True),
    help = 'Configuration directory to use.'
)
@click.pass_context
def main(ctx, debug, config_dir):
    """
    Minion workflow manager.
    """
    logging.basicConfig(
        format = "[%(levelname)s] [%(name)s] %(message)s",
        level = logging.DEBUG if debug else logging.INFO
    )
    ctx.obj = context.Context(config_dir or click.get_app_dir("minion"))


@main.group(name = "connector")
def connector_group():
    """
    Manage connectors.
    """


@connector_group.command(name = "ls")
@click.pass_obj
def connector_list(ctx):
    """
    List the available connectors.
    """
    connectors = ctx.connectors
    if connectors:
        click.echo(tabulate(
            [
                (
                    c.name,
                    '{}.{}'.format(type(c).__module__, type(c).__qualname__)
                )
                for c in sorted(connectors.values(), key = lambda c: c.name)
            ],
            headers = ('Name', 'Connector'),
            tablefmt = 'psql'
        ))
    else:
        click.echo('No connectors available.')


@main.group(name = "repo")
def repo_group():
    """
    Manage template repositories.
    """


@repo_group.command(name = "ls")
@click.pass_obj
def repo_list(ctx):
    """
    List the available template repositories.
    """
    repositories = list(ctx.repositories.all())
    if repositories:
        click.echo(tabulate(
            [
                (repo.name, repo.type, repo.location)
                for repo in sorted(repositories, key = lambda r: r.name)
            ],
            headers = ('Name', 'Type', 'Location'),
            tablefmt = 'psql'
        ))
    else:
        click.echo('No repositories available.')


class RegexStringParamType(click.types.StringParamType):
    """
    Click parameter type for a string matching a regex.
    """
    name = "text"

    def __init__(self, regex, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.regex = re.compile(regex)

    def convert(self, value, param, ctx):
        value = super().convert(value, param, ctx)
        if self.regex.search(value) is None:
            self.fail("{} does not match regex '{}'".format(
                repr(value),
                self.regex.pattern
            ))
        return value


@repo_group.command(name = "add")
@click.option(
    "--copy",
    is_flag = True,
    default = False,
    help = "Copy the contents of the specified directory to the repository "
           "location instead of making a symlink (only used when REPO_SOURCE "
           "is a local directory)."
)
@click.argument(
    'repo_name',
    type = RegexStringParamType(regex = "^[a-zA-Z0-9-_]+$")
)
@click.argument('repo_source', type = click.STRING)
@click.pass_obj
def repo_add(ctx, copy, repo_name, repo_source):
    """
    Add a template repository.

    REPO_NAME is the name of the repository and is used when referring to
    templates from the repository, e.g. REPO_NAME/my-template.

    Currently, REPO_SOURCE can be a local directory or a git repository.
    """
    ctx.repositories.add(repo_name, repo_source, copy)


@repo_group.command(name = "update")
@click.argument('repo_name', type = click.STRING)
@click.pass_obj
def repo_update(ctx, repo_name):
    """
    Update a repository.

    For git repositories, this will pull changes from the origin. For local
    repositories, it is a no-op.
    """
    ctx.repositories.update(repo_name)


@repo_group.command(name = "rm")
@click.option("-f", "--force", is_flag = True, default = False,
              help = "Suppress confirmation.")
@click.argument('repo_name', type = click.STRING)
@click.pass_obj
def repo_delete(ctx, force, repo_name):
    """
    Delete a repository.
    """
    if not force:
        click.confirm("Are you sure?", abort = True)
    ctx.repositories.delete(repo_name)


@main.group(name = "template")
def template_group():
    """
    Manage templates.
    """


@template_group.command(name = "ls")
@click.pass_obj
def template_list(ctx):
    """
    List the available templates.
    """
    # Get a list of (repo, template) pairs
    templates = list(ctx.templates.all())
    if templates:
        click.echo(tabulate(
            [(t.name, t.description) for t in templates],
            headers = ('Name', 'Description'),
            tablefmt = 'psql'
        ))
    else:
        click.echo("No templates available.")


@main.group(name = "job")
def job_group():
    """
    Manage jobs.
    """


@job_group.command(name = "ls")
@click.option(
    '-q',
    '--quiet',
    is_flag = True, default = False,
    help = "Print job names only, one per line."
)
@click.pass_obj
def job_list(ctx, quiet):
    """
    List the available jobs.
    """
    jobs = list(ctx.jobs.all())
    if quiet:
        for job in jobs:
            click.echo(job.name)
    elif jobs:
        click.echo(tabulate(
            [(j.name, j.description or '-', j.template.name) for j in jobs],
            headers = ('Name', 'Description', 'Template'),
            tablefmt = 'psql'
        ))
    else:
        click.echo("No jobs available.")


def _merge(destination, values):
    if not isinstance(values, dict):
        return
    for key, value in values.items():
        if isinstance(value, dict):
            _merge(destination.setdefault(key, {}), value)
        elif isinstance(value, list):
            destination.setdefault(key, []).extend(value)
        elif isinstance(value, set):
            destination.setdefault(key, set()).update(value)
        else:
            destination[key] = value


@job_group.command(name = "create")
@click.option(
    "-n",
    "--name",
    type = str,
    default = lambda: '_'.join(coolname.generate(2)),
    help = "A name for the job. If not given, a random name will be generated."
)
@click.option(
    "-f",
    "--values-file",
    type = click.File(),
    multiple = True,
    help = "YAML file containing parameter values (multiple allowed)."
)
@click.option(
    "--values",
    "values_str",
    type = str,
    help = "Parameter values as a YAML string."
)
@click.option(
    "--input/--no-input",
    "interactive",
    default = True,
    help = "Turn interactive configuration on/off."
)
@click.argument("template_name", required = False)
@click.pass_obj
def job_create(ctx, name, values_file, values_str, interactive, template_name):
    """
    Create a job.
    """
    # Only allow interactive creation if this is a TTY
    interactive = interactive and sys.stdin.isatty()
    if interactive:
        click.echo("*****************************")
        click.echo("** Interative mode enabled **")
        click.echo("*****************************")
        click.echo("")
    if template_name is None:
        if interactive:
            # Allow the user to pick a template from the list of available ones
            templates = list(ctx.templates.all())
            if not templates:
                click.secho("No templates available", fg = "red", bold = True)
                raise SystemExit(1)
            click.echo("Available templates:")
            for i, template in enumerate(templates, start = 1):
                click.echo("[{}] {}".format(i, template.name))
            template_index = click.prompt(
                "Select a template",
                type = click.IntRange(1, len(templates))
            )
            click.echo("")
            template = templates[template_index - 1]
        else:
            raise click.UsageError(
                "TEMPLATE_NAME is required when in non-interactive mode."
            )
    else:
        template = ctx.templates.find(template_name)
    # Merge the values files in the order they were given
    # Values from later files take precedence
    values = {}
    for f in values_file:
        _merge(values, yaml.safe_load(f))
    # Merge any overrides from the command line
    if values_str:
        _merge(values, yaml.safe_load(values_str))
    if interactive:
        # If running interactively, collect a description
        description = click.prompt('Brief description of job')
        click.echo("")
        # Collect parameter values
        for parameter in sorted(template.parameters, key = lambda p: p.name):
            click.echo("Parameter '{}'".format(parameter.name))
            if parameter.hint:
                click.echo(
                    "  Hint: " +
                    textwrap.indent(parameter.hint, "    ").strip()
                )
            if parameter.example:
                click.echo(
                    "  Example: " +
                    textwrap.indent(parameter.example, "    ").strip()
                )
            if parameter.default is not Parameter.NO_DEFAULT:
                click.echo(
                    "  Default: " +
                    textwrap.indent(str(parameter.default), "    ").strip()
                )
            _merge(
                values,
                # Create a dictionary with nesting based on "."s in param name
                functools.reduce(
                    lambda v, p: { p: v },
                    reversed(parameter.name.split(".")),
                    click.prompt(
                        "Enter value",
                        show_default = False,
                        default = parameter.default
                            if parameter.default is not Parameter.NO_DEFAULT
                            else None
                    )
                )
            )
            click.echo("")
    else:
        description = None
        # Just try to resolve each parameter to see if an error is raised
        for parameter in template.parameters:
            parameter.resolve(values)
    # Save the job
    ctx.jobs.save(name, description, template, values)
    click.secho(f"Created job '{name}'", fg = 'green')


@job_group.command(name = "run")
@click.argument('name')
@click.pass_obj
def job_run(ctx, name):
    """
    Run a job.
    """
    ctx.jobs.find(name).run(ctx.connectors)


@job_group.command(name = "rm")
@click.option(
    "-f",
    "--force",
    is_flag = True, default = False,
    help = "Suppress confirmation."
)
@click.argument('job_name')
@click.pass_obj
def job_delete(ctx, force, job_name):
    """
    Delete a job.
    """
    if not force:
        click.confirm("Are you sure?", abort = True)
    ctx.jobs.delete(job_name)
