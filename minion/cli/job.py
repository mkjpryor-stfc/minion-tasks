"""
Command-line interface for managing and running Minion jobs.
"""

import sys
import functools

import click
import yaml
from tabulate import tabulate
import coolname

from ..core import Job, Parameter
from .loader import HierarchicalDirectoryLoader


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


@click.group()
def job():
    """
    Commands for managing jobs.
    """


@job.command(name = "list")
@click.option('-q', '--quiet', is_flag = True, default = False,
            help = "Print job names only, one per line.")
@click.pass_context
def list_jobs(ctx, quiet):
    """
    List the available jobs.
    """
    jobs = list(ctx.obj['jobs'].list())
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
    "--description",
    default = "",
    help = "A brief description of the job."
)
@click.option(
    "--no-input",
    is_flag = True,
    default = False,
    help = "Disable interactive collection of missing values."
)
@click.argument("template_name")
@click.pass_context
def create_job(ctx, name,
                    values_file,
                    values_str,
                    description,
                    no_input,
                    template_name):
    """
    Create a job.
    """
    # Force no_input if not in a TTY
    no_input = no_input or not sys.stdin.isatty()
    # Find the template
    try:
        template = ctx.obj['templates'].find(template_name)
    except LookupError as exc:
        click.secho(str(exc), err = True, fg = "red", bold = True)
        raise SystemExit(1)
    # Merge the values files in the order they were given
    # Values from later files take precedence
    values = {}
    for f in values_file:
        _merge(values, yaml.safe_load(f))
    # Merge any overrides from the command line
    if values_str:
        _merge(values, yaml.safe_load(values_str))
    if no_input:
        # If we are not to collect any interactive input, check the values
        # and bail if they are not good
        try:
            template.check_values(values)
        except Parameter.Missing as exc:
            click.secho(str(exc), err = True, fg = "red", bold = True)
            raise SystemExit(1)
    else:
        click.echo(template.description + "\n")
        # If no description is given, ask for one.
        description = click.prompt('Brief description of job')
        # If we are collecting interactive input, collect missing values until
        # we have a complete set of parameters
        while True:
            try:
                template.check_values(values)
            except Parameter.Missing as exc:
                # Prompt for the missing value and merge it in
                _merge(
                    values,
                    functools.reduce(
                        lambda v, p: {p: v},
                        reversed(exc.parameter_name.split(".")),
                        yaml.safe_load(click.prompt(exc.parameter_name))
                    )
                )
            else:
                break
    # Create the new job and save it
    ctx.obj['jobs'].save(Job(name, description, template, values))
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
    if not force:
        click.confirm("Are you sure?", abort = True)
    ctx.obj['jobs'].delete(name)
