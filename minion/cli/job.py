"""
Command-line interface for managing and running Minion jobs.
"""

import yaml

from ..core import Job


class JobManager:
    """
    Minion job manager.
    """
    def __init__(self, templates, directory):
        self.templates = templates
        self.directory = directory.resolve()

    def from_path(self, path):
        with path.open() as f:
            spec = yaml.safe_load(f)
        return Job(
            path.stem,
            spec.get('description', '-'),
            self.templates.find(spec['template']),
            spec.get('values', {})
        )

    def all(self):
        """
        Returns an iterable of all the available jobs.
        """
        for path in sorted(self.directory.glob("*.yaml"), key = lambda p: p.stem):
            yield self.from_path(path)

    def find(self, name):
        """
        Finds and returns a job by name.
        """
        path = self.directory.joinpath(name).with_suffix('.yaml')
        if path.is_file():
            return self.from_path(path)
        raise LookupError("Job {} does not exist".format(repr(name)))

    def save(self, name, description, template, values):
        """
        Saves the given job in the directory with the highest precedence.
        """
        # Before attempting to write, ensure the directory exists
        self.directory.mkdir(parents = True, exist_ok = True)
        dest = self.directory / "{}.yaml".format(name)
        with dest.open('w') as f:
            yaml.dump(
                dict(
                    description = description or '',
                    template = template.name,
                    values = values
                ),
                f
            )

    def delete(self, name):
        """
        Removes jobs with the given name from any directory in which they exist.
        """
        path = self.directory / f"{name}.yaml"
        if path.exists():
            path.unlink()
