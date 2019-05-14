"""
Module containing classes and helpers for working with Minion templates.
"""

import pathlib

import yaml

from ..core import Parameter, Template


class TemplateManager:
    """
    Minion template manager.
    """
    def __init__(self, directory):
        self.directory = directory.resolve()

    def from_path(self, path):
        try:
            name = str(path.relative_to(self.directory).with_suffix(''))
        except ValueError:
            # If the relative path could not be resolved, use the full path
            name = str(path)
        with path.open() as f:
            template_spec = yaml.safe_load(f)
        return Template(
            name,
            template_spec.get('description', '-'),
            set(
                Parameter(
                    name,
                    param.get('hint'),
                    param.get('example'),
                    param.get('default', Parameter.NO_DEFAULT)
                )
                for name, param in template_spec.get('parameters', {}).items()
            ),
            template_spec['spec']
        )

    def all(self):
        """
        Returns an iterable of available templates.
        """
        if not self.directory.exists():
            return
        template_paths = self.directory.glob("*/*.yaml")
        for path in sorted(template_paths, key = lambda p: str(p)):
            yield self.from_path(path)

    def find(self, name):
        """
        Finds and returns a template by name.
        """
        # First, see if name is an actual file - if it is, use it
        path = pathlib.Path(name)
        if path.is_file():
            return self.from_path(path)
        # If it is not, try and find it in our directory
        path = self.directory / path.with_suffix('.yaml')
        if path.exists():
            return self.from_path(path)
        raise LookupError("Template {} does not exist".format(repr(name)))
