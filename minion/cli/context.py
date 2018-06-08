"""
Module containing Minion CLI context and helpers.
"""

import pathlib

import yaml

from ..core import Connector
from .repository import RepositoryManager
from .template import TemplateManager
from .job import JobManager


class Context:
    """
    Minion CLI context.
    """
    def __init__(self, config_dir):
        self.config_dir = pathlib.Path(config_dir).resolve()
        self.repositories = RepositoryManager(self.config_dir / "repos")
        self.templates = TemplateManager(self.config_dir / "repos")
        self.jobs = JobManager(self.templates, self.config_dir / "jobs")

    @property
    def connectors(self):
        """
        Returns a map of the available connectors by name.
        """
        path = self.config_dir / "connectors.yaml"
        if not path.exists():
            return {}
        with path.open() as f:
            connectors = yaml.safe_load(f)
        return {
            name: Connector.from_config(name, config)
            for name, config in connectors.items()
        }
