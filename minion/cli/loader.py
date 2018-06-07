"""
Command-line interface for running Minion jobs.
"""

import pathlib


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
