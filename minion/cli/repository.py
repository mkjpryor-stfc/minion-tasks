"""
Module containing classes and helpers for working with Minion repositories.
"""

import pathlib
import shutil


class RepositoryError(Exception):
    """
    Base class for repository exceptions.
    """


class InvalidRepositorySourceError(RepositoryError, ValueError):
    """
    Raised when an invalid repository source is specified.
    """
    def __init__(self, repo_source):
        super().__init__(f"'{repo_source}' is not a directory")


class RepositoryAlreadyExistsError(RepositoryError, ValueError):
    """
    Raised when a repository with the given name already exists.
    """
    def __init__(self, name):
        super().__init__(f"Repository '{name}' already exists")


class RepositoryDoesNotExistError(RepositoryError, LookupError):
    """
    Raised when a repository does not exist.
    """
    def __init__(self, name):
        super().__init__(f"Repository '{name}' does not exist")


class Repository:
    """
    DTO for a Minion repository.
    """
    def __init__(self, name, path):
        self.name = name
        self.path = path
        self.type = "local"


class RepositoryManager:
    """
    Minion repository manager.
    """
    def __init__(self, directory):
        self.directory = directory.resolve()

    def all(self):
        """
        Returns an iterable of available repositories.
        """
        # If the repos directory has not been created, there are no repos
        if not self.directory.exists():
            return
        # Each child in the directory should be a repo
        for child in sorted(self.directory.iterdir(), key = lambda c: c.stem):
            # This will fail if a symlink is broken
            if child.is_dir():
                yield Repository(child.stem, child.resolve())

    def add(self, repo_name, repo_source, copy = False):
        """
        Create and return a new repository.
        """
        # Check if a repo with the name already exists
        repo_path = self.directory.joinpath(repo_name)
        if repo_path.exists():
            raise RepositoryAlreadyExistsError(repo_name)
        # If repo_source is a local directory, create a symlink or copy
        # depending on the value of the flag given
        source_path = pathlib.Path(repo_source)
        if source_path.is_dir():
            # Ensure that the repo directory exists
            self.directory.mkdir(parents = True, exist_ok = True)
            if copy:
                # Recursively copy the source to the destination
                shutil.copytree(source_path, repo_path)
            else:
                # Create a symlink at repo_path to the directory
                repo_path.symlink_to(source_path.resolve(), True)
            return
        raise InvalidRepositorySourceError(repo_source)

    def find(self, repo_name):
        """
        Finds and returns the specified repository.
        """
        repo_path = self.directory.joinpath(repo_name)
        if not repo_path.is_dir():
            raise RepositoryDoesNotExistError(repo_name)
        return Repository(repo_name, repo_path.resolve())

    def delete(self, repo_name):
        """
        Deletes the repository with the specified name.
        """
        repo_path = self.directory.joinpath(repo_name)
        if repo_path.is_symlink():
            repo_path.unlink()
        elif repo_path.is_dir():
            shutil.rmtree(repo_path)
