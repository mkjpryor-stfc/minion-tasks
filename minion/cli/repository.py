"""
Module containing classes and helpers for working with Minion repositories.
"""

import os
import pathlib
import shutil
from collections import namedtuple

from dulwich import porcelain


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


class Repository(namedtuple('Repository', ['name', 'type', 'location'])):
    """
    DTO for a Minion repository.
    """


class RepositoryManager:
    """
    Minion repository manager.
    """
    def __init__(self, directory):
        self.directory = directory.resolve()

    def _from_path(self, path):
        # We already know that the path is a (symlink to a) directory
        # If it is a symlink, it is a local repository, even if the other end
        # is a git repo
        if path.is_symlink():
            return Repository(path.stem, 'local', path.resolve())
        # Now we know we have a path to a directory that is not a symlink
        # To have type 'git', it must be a git repo with a remote called 'origin'
        # The remote URL is what we return as path
        try:
            with porcelain.open_repo_closing(path) as r:
                # Let the KeyError get caught be the outer try
                origin = r.get_config().get((b"remote", b"origin"), b"url")
            return Repository(path.stem, 'git', origin.decode())
        except Exception:
            # Ignore errors
            pass
        # If it is not a git repo with an origin, treat it as a regular directory
        return Repository(path.stem, 'local', path.resolve())

    def all(self):
        """
        Returns an iterable of available repositories.
        """
        # If the repos directory has not been created, there are no repos
        if not self.directory.exists():
            return
        # Each child in the directory should be a repo
        for child in sorted(self.directory.iterdir(), key = lambda c: c.stem):
            # This will fail if a symlink is broken
            if child.is_dir():
                yield self._from_path(child)

    def add(self, repo_name, repo_source, copy = False):
        """
        Create and return a new repository.
        """
        # Check if a repo with the name already exists
        repo_path = self.directory.joinpath(repo_name)
        # exists will fail if repo_path is a broken symlink, so test for it
        if repo_path.is_symlink() or repo_path.exists():
            raise RepositoryAlreadyExistsError(repo_name)
        # If repo_source is a local directory, create a symlink or copy
        # depending on the value of the flag given
        source_path = pathlib.Path(repo_source)
        if source_path.is_dir():
            # Ensure that the repo directory exists
            self.directory.mkdir(parents = True, exist_ok = True)
            if copy:
                # Recursively copy the source to the destination
                shutil.copytree(source_path, repo_path)
            else:
                # Create a symlink at repo_path to the directory
                repo_path.symlink_to(source_path.resolve(), True)
            return
        # Otherwise, try and treat repo_source as a git repository to clone
        try:
            with open(os.devnull, 'wb') as f:
                porcelain.clone(repo_source, str(repo_path), errstream = f)
        except Exception:
            # If an exception occurs, clean up anything at repo_path
            if repo_path.exists():
                shutil.rmtree(repo_path)
            raise

    def find(self, repo_name):
        """
        Finds and returns the specified repository.
        """
        repo_path = self.directory.joinpath(repo_name)
        if not repo_path.is_dir():
            raise RepositoryDoesNotExistError(repo_name)
        return self._from_path(repo_path)

    def update(self, repo_name):
        """
        Updates the specified repository. If the repository is a git repository,
        this will pull the latest changes from origin. If the repository is a
        local directory, it is a no-op.
        """
        repo = self.find(repo_name)
        if repo.type is 'git':
            repo_path = self.directory.joinpath(repo.name)
            with open(os.devnull, 'wb') as f:
                porcelain.pull(
                    str(repo_path),
                    remote_location = repo.location,
                    outstream = f,
                    errstream = f
                )

    def delete(self, repo_name):
        """
        Deletes the repository with the specified name.
        """
        repo_path = self.directory.joinpath(repo_name)
        if repo_path.is_symlink():
            repo_path.unlink()
        elif repo_path.is_dir():
            shutil.rmtree(repo_path)
