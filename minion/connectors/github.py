"""
Connectors for the Github API v3.
"""

import requests

from ..core import function as minion_function
from .rest import Connection, Resource, Manager, with_cache, RootResource, NestedResource


GITHUB_API = "https://api.github.com"


class Issue(Resource):
    endpoint = "issues"


class RepositoryManager(Manager):
    @with_cache("full_name")
    def fetch_one_by_full_name(self, full_name):
        return self.resource(self.connection.api_get(f"repos/{full_name}"))


class Repository(Resource):
    manager_class = RepositoryManager
    endpoint = "repositories"
    # Nested resources
    issues = NestedResource(Issue)


class Session(Connection):
    class Auth(requests.auth.AuthBase):
        """
        Requests authentication provider for Github API requests.
        """
        def __init__(self, api_token):
            self.api_token = api_token

        def __call__(self, req):
            req.headers.update({
                'Accept': 'application/vnd.github.v3+json',
                'Authorization': f"token {self.api_token}"
            })
            return req

    def __init__(self, name, api_token):
        super().__init__(name, GITHUB_API, self.Auth(api_token))

    issues = RootResource(Issue)
    repositories = RootResource(Repository)


@minion_function
def issues_assigned_to_user(session):
    """
    Returns a function that ignores its arguments and returns a list of issues
    assigned to the user.
    """
    return lambda *args: session.issues.fetch_all()


@minion_function
def issues_for_repository(session, repository_name):
    """
    Returns a function that ignores its arguments and returns a list of open
    issues for the given repository.
    """
    return lambda *args: (
        session.repositories
            .fetch_one_by_full_name(repository_name)
            .issues
            .fetch_all(state = 'all')
    )
