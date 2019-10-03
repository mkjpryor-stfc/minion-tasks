"""
Connectors for the Github API v3.
"""

import requests

from ..core import function as minion_function
from .rest import Connection, Resource, Manager


GITHUB_API = "https://api.github.com"


class IssueManager(Manager):
    def fetch_all_for_repo(self, repo, **params):
        endpoint = f"repos/{repo}/{self.resource_class.endpoint}"
        data = self.connection.api_get(endpoint, params = params)
        return tuple(self.resource(d) for d in data)


class Issue(Resource):
    manager_class = IssueManager
    endpoint = "issues"


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

    issues = Issue.manager()


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
    return lambda *args: session.issues.fetch_all_for_repo(repository_name, state = 'all')
