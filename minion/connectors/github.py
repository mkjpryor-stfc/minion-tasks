"""
Connectors for the Github API v3.
"""

import requests

from .base import Provider
from ..core import function as minion_function


class Session(Provider):
    class Auth(requests.auth.AuthBase):
        """
        Requests authentication provider for Github API requests.
        """
        def __init__(self, api_token):
            self.api_token = api_token

        def __call__(self, req):
            req.headers.update({
                'Authorization': 'token {}'.format(self.api_token)
            })
            return req

    def __init__(self, name, api_token):
        super().__init__(name)
        self._session = requests.Session()
        self._session.auth = self.Auth(api_token)

    def url(self, url):
        """
        Prepend the API root to the given URL.
        """
        return f"https://api.github.com{url}"

    def as_json(self, response):
        """
        Parse the response as JSON and return the result.
        """
        response.raise_for_status()
        return response.json()

    def issues_assigned_to_user(self):
        """
        Returns the open issues and pull requests assigned to the user.
        """
        return self.as_json(self._session.get(self.url("/issues")))

    def issues_for_repository(self, repository_name):
        """
        Returns the open issues and pull requests for the given repository.
        """
        return self.as_json(
            self._session.get(self.url(f"/repos/{repository_name}/issues"))
        )


@minion_function
def issues_assigned_to_user(session):
    """
    Returns a function that ignores its arguments and returns a list of issues
    assigned to the user.
    """
    return lambda *args: session.issues_assigned_to_user()


@minion_function
def issues_for_repository(session, repository_name):
    """
    Returns a function that ignores its arguments and returns a list of open
    issues for the given repository.
    """
    return lambda *args: session.issues_for_repository(repository_name)[:2]
