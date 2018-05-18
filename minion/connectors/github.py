"""
Connectors for the Github API v3.
"""

import functools

import requests

from ..core import function as minion_function


class Session:
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

    def __init__(self, api_token):
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

    @functools.lru_cache()
    def issues_assigned_to_user(self):
        return self.as_json(self._session.get(self.url("/issues")))

    @classmethod
    @functools.lru_cache()
    def get(cls, api_token):
        """
        Returns a session for the given API token.

        The sessions are cached based on the token.
        """
        return cls(api_token)


@minion_function
def issues_assigned_to_user(api_token):
    return lambda: Session.get(api_token).issues_assigned_to_user()
