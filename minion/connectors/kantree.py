"""
Connectors for the Kantree API.
"""

import base64

import requests

from .base import Provider
from ..core import function as minion_function


class Session(Provider):
    class Auth(requests.auth.AuthBase):
        """
        Requests authentication provider for Kantree API requests.
        """
        def __init__(self, api_token):
            # For some reason, the API token is given as base64-encoded
            self.api_token = base64.b64encode(api_token.encode()).decode()

        def __call__(self, req):
            req.headers.update({
                'Authorization': 'Basic {}'.format(self.api_token)
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
        return f"https://kantree.io/api/1.0{url}"

    def as_json(self, response):
        """
        Parse the response as JSON and return the result.
        """
        response.raise_for_status()
        return response.json()

    def cards_assigned_to_user(self):
        return self.as_json(
            self._session.get(
                self.url("/search"),
                params = { 'filters': '@me' }
            )
        )


@minion_function
def cards_assigned_to_user(session):
    """
    Returns a function that ignores its arguments and returns a list of cards
    assigned to the user.
    """
    return lambda *args: session.cards_assigned_to_user()
