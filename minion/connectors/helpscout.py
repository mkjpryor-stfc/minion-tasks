"""
Connectors for the Help Scout API.
"""

import base64
import functools

import requests

from ..core import function as minion_function, Connector


class Session(Connector):
    class Auth(requests.auth.AuthBase):
        """
        Requests authentication provider for Help Scout API requests.
        """
        def __init__(self, api_token):
            # The API uses Basic auth with the API key as the username and a
            # dummy password
            # So we need to base64-encode
            self.api_token = base64.b64encode(f"{api_token}:X".encode()).decode()

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
        return f"https://api.helpscout.net/v1{url}"

    def as_json(self, response):
        """
        Parse the response as JSON and return the result.
        """
        response.raise_for_status()
        return response.json()

    def _iter_pages(self, url):
        page_num = 1
        while True:
            page = self.as_json(
                self._session.get(url, params = { 'page': page_num })
            )
            yield from page['items']
            if page_num == page['pages']:
                break
            page_num += 1

    @functools.lru_cache()
    def authenticated_user(self):
        """
        Returns the authenticated user.
        """
        return self.as_json(self._session.get(self.url('/users/me.json')))['item']

    def mailboxes(self):
        """
        Returns an iterable of all the available mailboxes.
        """
        return self._iter_pages(self.url('/mailboxes.json'))

    @functools.lru_cache()
    def find_mailbox_by_name(self, mailbox_name):
        """
        Find a mailbox by name.
        """
        try:
            return next(
                m
                for m in self.mailboxes()
                if m['name'] == mailbox_name
            )
        except StopIteration:
            raise LookupError(f"Could not find mailbox '{mailbox_name}'")

    def conversations_assigned_to_user(self, mailbox_id):
        """
        Returns a list of conversations assigned to the user in the given
        mailbox.
        """
        user_id = self.authenticated_user()['id']
        return self._iter_pages(
            self.url(
                f"/mailboxes/{mailbox_id}/users/{user_id}/conversations.json"
            )
        )


@minion_function
def conversations_assigned_to_user(session, mailbox_name):
    """
    Returns a function that ignores its incoming arguments and returns a list
    of conversations in the given mailbox assigned to the user.
    """
    return lambda *args: session.conversations_assigned_to_user(
        session.find_mailbox_by_name(mailbox_name)['id']
    )
