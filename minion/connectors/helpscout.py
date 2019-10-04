"""
Connectors for the Help Scout API.
"""

import base64
import functools
import time
from datetime import datetime, timedelta

import requests

from ..core import function as minion_function
from .rest import Connection, Resource, Manager


HELPSCOUT_API = "https://api.helpscout.net/v2"


class HelpscoutManager(Manager):
    def extract_data(self, response):
        # Helpscount uses HAL formatted responses
        return response.json()['_embedded'][self.resource_class.endpoint]

    def next_page(self, response):
        # By default, use the links from the Link header
        return response.json().get('_links', {}).get('next', {}).get('href')


class UserManager(HelpscoutManager):
    def me(self):
        return self.resource(self.connection.api_get("users/me"))


class User(Resource):
    manager_class = UserManager
    endpoint = "users"


class Mailbox(Resource):
    manager_class = HelpscoutManager
    endpoint = "mailboxes"


class Conversation(Resource):
    manager_class = HelpscoutManager
    endpoint = "conversations"


class Session(Connection):
    class Auth(requests.auth.AuthBase):
        """
        Requests authentication provider for Github API requests.
        """
        def __init__(self, client_id, client_secret):
            self.client_id = client_id
            self.client_secret = client_secret
            self.token = None
            self.token_expires_at = None

        def __call__(self, req):
            # Fetch a token with client credentials if required
            if not self.token or time.time() >= self.token_expires_at:
                response = requests.post(
                    HELPSCOUT_API + "/oauth2/token",
                    params = dict(
                        grant_type = "client_credentials",
                        client_id = self.client_id,
                        client_secret = self.client_secret
                    )
                )
                response.raise_for_status()
                token_json = response.json()
                self.token = token_json['access_token']
                self.token_expires_at = time.time() + token_json['expires_in']
            # Include the token in the auth header
            req.headers.update({ 'Authorization': f"Bearer {self.token}" })
            return req

    def __init__(self, name, client_id, client_secret):
        super().__init__(name, HELPSCOUT_API, self.Auth(client_id, client_secret))

    conversations = Conversation.manager()
    mailboxes = Mailbox.manager()
    users = User.manager()


@minion_function
def conversations_assigned_to_user(session, mailbox_name):
    """
    Returns a function that ignores its incoming arguments and returns a list
    of recently modified conversations in the given mailbox assigned to the user.
    """
    def func(*args):
        mailbox = session.mailboxes.fetch_one_by_name(mailbox_name)
        user = session.users.me()
        # Just fetch conversations modified in the last 4 weeks
        threshold = datetime.utcnow() - timedelta(days = 28)
        return session.conversations.fetch_all(
            mailbox = mailbox.id,
            assigned_to = user.id,
            status = 'active,closed',
            modifiedSince = threshold.isoformat(timespec = 'seconds') + 'Z'
        )
    return func
