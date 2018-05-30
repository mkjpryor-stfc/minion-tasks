"""
Connectors for the Trello API.
"""

import copy
import functools

import requests

from ..core import function as minion_function, Connector


class Session(Connector):
    class Auth(requests.auth.AuthBase):
        """
        Requests authentication provider for Trello API requests.
        """
        def __init__(self, api_key, api_token):
            self.api_key = api_key
            self.api_token = api_token

        def __call__(self, req):
            # Inject the additional URL parameters for authentication
            req.prepare_url(
                req.url,
                dict(key = self.api_key, token = self.api_token)
            )
            return req

    def __init__(self, name, api_key, api_token):
        super().__init__(name)
        self._session = requests.Session()
        self._session.auth = self.Auth(api_key, api_token)

    def url(self, url):
        """
        Prepend the API root to the given URL.
        """
        return f"https://api.trello.com/1{url}"

    def as_json(self, response):
        """
        Parse the response as JSON and return the result.
        """
        response.raise_for_status()
        return response.json()

    def boards(self):
        """
        Returns the boards available to the session.
        """
        return self.as_json(
            self._session.get(
                self.url("/members/me/boards"),
                params = { 'lists': 'open' }
            )
        )

    @functools.lru_cache()
    def find_board_by_id(self, id):
        """
        Finds a board by id.
        """
        try:
            return self.as_json(
                self._session.get(
                    self.url(f"/boards/{id}"),
                    params = { 'lists': 'open' }
                )
            )
        except requests.exceptions.HTTPError:
            raise RuntimeError(f"Could not find board with id '{id}'")

    @functools.lru_cache()
    def find_board_by_name(self, name):
        """
        Finds a board by name.
        """
        try:
            return next(b for b in self.boards() if b['name'] == name)
        except StopIteration:
            raise RuntimeError(f"Could not find board '{name}'")

    def cards_assigned_to_user(self):
        """
        Returns the cards assigned to the user.
        """
        return self.as_json(
            self._session.get(
                self.url("/members/me/cards"),
                params = { 'attachments': 'true', 'filter': 'visible' }
            )
        )

    def cards_for_board(self, board):
        """
        Returns the cards for the given board name.
        """
        board_id = board['id']
        return self.as_json(
            self._session.get(
                self.url(f"/boards/{board_id}/cards"),
                params = { 'attachments': 'true', 'filter': 'all' }
            )
        )

    def create_card(self, card):
        """
        Creates and returns a card as specified by the given dict.
        """
        return self.as_json(
            self._session.post(
                self.url("/cards"),
                params = card
            )
        )

    def update_card(self, card_id, updates):
        """
        Updates the card with the given id as specified by the given dict.
        """
        return self.as_json(
            self._session.put(
                self.url(f"/cards/{card_id}"),
                params = updates
            )
        )

    def delete_card(self, card_id):
        """
        Deletes the card with the given id.
        """
        return self.as_json(
            self._session.delete(self.url(f"/cards/{card_id}"))
        )

    def add_url_attachment(self, card, url):
        """
        Attaches the given url to the card and returns the updated card.
        """
        # If the attachment already exists, there is nothing to do
        if not any(a.get('url') == url for a in card.get('attachments', [])):
            card.setdefault('attachments', []).append(
                self.as_json(
                    self._session.post(
                        self.url("/cards/{}/attachments".format(card['id'])),
                        dict(url = url)
                    )
                )
            )
        return card

    def add_label(self, card, label):
        """
        Adds the given label to the card and returns the updated card.
        """
        # If a label with the same name already exists, there is nothing to do
        if not any(l['name'] == label['name'] for l in card.get('labels', [])):
            card.setdefault('labels', []).append(
                self.as_json(
                    self._session.post(
                        self.url("/cards/{}/labels".format(card['id'])),
                        label
                    )
                )
            )
        return card


@minion_function
def cards_assigned_to_user(session):
    """
    Returns a function that ignores its arguments and returns a list of cards
    assigned to the user.
    """
    return lambda *args: session.cards_assigned_to_user()[:1]


@minion_function
def cards_for_board(board_name, session):
    """
    Returns a function that ignores its arguments and returns a list of cards
    in the given board.
    """
    return lambda *args: session.cards_for_board(session.find_board_by_name(board_name))


@minion_function
def find_board_by_id(session):
    """
    Returns a function that accepts a board id as the incoming item and returns
    the Trello board with that id. If no board exists, an exception is raised.
    """
    return lambda item: session.find_board_by_id(item)


@minion_function
def create_card(board_name, list_name, session):
    """
    Returns a function that creates and returns a Trello card based on the
    incoming item.
    """
    def func(item):
        # We don't want to modify the incoming item directly, but a shallow
        # copy is fine
        item = copy.copy(item)
        # Attach the list id for the specified list
        board = session.find_board_by_name(board_name)
        try:
            item.update(
                idList = next(
                    l['id']
                    for l in board['lists'] if l['name'] == list_name
                )
            )
        except StopIteration:
            raise RuntimeError(
                f"Could not find list '{list_name}' in board '{board_name}'"
            )
        # Labels and attachments are processed after the card is created
        labels = item.pop('labels', [])
        attachments = item.pop('attachments', [])
        return functools.reduce(
            lambda card, label: session.add_label(card, label),
            labels,
            functools.reduce(
                lambda card, url: session.add_url_attachment(card, url),
                attachments,
                session.create_card(item)
            )
        )
    return func


@minion_function
def update_card(session):
    """
    Returns a function that updates and returns a Trello card based on the
    incoming item.

    The incoming item should be a ``{ card, updates }`` dict where updates is
    a dict of the updates to make.
    """
    def func(item):
        # Get the card and updates from the incoming item
        # Use shallow copies so we don't modify the incoming item
        card, updates = copy.copy(item['card']), copy.copy(item['updates'])
        # Labels and attachments are processed after the card is created
        labels = updates.pop('labels', [])
        attachments = updates.pop('attachments', [])
        # Throw out any keys in updates that match the corresponding key in the card
        updates = { k: v for k, v in updates.items() if v != card[k] }
        return functools.reduce(
            lambda card, label: session.add_label(card, label),
            labels,
            functools.reduce(
                lambda card, url: session.add_url_attachment(card, url),
                attachments,
                session.update_card(card['id'], updates) if updates else card
            )
        )
    return func


@minion_function
def delete_card(session):
    """
    Returns a function that deletes the Trello card corresponding to the
    incoming item. The card is returned.
    """
    return lambda item: session.delete_card(item['id'])
