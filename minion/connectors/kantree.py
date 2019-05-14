"""
Connectors for the Kantree API.
"""

import base64
import functools

import requests

from ..core import function as minion_function, Connector


class Session(Connector):
    class Auth(requests.auth.AuthBase):
        """
        Requests authentication provider for Kantree API requests.
        """
        def __init__(self, api_token):
            # For some reason, the API token is given as base64-encoded
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

    def projects(self):
        """
        Returns a list of projects available to the user.
        """
        return self.as_json(self._session.get(self.url(f"/projects/all")))

    @functools.lru_cache()
    def find_project_by_id(self, id):
        """
        Finds a project by id.
        """
        return self.as_json(self._session.get(self.url(f"/projects/{id}")))

    @functools.lru_cache()
    def find_project_by_name(self, name):
        """
        Finds a project by name.
        """
        # Find the project from the list by name
        try:
            return next(p for p in self.projects() if p['title'] == name)
        except StopIteration:
            raise LookupError(f"Could not find project '{name}'")

    def cards_assigned_to_user(self):
        """
        Returns a list of cards assigned to the user.
        """
        return self.as_json(
            self._session.get(
                self.url("/search"),
                params = { 'filters': '@me' }
            )
        )

    def cards_for_project(self, id, with_archived = True):
        """
        Returns a list of cards for the given project id.
        """
        return self.as_json(
            self._session.get(
                self.url(f"/projects/{id}/cards"),
                params = dict(with_archived = with_archived)
            )
        )

    @functools.lru_cache()
    def find_or_create_group_type(self, project_id, name):
        """
        Find the group type in the given project, or create it if it doesn't
        exist.
        """
        project = self.find_project_by_id(project_id)
        try:
            return next(gt for gt in project['group_types'] if gt['name'] == name)
        except StopIteration:
            return self.as_json(
                self._session.post(
                    self.url("/group-types"),
                    params = dict(
                        name = name,
                        scope = "project",
                        project_id = project_id,
                        one_group_per_card = "false"
                    )
                )
            )

    @functools.lru_cache()
    def find_groups_for_card(self, card_id, group_type_id = None):
        """
        Find the groups for the given card, optionally restricted by group type.
        """
        params = dict(type_id = group_type_id) if group_type_id is not None else {}
        return self.as_json(
            self._session.get(
                self.url(f"/cards/{card_id}/groups"),
                params = params
            )
        )

    @functools.lru_cache()
    def find_or_create_group(self, card_id, group_type_id, name):
        """
        Find the specified group in the given card of the given type, or create
        it if it doesn't exist.
        """
        groups = self.find_groups_for_card(card_id, group_type_id)
        try:
            return next(g for g in groups if g['title'] == name)
        except StopIteration:
            return self.as_json(
                self._session.post(
                    self.url(f"/cards/{card_id}/groups"),
                    params = dict(type_id = group_type_id, title = name)
                )
            )

    @functools.lru_cache()
    def find_project_attribute(self, project_id, name):
        """
        Finds an attribute in a project by name.
        """
        attrs = self.as_json(
            self._session.get(self.url(f"/projects/{project_id}/attributes"))
        )
        return next((a for a in attrs if a['name'] == name), None)

    @functools.lru_cache()
    def find_card_model_attribute(self, model_id, name):
        """
        Finds an attribute in a model by name.
        """
        attrs = self.as_json(
            self._session.get(self.url(f"/models/{model_id}/attributes"))
        )
        return next((a for a in attrs if a['name'] == name), None)

    @functools.lru_cache()
    def find_card_model_by_name(self, project_id, name):
        """
        Finds a card model in a project by name.
        """
        models = self.as_json(
            self._session.get(
                self.url(f"/projects/{project_id}/models")
            )
        )
        try:
            return next(model for model in models if model['name'] == name)
        except StopIteration:
            raise LookupError(f"Could not find card model '{name}'")

    def create_card(self, project, card):
        """
        Creates a new card in the given project.
        """
        parent_id = project['top_level_card_id']
        return self.as_json(
            self._session.post(
                self.url(f"/cards/{parent_id}/children"),
                params = card
            )
        )

    def update_card(self, card_id, patch):
        """
        Update the given card with the given patch.
        """
        return self.as_json(
            self._session.put(
                self.url(f"/cards/{card_id}"),
                params = patch
            )
        )

    def add_card_to_group(self, card, group_type, group_name):
        """
        Adds the card to the given group.
        """
        project = self.find_project_by_id(card['project_id'])
        group_type = self.find_or_create_group_type(
            project['id'],
            group_type
        )
        group = self.find_or_create_group(
            project['top_level_card_id'],
            group_type['id'],
            group_name
        )
        return self.as_json(
            self._session.post(
                self.url("/cards/{}/add-to-group".format(card['id'])),
                params = dict(group_id = group['id'])
            )
        )

    def append_attribute_value(self, card, name, value):
        """
        Appends the given value to the specified attribute for the specified card.
        """
        attr = None
        if card['model_id'] is not None:
            attr = self.find_card_model_attribute(card['model_id'], name)
        if attr is None:
            attr = self.find_project_attribute(card['project_id'], name)
        if attr is None:
            raise LookupError(f"Could not find attribute '{name}'")
        return self.as_json(
            self._session.post(
                self.url("/cards/{}/attributes/{}/append".format(
                    card['id'],
                    attr['id']
                )),
                params = dict(value = value)
            )
        )


@minion_function
def cards_assigned_to_user(session):
    """
    Returns a function that ignores its arguments and returns a list of cards
    assigned to the user.
    """
    return lambda *args: session.cards_assigned_to_user()


@minion_function
def cards_for_project(session, project_name, with_archived = True):
    """
    Returns a function that ignores its arguments and returns a list of cards
    for the given project.
    """
    return lambda *args: session.cards_for_project(
        session.find_project_by_name(project_name)['id'],
        with_archived
    )


@minion_function
def create_card(session, project_name):
    """
    Returns a function that uses the incoming item to create a new card in the
    given project.
    """
    def func(item):
        project = session.find_project_by_name(project_name)
        # Pop off the groups and attributes as we will deal with them later
        groups = item.pop('groups', [])
        attributes = item.pop('attributes', [])
        # Pop the model name and convert it to a model id
        if 'model_name' in item:
            item['model_id'] = session.find_card_model_by_name(
                project['id'],
                item.pop('model_name')
            )['id']
        # Then create a new card in the project
        card = session.create_card(project, item)
        # Process the groups
        for group in groups:
            card.setdefault('groups', []).append(
                session.add_card_to_group(card, group['type'], group['name'])
            )
        # Process the attributes
        for attr in attributes:
            card.setdefault('attributes', []).append(
                session.append_attribute_value(
                    card,
                    attr['name'],
                    attr['value']
                )
            )
        return card
    return func


@minion_function
def update_card(session):
    """
    Returns a function that accepts a ``(card, updates)`` tuple as the incoming
    item and updates the card accordingly.
    """
    def func(item):
        card, updates = item
        # Pop off the groups and attributes as we will deal with them later
        groups = updates.pop('groups', [])
        attributes = updates.pop('attributes', [])
        # Pop the model name and convert it to a model id
        if 'model_name' in updates:
            updates['model_id'] = session.find_card_model_by_name(
                card['project_id'],
                updates.pop('model_name')
            )['id']
        # Extract the required patch
        patch = { k: v for k, v in updates.items() if card[k] != v }
        # Update the card if required
        if patch:
            card = session.update_card(card['id'], patch)
        # Process the groups
        for group in groups:
            card.setdefault('groups', []).append(
                session.add_card_to_group(card, group['type'], group['name'])
            )
        # Process the attributes
        for attr in attributes:
            card.setdefault('attributes', []).append(
                session.append_attribute_value(
                    card,
                    attr['name'],
                    attr['value']
                )
            )
        return card
    return func
