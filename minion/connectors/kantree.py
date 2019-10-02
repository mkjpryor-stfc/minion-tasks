"""
Connectors for the Kantree API.
"""

import base64
import functools

import requests

from ..core import function as minion_function
from .rest import Connection, Resource, Manager


KANTREE_API = "https://kantree.io/api/1.0"


class Group(Resource):
    endpoint = "groups"


class GroupType(Resource):
    endpoint = "group-types"
    # Nested resources
    groups = Group.manager()


class Attribute(Resource):
    endpoint = "attributes"


class Model(Resource):
    endpoint = "models"


class CardManager(Manager):
    def search(self, filters):
        """
        Search cards using the given filters.
        """
        data = self.connection.api_get("search", params = dict(filters = filters))
        return tuple(self.resource(d) for d in data)

    def create(self, project, **params):
        data = self.connection.api_post(
            f"cards/{project.top_level_card_id}/children",
            params = params
        )
        return self.resource(data)

    def set_card_model(self, card, model = None):
        """
        Sets the model of given card to the given model.
        """
        model_id = getattr(model, 'id', model)
        params = dict(model_id = model_id) if model_id else dict()
        return self.action(card, "model", params)

    def set_card_state(self, card, state = 'undecided'):
        """
        Sets the state of the card.
        """
        return self.action(card, "state", dict(state = state))

    def add_card_to_group(self, card, group):
        group_id = getattr(group, 'id', group)
        return self.action(card, "add-to-group", dict(group_id = group_id))

    def remove_card_from_group(self, card, group):
        group_id = getattr(group, 'id', group)
        return self.action(card, "remove-from-group", dict(group_id = group_id))

    def attach_link_to_card(self, card, url):
        """
        Ensures that the given URL is present as an attachment on the given card.
        """
        attr = card.project.attributes.fetch_one_by_name("Attachments")
        # First, check if the link has already been attached
        for card_attr in card.attributes:
            # This isn't the correct attribute
            if card_attr['id'] != attr.id:
                continue
            # If there is a URL that matches, we are done
            if any(v['url'] == url for v in card_attr['value']):
                break
        else:
            # Create the attribute via the card
            data = (
                card._attributes
                    .fetch_one(attr.id, lazy = True)
                    .action("append", dict(value = url))
                    .data
            )
            card.attributes.append(data)
        return card


class Card(Resource):
    manager_class = CardManager
    endpoint = "cards"
    # Nested resources
    # cards contain attributes already, so we need to use a different name
    _attributes = Attribute.manager()
    groups = Group.manager()

    @property
    def project(self):
        return self.manager.connection.projects.fetch_one(self.project_id, lazy = True)

    def set_model(self, model = None):
        return self.manager.set_card_model(self, model)

    def set_state(self, state = 'undecided'):
        return self.manager.set_card_state(self, state)

    def add_to_group(self, group):
        return self.manager.add_card_to_group(self, group)

    def remove_from_group(self, group):
        return self.manager.remove_card_from_group(self, group)

    def attach_link(self, url):
        return self.manager.attach_link_to_card(self, url)


class ProjectManager(Manager):
    def fetch_all(self, **params):
        # For some bizarre reason, /projects doesn't fetch all projects, /projects/all does
        # It takes no parameters, so we ignore them
        return tuple(self.resource(d) for d in self.connection.api_get("projects/all"))


class Project(Resource):
    manager_class = ProjectManager
    endpoint = "projects"
    # Nested resources
    attributes = Attribute.manager()
    cards = Card.manager()
    models = Model.manager()
    group_types = GroupType.manager()

    @property
    def top_level_card(self):
        # Return a lazy card resource for the top-level card
        return self.manager.connection.cards.fetch_one(self.top_level_card_id, lazy = True)


class Session(Connection):
    class Auth(requests.auth.AuthBase):
        """
        Requests authentication provider for Kantree API requests.
        """
        def __init__(self, api_token):
            # For some reason, the API token is given as base64-encoded
            self.api_token = base64.b64encode(api_token.encode()).decode()

        def __call__(self, req):
            req.headers.update({ 'Authorization': f'Basic {self.api_token}' })
            return req

    def __init__(self, name, api_token):
        super().__init__(name, KANTREE_API, self.Auth(api_token))

    projects = Project.manager()
    cards = Card.manager()
    models = Model.manager()
    group_types = GroupType.manager()


@minion_function
def cards_assigned_to_user(session):
    """
    Returns a function that ignores its arguments and returns a list of cards
    assigned to the user.
    """
    return lambda *args: session.cards.search('@me')


@minion_function
def cards_for_project(session, project_name, with_archived = True):
    """
    Returns a function that ignores its arguments and returns a list of cards
    for the given project.
    """
    return lambda *args: (
        session.projects
            .fetch_one_by_title(project_name)
            .cards
            .fetch_all(with_archived = with_archived)
    )


@minion_function
def create_or_update_card(session, project_name):
    """
    Returns a function that accepts a ``(card, updates)`` tuple, where ``card`` can
    be ``None``, and either creates or updates the corresponding card in the given
    project.
    """
    project = session.projects.fetch_one_by_title(project_name)
    project_group_types = set(project.group_types.fetch_all())
    project_groups = set(project.top_level_card.groups.fetch_all())
    def func(item):
        card, updates = item
        # We will deal with these later
        model_name = updates.pop('model_name', None)
        state = updates.pop('state', None) or getattr(card, 'state', 'undecided')
        groups_add = updates.pop('groups_add', []) + updates.pop('groups', [])
        groups_remove = updates.pop('groups_remove', [])
        links = updates.pop('links', [])
        # Create or update the card
        if card:
            card = card.update(**updates)
        else:
            card = session.cards.create(project, **updates)
        # Set the card state
        card = card.set_state(state)
        # Set the card model
        if model_name:
            model = project.models.fetch_one_by_name(model_name)
        else:
            model = None
        card = card.set_model(model)
        # Process the groups
        for group in groups_add:
            try:
                group_type = next(gt for gt in project_group_types if gt.name == group['type'])
            except StopIteration:
                group_type = project.group_types.create(name = group['type'], one_group_per_card = "false")
                project_group_types.add(group_type)
            try:
                group = next(g for g in project_groups if g['title'] == group['name'] and g['type_id'] == group_type.id)
            except StopIteration:
                group = project.top_level_card.groups.create(type_id = group_type.id, title = group['name'])
            card = card.add_to_group(group)
        for group in groups_remove:
            try:
                group_type = next(gt for gt in project_group_types if gt.name == group['type'])
                group = next(g for g in project_groups if g['title'] == group['name'] and g['type_id'] == group_type.id)
            except StopIteration:
                # If the group type or group is not found, the card cannot be in it
                continue
            card = card.remove_from_group(group)
        # Attach any links
        for link in links:
            card = card.attach_link(link)
        return card
    return func
