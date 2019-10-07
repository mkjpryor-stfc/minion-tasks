"""
Connectors for the Kantree API.
"""

import base64
import functools
from urllib.parse import urlsplit, parse_qs, urlencode

import requests

from ..core import function as minion_function
from .rest import Connection, Resource, Manager


KANTREE_API = "https://kantree.io/api/1.0"


class KantreeManager(Manager):
    def next_page(self, response):
        next_page = response.headers.get('X-Kantree-NextPage')
        if next_page:
            # Split the URL
            url_parts = urlsplit(response.url)
            # Parse the existing params into a dict
            params = parse_qs(url_parts.query)
            # Update the pages element
            params.update(page = next_page)
            # Update the url parts with the new query params
            next_url_parts = url_parts._replace(query = urlencode(params, doseq = True))
            return next_url_parts.geturl()
        else:
            return None


class Group(Resource):
    manager_class = KantreeManager
    endpoint = "groups"


class GroupType(Resource):
    manager_class = KantreeManager
    endpoint = "group-types"
    # Nested resources
    groups = Group.manager()


class Attribute(Resource):
    manager_class = KantreeManager
    endpoint = "attributes"


class Model(Resource):
    manager_class = KantreeManager
    endpoint = "models"


class CardManager(KantreeManager):
    def search(self, filters):
        """
        Search cards using the given filters.
        """
        response = self.connection.api_get(
            "search",
            params = dict(filters = filters),
            return_response = True
        )
        return self.resource_list(response)

    def create(self, project, **params):
        data = self.connection.api_post(
            f"cards/{project.top_level_card_id}/children",
            json = params
        )
        return self.resource(data)

    def _card_or_id_to_card(self, card_or_id):
        if isinstance(card_or_id, Card):
            return card_or_id
        elif self.cache.has(card_or_id):
            return self.resource(self.cache.get(card_or_id).data)
        else:
            return None

    def set_card_archived(self, card_or_id, archived):
        """
        Sets the archived state of the given card.
        """
        card = self._card_or_id_to_card(card_or_id)
        if card and card.is_archived == archived:
            return card
        endpoint = self.single_endpoint(card_or_id) + "/archive"
        if archived:
            data = self.connection.api_post(endpoint)
        else:
            data = self.connection.api_delete(endpoint)
        return self.resource(data)

    def set_card_model(self, card_or_id, model = None):
        """
        Sets the model of given card to the given model.
        """
        card = self._card_or_id_to_card(card_or_id)
        model_id = getattr(model, 'id', model)
        if card and card.model_id == model_id:
            return card
        params = dict(model_id = model_id) if model_id else dict()
        return self.action(card_or_id, "model", params)

    def set_card_state(self, card_or_id, state = 'undecided'):
        """
        Sets the state of the card.
        """
        card = self._card_or_id_to_card(card_or_id)
        if card and card.state == state:
            return card
        return self.action(card_or_id, "state", dict(state = state))

    def add_card_to_group(self, card_or_id, group):
        group_id = getattr(group, 'id', group)
        card = self._card_or_id_to_card(card_or_id)
        if card:
            for card_group in card.groups:
                if card_group['id'] == group_id:
                    return card
        return self.action(card_or_id, "add-to-group", dict(group_id = group_id))

    def remove_card_from_group(self, card_or_id, group):
        group_id = getattr(group, 'id', group)
        card = self._card_or_id_to_card(card_or_id)
        if card:
            for card_group in card.groups:
                if card_group['id'] == group_id:
                    break
            else:
                return card
        return self.action(card_or_id, "remove-from-group", dict(group_id = group_id))

    def attach_link_to_card(self, card_or_id, url):
        """
        Attaches the given URL to the card.
        """
        if isinstance(card_or_id, Card):
            card = card_or_id
        else:
            card = self.fetch_one(card_or_id)
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
                card.attributes_
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
    # cards have attributes and groups properties already, so the managers need different names
    attributes_ = Attribute.manager()
    groups_ = Group.manager()

    @property
    def project(self):
        return self.manager.connection.projects.fetch_one(self.project_id, lazy = True)

    def set_archived(self, archived):
        return self.manager.set_card_archived(self, archived)

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


class ProjectManager(KantreeManager):
    def fetch_all(self, **params):
        # For some bizarre reason, /projects doesn't fetch all projects, /projects/all does
        # It takes no parameters, so we ignore them
        response = self.connection.api_get("projects/all", return_response = True)
        return self.resource_list(response)


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
    Returns a function that accepts a ``(card, patch)`` tuple, where ``card`` can
    be ``None``, and either creates or updates the corresponding card in the given
    project.
    """
    project = session.projects.fetch_one_by_title(project_name)
    project_group_types = set(project.group_types.fetch_all())
    project_groups = set(project.top_level_card.groups_.fetch_all())
    def func(item):
        card, patch = item
        # We will deal with these later
        has_archived = 'archived' in patch
        if has_archived:
            archived = patch.pop('archived')
        has_state = 'state' in patch
        if has_state:
            state = patch.pop('state')
        has_model_name = 'model_name' in patch
        if has_model_name:
            model_name = patch.pop('model_name')
        groups_add = patch.pop('groups_add', []) + patch.pop('groups', [])
        groups_remove = patch.pop('groups_remove', [])
        links = patch.pop('links', [])
        # Create or update the card
        if card:
            card = card.update(**patch)
        else:
            card = session.cards.create(project, **patch)
        # Set the archived state
        if has_archived:
            card = card.set_archived(archived)
        # Set the card state
        if has_state:
            card = card.set_state(state)
        # Set the card model
        if has_model_name:
            card = card.set_model(project.models.fetch_one_by_name(model_name))
        # Process the groups
        for group in groups_add:
            try:
                group_type = next(gt for gt in project_group_types if gt.name == group['type'])
            except StopIteration:
                group_type = project.group_types.create(
                    name = group['type'],
                    one_group_per_card = "false"
                )
                project_group_types.add(group_type)
            try:
                group = next(
                    g
                    for g in project_groups
                    if g.title == group['name'] and g.type_id == group_type.id
                )
            except StopIteration:
                group = project.top_level_card.groups_.create(
                    type_id = group_type.id,
                    title = group['name']
                )
                project_groups.add(group)
            card = card.add_to_group(group)
        for group in groups_remove:
            try:
                group_type = next(gt for gt in project_group_types if gt.name == group['type'])
                group = next(
                    g
                    for g in project_groups
                    if g.title == group['name'] and g.type_id == group_type.id
                )
            except StopIteration:
                # If the group type or group is not found, the card cannot be in it
                continue
            card = card.remove_from_group(group)
        # Attach any links
        for link in links:
            card = card.attach_link(link)
        return card
    return func
