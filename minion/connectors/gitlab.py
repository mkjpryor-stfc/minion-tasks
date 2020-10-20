"""
Minion connector for GitLab.
"""

import requests

from rackit import (
    Connection,
    ResourceManager as BaseResourceManager,
    Resource as BaseResource,
    RootResource,
    NestedResource,
    RelatedResource
)

from ..core import Connector, function as minion_function


class ResourceManager(BaseResourceManager):
    def extract_list(self, response):
        next_page = response.links.get('next', {}).get('url')
        return response.json(), next_page


class Resource(BaseResource):
    class Meta:
        manager_cls = ResourceManager
        update_http_verb = 'put'


class LinkManager(ResourceManager):
    def create(self, params = None, **kwargs):
        # Creating a link doesn't return anything sensible (boo!!)
        params = params.copy() if params else dict()
        params.update(kwargs)
        params = self.prepare_params(params)
        self.connection.api_post(self.prepare_url(), json = params)


class Link(Resource):
    class Meta:
        manager_cls = LinkManager
        endpoint = '/links'


class IssueManager(ResourceManager):
    def canonical_manager(self, data):
        # The canonical manager for issues is the project issue manager
        return self.related_manager(Project).get(data['project_id']).issues


class Issue(Resource):
    class Meta:
        manager_cls = IssueManager
        endpoint = '/issues'
        primary_key_field = 'iid'

    project = RelatedResource('Project', 'project_id')
    links = NestedResource(Link)


class Project(Resource):
    class Meta:
        endpoint = "/projects"

    issues = NestedResource(Issue)


class Session(Connection, Connector):
    class Auth(requests.auth.AuthBase):
        def __init__(self, token):
            self.token = token

        def __call__(self, request):
            # Add the correctly formatted header to the request
            request.headers['Authorization'] = "Bearer {}".format(self.token)
            return request

    path_prefix = '/api/v4'

    # Register the root resources
    projects = RootResource(Project)
    issues = RootResource(Issue)

    def __init__(self, name, url, api_token, verify_ssl = True):
        self.name = name
        # Build the session to pass to the connection
        session = requests.Session()
        session.auth = self.Auth(api_token)
        session.verify = verify_ssl
        # Call the superclass method to initialise the connection
        super().__init__(url, session)


@minion_function
def issues(session, **kwargs):
    """
    Returns a function that returns a list of issues with the given kwargs as URL parameters.
    """
    return lambda *args: session.issues.all(**kwargs)


@minion_function
def project_issues(session, project):
    """
    Returns a function that returns a list of issues for the given project.
    """
    return lambda *args: session.projects.find_by_path_with_namespace(project).issues.all()


@minion_function
def create_or_update_issue(session, project):
    """
    Returns a function that creates or updates an issue in the given project.
    """
    def func(item):
        issue, patch = item
        if issue:
            return issue._update(patch)
        else:
            return session.projects.find_by_path_with_namespace(project).issues.create(patch)
    return func
