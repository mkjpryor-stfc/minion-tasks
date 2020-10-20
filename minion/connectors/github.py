"""
Minion connector for GitHub.
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


class Issue(Resource):
    class Meta:
        endpoint = "/issues"


class Session(Connection, Connector):
    GITHUB_API = 'https://api.github.com'
    GITHUB_ACCEPT = 'application/vnd.github.v3+json'

    class Auth(requests.auth.AuthBase):
        def __init__(self, token):
            self.token = token

        def __call__(self, request):
            # Add the correctly formatted header to the request
            request.headers['Authorization'] = "token {}".format(self.token)
            return request

    # Register the root resources
    issues = RootResource(Issue)

    def __init__(self, name, api_token):
        self.name = name
        # Build the session to pass to the connection
        session = requests.Session()
        session.auth = self.Auth(api_token)
        # Make sure to add the version header
        session.headers.update({ 'Accept': self.GITHUB_ACCEPT })
        # Call the superclass method to initialise the connection
        super().__init__(self.GITHUB_API, session)


@minion_function
def issues(session, **kwargs):
    """
    Returns a function that returns a list of issues with the given kwargs as URL parameters.
    """
    return lambda *args: session.issues.all(**kwargs)
