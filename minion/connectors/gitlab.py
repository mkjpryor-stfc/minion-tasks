"""
Connectors for the Github API v3.
"""

import functools

import requests

from ..core import function as minion_function, Connector


class Session(Connector):
    class Auth(requests.auth.AuthBase):
        """
        Requests authentication provider for GitLab API requests.
        """
        def __init__(self, api_token):
            self.api_token = api_token

        def __call__(self, req):
            req.headers.update({
                'Authorization': 'Bearer {}'.format(self.api_token)
            })
            return req

    def __init__(self, name, url, api_token):
        super().__init__(name)
        self._base_url = url
        self._session = requests.Session()
        self._session.auth = self.Auth(api_token)

    def url(self, url):
        """
        Prepend the API root to the given URL.
        """
        return f"{self._base_url}/api/v4{url}"

    def as_json(self, response):
        """
        Parse the response as JSON and return the result.
        """
        response.raise_for_status()
        return response.json()

    @functools.lru_cache()
    def project_id_for_name(self, name):
        """
        Returns the project ID for a project name.
        """
        namespace, project_name = name.split("/", maxsplit = 1)
        projects = self.as_json(self._session.get(
            self.url("/projects"),
            params = dict(search = project_name)
        ))
        project = next(p for p in projects if p['path_with_namespace'] == name)
        return project['id']

    def issues_assigned_to_user(self):
        """
        Returns the issues assigned to the user.
        """
        return self.as_json(self._session.get(
            self.url("/issues"),
            params = dict(scope = "assigned_to_me")
        ))

    def issues_for_project(self, project_name):
        """
        Returns the open issues for the given project.
        """
        project_id = self.project_id_for_name(project_name)
        return self.as_json(self._session.get(self.url(f"/projects/{project_id}/issues")))


@minion_function
def issues_assigned_to_user(session):
    """
    Returns a function that ignores its arguments and returns a list of issues
    assigned to the user.
    """
    return lambda *args: session.issues_assigned_to_user()


@minion_function
def issues_for_project(session, project_name):
    """
    Returns a function that ignores its arguments and returns a list of
    issues for the given project.
    """
    return lambda *args: session.issues_for_project(project_name)
