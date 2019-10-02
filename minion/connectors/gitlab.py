"""
Connectors for the Github API v3.
"""

import functools

import requests

from ..core import function as minion_function
from .rest import Connection, Resource


class Issue(Resource):
    endpoint = "issues"


class Project(Resource):
    endpoint = "projects"
    issues = Issue.manager()


class Session(Connection):
    class Auth(requests.auth.AuthBase):
        """
        Requests authentication provider for GitLab API requests.
        """
        def __init__(self, api_token):
            self.api_token = api_token

        def __call__(self, req):
            req.headers.update({ 'Authorization': f"Bearer {self.api_token}" })
            return req

    def __init__(self, name, url, api_token, verify_ssl = True):
        api_base = url.rstrip('/') + "/api/v4"
        super().__init__(name, api_base, self.Auth(api_token), verify_ssl)

    projects = Project.manager()
    issues = Issue.manager()


@minion_function
def issues_assigned_to_user(session):
    """
    Returns a function that ignores its arguments and returns a list of issues
    assigned to the user.
    """
    return lambda *args: session.issues.fetch_all(scope = "assigned_to_me")


@minion_function
def issues_for_project(session, project_name):
    """
    Returns a function that ignores its arguments and returns a list of
    issues for the given project.
    """
    return lambda *args: (
        session
            .projects
            .fetch_one_by_path_with_namespace(project_name)
            .issues
            .fetch_all()
    )
