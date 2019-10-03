"""
Module containing helpers for interacting with REST APIs.
"""

import logging
import re
import functools
import io
import pprint

import requests

from ..core import Connector


logger = logging.getLogger(__name__)


class Error(RuntimeError):
    """Base class for resource fetching errors."""


class BadRequest(Error):
    """Raised when a 400 Bad Request occurs."""


class Unauthorized(Error):
    """Raised when a 401 Unauthorized occurs."""


class Forbidden(Error):
    """Raised when a 403 Forbidden occurs."""


class NotFound(Error):
    """Raised when a 404 Not Found occurs."""


class Conflict(Error):
    """Raised when a 409 Conflict occurs."""


class ResourceCache:
    """
    Class for a cache of resources.
    """
    def __init__(self):
        self.data = dict()
        self.aliases = dict()

    def has(self, key):
        return str(key) in self.data or str(key) in self.aliases

    def get(self, key):
        try:
            return self.data[str(key)]
        except KeyError:
            return self.data[self.aliases[str(key)]]

    def put(self, resource, *aliases):
        cache_key = str(resource.primary_key)
        # The resource itself is stored against the cache key
        self.data[cache_key] = resource
        # The aliases reference the cache key
        self.aliases.update({ str(a): cache_key for a in aliases })
        return resource

    def evict(self, resource_or_key):
        cache_key = str(getattr(resource_or_key, 'primary_key', resource_or_key))
        resource = self.data.pop(cache_key, None)
        # Also evict any aliases that reference the cache key
        self.aliases = { k: v for k, v in self.aliases.items() if v != cache_key }
        return resource


class Connection(Connector):
    """
    Class for a connection to a REST API.
    """
    def __init__(self, name, api_base, auth, verify_ssl = True):
        super().__init__(name)
        self.session = requests.Session()
        self.session.verify = verify_ssl
        self.session.auth = auth
        self.api_base = api_base.rstrip('/')
        self.resources = {}
        # We maintain caches for each resource on the connection so they can be
        # shared with nested resources
        self.caches = {}

    def api_request(self, method, path, *args, as_json = True, **kwargs):
        """
        Makes an API request with the given arguments.
        """
        if re.match('https?://', path) is not None:
            api_url = path
        else:
            api_url = self.api_base + '/' + path
        logger.info(f"REST API request: {method.upper()} {api_url}")
        response = getattr(self.session, method.lower())(api_url, *args, **kwargs)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            if exc.response.status_code == 400:
                raise BadRequest
            elif exc.response.status_code == 401:
                raise Unauthorized
            elif exc.response.status_code == 403:
                raise Forbidden
            elif exc.response.status_code == 404:
                raise NotFound
            elif exc.response.status_code == 409:
                raise Conflict
            else:
                raise
        if as_json:
            return response.json()
        else:
            return response.text

    def api_get(self, *args, **kwargs):
        """
        Makes a GET API request with the given arguments.
        """
        return self.api_request('get', *args, **kwargs)

    def api_post(self, *args, **kwargs):
        """
        Makes a POST API request with the given arguments.
        """
        return self.api_request('post', *args, **kwargs)

    def api_put(self, *args, **kwargs):
        """
        Makes a PUT API request with the given arguments.
        """
        return self.api_request('put', *args, **kwargs)

    def api_patch(self, *args, **kwargs):
        """
        Makes a PATCH API request with the given arguments.
        """
        return self.api_request('patch', *args, **kwargs)

    def api_delete(self, *args, **kwargs):
        """
        Makes a DELETE API request with the given arguments.
        """
        return self.api_request('delete', *args, **kwargs)

    def close(self):
        """
        Closes the connection.
        """
        self.session.close()


class Manager:
    """
    Class for a REST API resource manager.
    """
    def __init__(self, resource_class, connection, context = None):
        self.resource_class = resource_class
        self.connection = connection
        self.context = context
        self.root_manager = None
        if context:
            # Work out if the connection has a root manager for the same resource
            root_manager_name = next(
                (
                    k
                    for k, v in vars(type(connection)).items()
                    if isinstance(v, ResourceManagerDescriptor)
                        and v.resource_class is resource_class
                ),
                None
            )
            if root_manager_name:
                self.root_manager = getattr(connection, root_manager_name)
        # Use the cache from the connection as our cache
        self.cache = connection.caches.setdefault(resource_class, ResourceCache())

    def list_endpoint(self):
        endpoint = self.resource_class.endpoint
        if self.context:
            return f"{self.context}/{endpoint}"
        else:
            return endpoint

    def single_endpoint(self, resource_or_key):
        key = getattr(resource_or_key, 'primary_key', resource_or_key)
        return f"{self.list_endpoint()}/{key}"

    def resource(self, data, lazy = False):
        # The manager that goes with the resource should be the root manager if there is one
        resource = self.resource_class(self.root_manager or self, data, lazy)
        # If we are not lazy, put the resource into the cache before returning it
        return resource if lazy else self.cache.put(resource)

    def fetch_all(self, **params):
        data = self.connection.api_get(self.list_endpoint(), params = params)
        return tuple(self.resource(d) for d in data)

    def fetch_one(self, key, lazy = False):
        # Even if lazy is true, return any cached instance
        if self.cache.has(key):
            return self.cache.get(key)
        if lazy:
            return self.resource({ self.resource_class.primary_key_name: key }, True)
        return self.resource(self.connection.api_get(self.single_endpoint(key)))

    def fetch_one_by_ATTR(self, attr):
        # This method is used by __getattr__ to produce the fetch_one_by_<attr>
        # methods that allow fetching one resource by an attribute value while making
        # use of the cache
        # It is assumed that the attribute is only unique in the current context
        def fetch_one_by_attr(value, params = {}):
            cache_alias = f"{attr}/{value}"
            if self.context:
                cache_alias = f"{self.context}/{cache_alias}"
            if self.cache.has(cache_alias):
                return self.cache.get(cache_alias)
            try:
                resource = next(r for r in self.fetch_all(**params) if getattr(r, attr) == value)
            except StopIteration:
                raise NotFound
            return self.cache.put(resource, cache_alias)
        return fetch_one_by_attr

    def __getattr__(self, name):
        if not name.startswith("fetch_one_by_"):
            raise AttributeError
        return self.fetch_one_by_ATTR(name[13:])

    def create(self, **params):
        endpoint = self.list_endpoint()
        data = self.connection.api_post(endpoint, json = params)
        return self.resource(data)

    def update(self, resource_or_key, **params):
        # For update, don't use the context if we have one
        endpoint = self.single_endpoint(resource_or_key)
        data = self.connection.api_put(endpoint, json = params)
        return self.resource(data)

    def delete(self, resource_or_key):
        # For delete, don't use the context if we have one
        endpoint = self.single_endpoint(resource_or_key)
        self.connection.api_delete(endpoint, as_json = False)
        self.cache.evict(resource_or_key)

    def action(self, resource_or_key, action, params):
        endpoint = self.single_endpoint(resource_or_key) + "/" + action
        data = self.connection.api_post(endpoint, json = params)
        return self.resource(data)


class Resource:
    """
    Class for a REST API resource.

    Data is accessible as dictionary keys and attributes.
    """
    #: The manager class to use for this resource
    manager_class = Manager
    #: The API endpoint for the resource
    endpoint = None
    #: The name of the primary key for the resource
    primary_key_name = 'id'

    def __init__(self, manager, data, lazy = False):
        self.manager = manager
        self.data = data
        self.lazy = lazy

    @property
    def primary_key(self):
        """
        The primary key for the resource.
        """
        return self.data[self.primary_key_name]

    def __getitem__(self, key):
        """
        Returns the value of the given key using dictionary key syntax.
        """
        # If we are a lazy resource and don't have the data, try to load it
        if key not in self.data and self.lazy:
            self.data = self.manager.fetch_one(self.primary_key).data
            # Once we have fetched the data, we are no longer lazy
            self.lazy = False
        return self.data[key]

    def __getattr__(self, name):
        """
        Returns the value of the given attribute.
        """
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"No such attribute '{name}'")

    def update(self, **params):
        """
        Returns a new resource which is this resource updated with the given parameters.
        """
        return self.manager.update(self, **params)

    def delete(self):
        """
        Deletes this resource.
        """
        self.manager.delete(self)

    def action(self, action, params):
        """
        Executes the specified action.
        """
        return self.manager.action(self, action, params)

    def __hash__(self):
        return hash(self.primary_key)

    def __repr__(self):
        klass = self.__class__
        return f"{klass.__module__}.{klass.__qualname__}({repr(self.data)})"

    @classmethod
    def manager(cls):
        """
        Returns a property descriptor for a manager for the resource.
        """
        return ResourceManagerDescriptor(cls)


# Register a method with the pretty printer for a resource
def pprint_resource(printer, object, stream, indent, allowance, context, level):
    write = stream.write
    klass = object.__class__
    class_name = f"{klass.__module__}.{klass.__qualname__}"
    write(class_name + '({\n')
    if len(object.data):
        write(' ' * (indent + printer._indent_per_level))
        printer._format_dict_items(
            object.data.items(),
            stream,
            indent,
            allowance + 1,
            context,
            level
        )
        write('\n')
    write(indent * ' ' + '})')

pprint.PrettyPrinter._dispatch[Resource.__repr__] = pprint_resource


class ResourceManagerDescriptor:
    def __init__(self, resource_class):
        self.resource_class = resource_class

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner):
        """
        Returns the manager for the given instance.
        """
        # owner is either Connection or a subclass of Resource
        # instance is the particular instance we are being called on
        # We require an instance
        if not instance:
            raise AttributeError()
        # We create one manager per instance and cache it on the instance
        attribute = f"_resource_manager_{self.name}"
        if not hasattr(instance, attribute):
            # How we get the connection and context depend on whether we are
            # a root resource (on a Connection) or a nested resource (on a
            # Resource)
            if isinstance(instance, Resource):
                connection = instance.manager.connection
                context = instance.manager.single_endpoint(instance)
            else:
                connection = instance
                context = None
            # Create a manager instance for the resource
            manager = self.resource_class.manager_class(self.resource_class, connection, context)
            # Cache the manager instance
            setattr(instance, attribute, manager)
        return getattr(instance, attribute)
