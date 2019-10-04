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


def with_cache(attribute_name):
    """
    Decorator for manager methods that allows them to benefit from the cache
    for non-primary-key values.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(manager, value, *args, **kwargs):
            cache_alias = f"{attribute_name}/{value}"
            if manager.context:
                cache_alias = f"{manager.context}/{cache_alias}"
            if manager.cache.has(cache_alias):
                # Return a new resource with the manager as it's manager
                return manager.resource(manager.cache.get(cache_alias).data)
            # Otherwise, fetch the resource and put it into the cache with the alias
            return manager.cache.put(func(manager, value, *args, **kwargs), cache_alias)
        return wrapper
    return decorator


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

    def api_request(self, method, path, *args, return_response = False, **kwargs):
        """
        Makes an API request with the given arguments.
        """
        if re.match('https?://', path) is not None:
            api_url = path
        else:
            api_url = self.api_base + '/' + path
        response = getattr(self.session, method.lower())(api_url, *args, **kwargs)
        logger.info(f"REST API request: {method.upper()} {response.url}")
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
        if return_response:
            return response
        else:
            return response.json()

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
        # Don't cache lazy resources
        if lazy:
            return resource
        else:
            return self.cache.put(resource)

    def extract_data(self, response):
        # By default, just use the repsonse JSON
        return response.json()

    def next_page(self, response):
        # By default, use the links from the Link header
        return response.links.get('next', {}).get('url')

    def resource_list(self, response):
        # Paginate the response
        # We use a generator to avoid downloading pages that we might not need
        while True:
            # Since we have already downloaded the data for the response,
            # make sure that the resources are cached before yielding
            resources = [self.resource(d) for d in self.extract_data(response)]
            yield from resources
            # Once the response has been exhausted, check for a next page
            next_url = self.next_page(response)
            if next_url:
                response = self.connection.api_get(next_url, return_response = True)
            else:
                break

    def fetch_all(self, **params):
        response = self.connection.api_get(
            self.list_endpoint(),
            params = params,
            return_response = True
        )
        return self.resource_list(response)

    def fetch_one(self, key, lazy = False):
        # Even if lazy is true, return any cached instance
        if self.cache.has(key):
            # Return a new resource with this manager as it's manager
            return self.resource(self.cache.get(key).data)
        if lazy:
            return self.resource({ self.resource_class.primary_key_name: key }, True)
        return self.resource(self.connection.api_get(self.single_endpoint(key)))

    def fetch_one_by_ATTR(self, attr):
        # This method is used by __getattr__ to produce the fetch_one_by_<attr>
        # methods that allow fetching one resource by an attribute value while making
        # use of the cache
        # It is assumed that the attribute is only unique in the current context
        @with_cache(attr)
        def fetch_one_by_attr(manager, value, params = {}):
            try:
                return next(r for r in manager.fetch_all(**params) if getattr(r, attr) == value)
            except StopIteration:
                raise NotFound
        return lambda *args, **kwargs: fetch_one_by_attr(self, *args, **kwargs)

    def __getattr__(self, name):
        if not name.startswith("fetch_one_by_"):
            raise AttributeError
        return self.fetch_one_by_ATTR(name[13:])

    def create(self, **params):
        endpoint = self.list_endpoint()
        data = self.connection.api_post(endpoint, json = params)
        return self.resource(data)

    def update(self, resource_or_key, **params):
        # Check if there are actually any updates to apply
        # If we were given a key, check if there is a cached resource to compare with
        if isinstance(resource_or_key, Resource):
            resource = resource_or_key
        elif self.cache.has(resource_or_key):
            resource = self.resource(self.cache.get(resource_or_key).data)
        else:
            resource = None
        if resource:
            params = { k: v for k, v in params.items() if resource[k] != v }
            if not params:
                return resource
        endpoint = self.single_endpoint(resource_or_key)
        data = self.connection.api_put(endpoint, json = params)
        return self.resource(data)

    def delete(self, resource_or_key):
        endpoint = self.single_endpoint(resource_or_key)
        self.connection.api_delete(endpoint)
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
            raise AttributeError(name)

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
