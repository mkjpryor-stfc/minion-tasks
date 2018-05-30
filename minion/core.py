"""
Utilities for building and running Minion jobs.
"""

import collections
import collections.abc
import importlib


class MinionFunction:
    """
    Wrapper for any callable that marks it as a Minion function.

    This can be used by loaders to identify functions that have been marked for
    use with Minion and avoid executing arbitrary Python functions.

    Although no checks are made by this class, a callable is "Minion-compatible"
    if it can be called with a (possibly empty) set of "configuration"
    parameters and returns a new function. This happens exactly once.
    The returned function should take a single argument representing the
    incoming item. This may be called many times. A function for which the
    single argument is optional is referred to as a "source".
    """
    def __init__(self, wrapped):
        self._wrapped = wrapped

    def __call__(self, *args, **kwargs):
        return self._wrapped(*args, **kwargs)


def function(f):
    """
    Decorator that marks the decorated function as a Minion function.
    """
    return MinionFunction(f)


def import_path(path):
    """
    Imports the given dotted path.
    """
    module, name = path.rsplit(".", 1)
    return getattr(importlib.import_module(module), name)


def isiterable(obj):
    """
    Tests if an object is iterable but not a string or bytes.
    """
    return (
        not isinstance(obj, (str, bytes)) and
        isinstance(obj, collections.abc.Iterable)
    )


class Connector:
    """
    Base class for connectors.

    Connectors provide access to an external service, e.g. GitHub.
    """
    def __init__(self, name):
        self.name = name

    @classmethod
    def from_config(cls, name, config):
        """
        Takes a name and a configuration directory and returns the specified
        connector.
        """
        path = config.pop('path')
        connector_class = import_path(path)
        if not issubclass(connector_class, Connector):
            raise TypeError(f"'{path}' is not a Minion connector")
        return connector_class(name, **config)


class Template(collections.namedtuple('Template', ['name',
                                                   'description',
                                                   'spec'])):
    """
    A Minion template is a parameterisable specification of a Minion function
    using a dict-based configuration format.

    Attributes:
        name: The name of the template.
        description: A brief description of the template.
        spec: The dictionary specification of the template.
    """
    class Parameter(collections.namedtuple('Parameter', ['name', 'default'])):
        """
        Class representing a parameter to a template.
        """
        NO_DEFAULT = object()

    def _resolve_parameters(self, spec):
        if isinstance(spec, collections.abc.Mapping):
            if 'parameterRef' in spec:
                ref = spec['parameterRef']
                yield self.Parameter(
                    ref['name'],
                    ref.get('default', self.Parameter.NO_DEFAULT)
                )
            else:
                for v in spec.values():
                    yield from self._resolve_parameters(v)
        elif isiterable(spec):
            for v in spec:
                yield from self._resolve_parameters(v)

    @property
    def parameters(self):
        """
        Set of parameters required by the template.
        """
        return set(self._resolve_parameters(self.spec))

    def _resolve_parameter_value(self, name, values):
        parts = name.split(".")
        for part in parts:
            try:
                values = values[part]
            except KeyError:
                raise ValueError(f"Missing parameter '{name}'")
        return values

    def check_values(self, values):
        """
        Checks if the given values satisfy the required parameters. If not, a
        ``ValueError`` is raised.
        """
        for param in self.parameters:
            if param.default is self.Parameter.NO_DEFAULT:
                _ = self._resolve_parameter_value(param.name, values)

    def _resolve_refs(self, connectors, values, spec):
        if isinstance(spec, collections.abc.Mapping):
            if 'functionRef' in spec:
                function_spec = self._resolve_refs(
                    connectors,
                    values,
                    spec['functionRef']
                )
                path = function_spec.pop('path')
                function = import_path(path)
                if not isinstance(function, MinionFunction):
                    raise TypeError(f"'{path}' is not a Minion function")
                return function(**function_spec)
            elif 'connectorRef' in spec:
                connector_name = self._resolve_refs(
                    connectors,
                    values,
                    spec['connectorRef']
                )
                try:
                    return connectors[connector_name]
                except KeyError:
                    raise LookupError(f"Could not find connector '{connector_name}'")
            elif 'parameterRef' in spec:
                parameter_spec = self._resolve_refs(
                    connectors,
                    values,
                    spec['parameterRef']
                )
                parameter_name = parameter_spec['name']
                try:
                    return self._resolve_parameter_value(
                        parameter_name,
                        values
                    )
                except ValueError:
                    if 'default' in parameter_spec:
                        return parameter_spec['default']
                    else:
                        raise LookupError(
                            f"No value for parameter '{parameter_name}'"
                        )
            else:
                return {
                    k: self._resolve_refs(connectors, values, v)
                    for k, v in spec.items()
                }
        elif isiterable(spec):
            return [self._resolve_refs(connectors, values, v) for v in spec]
        else:
            return spec

    def resolve_refs(self, connectors, values):
        """
        Resolves the references in the template using the given connectors and
        parameter values and returns the resulting function.

        Args:
            connectors: The connectors to use, indexed by name.
            values: The parameter values indexed by parameter name.

        Returns:
            The fully parameterised function.
        """
        return self._resolve_refs(connectors, values, self.spec)


class Job(collections.namedtuple('Job', ['name',
                                         'description',
                                         'template',
                                         'values'])):
    """
    A Minion job is a specific parameterisation of a template. It is not a
    complete function yet though, as the connectors are global and injected at
    runtime.

    Attributes:
        name: The name of the job.
        description: A brief description of the job.
        template: The :class:`Template` that the job uses.
        values: The parameter values to be used when resolving template refs.
    """
    def run(self, connectors):
        """
        Runs the job using the given connectors.

        Args:
            connectors: The connectors to use, indexed by name.
        """
        result = self.template.resolve_refs(connectors, self.values)()
        # If the result is an iterable, ensure it has run to completion
        if not isinstance(result, str) and \
           isinstance(result, collections.Iterable):
            iterator = iter(result)
            try:
                while True:
                    next(iterator)
            except StopIteration:
                pass
