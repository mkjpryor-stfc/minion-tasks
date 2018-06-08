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


class ParameterMissing(LookupError):
    """
    Raised when a required parameter is missing during resolution.
    """
    def __init__(self, parameter):
        self.parameter = parameter
        super().__init__("No value for parameter '{}'".format(parameter.name))


class Parameter(collections.namedtuple('Parameter', ['name',
                                                     'hint',
                                                     'example',
                                                     'default'])):
    """
    Class representing a parameter to a template.
    """
    NO_DEFAULT = object()

    def __eq__(self, other):
        if not isinstance(other, Parameter):
            return None
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def resolve(self, values):
        """
        Resolve the value of this parameter in the given values.
        """
        parts = self.name.split(".")
        for part in parts:
            try:
                values = values[part]
            except KeyError:
                if self.default is not self.NO_DEFAULT:
                    return self.default
                raise ParameterMissing(self)
        return values


class Template:
    """
    A Minion template is a parameterisable specification of a Minion function
    using a dict-based configuration format.

    Attributes:
        name: The name of the template.
        description: A brief description of the template.
        parameters: A tuple of :class:`.Parameter`s for the template.
        spec: The dictionary specification of the template.
    """
    def __init__(self, name, description, parameters, spec):
        self.name = name
        self.description = description
        # Merge the set of given parameters with discovered ones, which will
        # have no description or example or default
        self.parameters = set(parameters).union(self._find_parameters(spec))
        self.spec = spec

    def _find_parameters(self, spec):
        if isinstance(spec, collections.abc.Mapping):
            if 'parameterRef' in spec:
                yield Parameter(
                    spec['parameterRef'],
                    # No hint or example
                    None,
                    None,
                    Parameter.NO_DEFAULT
                )
            else:
                for v in spec.values():
                    yield from self._find_parameters(v)
        elif isiterable(spec):
            for v in spec:
                yield from self._find_parameters(v)

    def _resolve_refs(self, connectors, values, spec):
        if isinstance(spec, collections.abc.Mapping):
            if 'functionRef' in spec:
                function_ref = self._resolve_refs(
                    connectors,
                    values,
                    spec['functionRef']
                )
                path = function_ref.pop('path')
                function = import_path(path)
                if not isinstance(function, MinionFunction):
                    raise TypeError(f"'{path}' is not a Minion function")
                return function(**function_ref)
            elif 'connectorRef' in spec:
                connector_name = self._resolve_refs(
                    connectors,
                    values,
                    spec['connectorRef']
                )
                try:
                    return connectors[connector_name]
                except KeyError:
                    raise LookupError(
                        f"Could not find connector '{connector_name}'"
                    )
            elif 'parameterRef' in spec:
                parameter_name = self._resolve_refs(
                    connectors,
                    values,
                    spec['parameterRef']
                )
                # Get the parameter from the list of parameters and resolve it
                # It should be impossible for this to raise StopIteration
                parameter = next(
                    p for p in self.parameters if p.name == parameter_name
                )
                return parameter.resolve(values)
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
