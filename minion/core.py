"""
Utilities for building and running Minion jobs.
"""

import types


class MinionFunction:
    """
    Wrapper for any "compatible" callable that marks it as a Minion function.

    This can be used by loaders to identify functions that have been marked for
    use with Minion and avoid executing arbitrary Python functions.

    A callable is "compatible" if it can be called with a (possibly empty) set
    of "configuration" parameters and returns a new function. This happens
    exactly once. The returned function should take a single argument
    representing the incoming item. This may be called many times. A function
    for which the single argument is optional is referred to as a "source".
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


class Job:
    """
    A Minion job is a wrapper around a :class:`MinionFunction`.
    """
    def __init__(self, name, description, function):
        self.name = name
        self.description = description
        self.function = function

    def __call__(self):
        """
        Call the Minion function with the given connectors as the only argument.
        """
        result = self.function()
        # If the result is a generator, ensure it has run
        if isinstance(result, types.GeneratorType):
            try:
                while True:
                    next(result)
            except StopIteration:
                pass
