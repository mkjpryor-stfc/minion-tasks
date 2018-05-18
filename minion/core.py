"""
Utilities for building and running Minion jobs.
"""


class MinionFunction:
    """
    Wrapper for any callable that "marks" the callable as a Minion function.

    This can be used by loaders to identify functions that have been marked for
    use with Minion and avoid executing arbitrary Python functions.
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


class Terminate(RuntimeError):
    """
    Control-flow exception that indicates processing should proceed immediately
    to the next item, similar to the ``continue`` statement in a ``for`` loop.
    """


class Job:
    def __init__(self, name, description, source, function):
        self.name = name
        self.description = description
        self.source = source
        self.function = function

    def __call__(self):
        for item in self.source():
            try:
                self.function(item)
            except Terminate:
                pass
