"""

"""

import functools
import pprint
import collections

import jinja2
import yaml

from .core import function as minion_function, Job


@minion_function
def compose(functions):
    """
    Returns a function that composes the given functions, i.e. the result of
    the previous function is used as the input to the next.

    The returned function can take any number of positional arguments.
    """
    first, *rest = functions
    return lambda *args: functools.reduce(lambda i, f: f(i), rest, first(*args))


@minion_function
def collect(function):
    """
    Returns a function that accepts an iterable as the incoming item and returns
    a new iterable that is the result of applying the given function to each
    item.
    """
    return lambda items: (function(item) for item in items)


@minion_function
def select(predicate):
    """
    Returns a function that accepts an iterable as the incoming item and returns
    a new iterable containing only the items for which the given predicate
    returns true.
    """
    return lambda items: (item for item in items if predicate(item))


@minion_function
def zip_matching(matcher):
    """
    Returns a function that accepts a tuple containing two iterables as the
    incoming item and returns an iterable of tuples of matching items as per
    the given matcher.
    """
    def func(item):
        first, second = item
        # second will be iterated multiple times, so force it to be a tuple
        second = tuple(second)
        for item1 in first:
            for item2 in second:
                if matcher((item1, item2)):
                    yield (item1, item2)
                    break
            else:
                yield (item1, None)
    return func


@minion_function
def where(condition, then, default = lambda item: item):
    """
    Returns a function that takes an iterable as the incoming item and returns
    a new iterable where each item is the result of ``then`` for items for which
    ``condition`` returns true and ``default`` otherwise. If not explicitly
    given, ``default`` is the identity function.
    """
    def func(items):
        for item in items:
            if condition(item):
                yield then(item)
            else:
                yield default(item)
    return func


@minion_function
def take(number):
    """
    Returns a function that takes an iterable as the incoming item and returns
    a new iterable containing at most the first ``number`` items.
    """
    def func(items):
        for i, item in enumerate(items):
            if i == number:
                break
            yield item
    return func


@minion_function
def identity():
    """
    Returns an identity function, i.e. a function that just returns the
    incoming item.
    """
    return lambda item: item


@minion_function
def fork(functions):
    """
    Returns a function that executes each of the given functions for the
    incoming item and returns a tuple of the results in the same order.
    """
    return lambda item: tuple(f(item) for f in functions)


@minion_function
def when(condition, then, default = identity):
    """
    Returns a function that evaluates the given condition for the incoming item
    and returns the result of executing ``then`` or ``default`` depending on
    whether the condition returns ``True`` or ``False``.
    """
    return lambda item: then(item) if condition(item) else default(item)


@minion_function
def template(template, globals = None):
    """
    Returns a function that evaluates the given Jinja2 template with the
    incoming item as ``input`` and the optional dictionary of globals. The
    result is parsed as YAML and returned.
    """
    globals = globals if globals is not None else {}
    template = jinja2.Template(template)
    return lambda item: yaml.safe_load(template.render(input = item, **globals))


@minion_function
def expression(expression, globals = None):
    """
    Returns a function that evaluates the given Jinja2 expression with the
    incoming item as ``input`` and optional dictionary of globals and returns
    the result.
    """
    globals = globals if globals is not None else {}
    expression = jinja2.Environment().compile_expression(expression)
    return lambda item: expression(input = item, **globals)


@minion_function
def pretty_print():
    """
    Returns a function that pretty-prints the incoming item using
    ``pprint.pprint`` before returning it.
    """
    def func(item):
        # Convert anything iterable to a list first
        if isinstance(item, collections.Iterable) and not isinstance(item, str):
            to_print = list(item)
        else:
            to_print = item
        pprint.pprint(to_print)
        return item
    return func


@minion_function
def exit():
    """
    Returns a function that exits the pipeline.
    """
    def func(item):
        raise Job.Exit()
    return func
