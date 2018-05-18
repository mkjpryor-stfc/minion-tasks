"""

"""

import functools
import pprint

import jinja2
import yaml

from .core import Terminate, function as minion_function


@minion_function
def identity(item):
    """
    Function that just returns the incoming item.
    """
    return item


@minion_function
def pipeline(*steps):
    """
    Returns a function that executes the given steps in sequence for the
    incoming item.
    """
    return lambda item: functools.reduce(lambda i, s: s(i), steps, item)


@minion_function
def fork_join(**parts):
    """
    Returns a function that executes each of the given named functions for the
    incoming item. The result is a dictionary containing the result of each
    invocation by name.
    """
    return lambda item: { name: func(item) for name, func in parts.items() }


@minion_function
def when(condition, then, default = identity):
    """
    Returns a function that evaluates the given condition for the incoming item
    and returns the result of executing ``then`` or ``default`` depending on
    whether the condition returns ``True`` or ``False``.
    """
    return lambda item: then(item) if condition(item) else default(item)


@minion_function
def template(template):
    """
    Returns a function that evaluates the given Jinja2 template with the
    incoming item as ``input``. The result is parsed as YAML and returned.
    """
    template = jinja2.Template(template)
    return lambda item: yaml.load(template.render(input = item))


@minion_function
def expression(expression):
    """
    Returns a function that evaluates the given Jinja2 expression with the
    incoming item as ``input`` and returns the result.
    """
    expression = jinja2.Environment().compile_expression(expression)
    return lambda item: expression(input = item)


@minion_function
def pretty_print(item):
    """
    Function that pretty-prints the item using ``pprint.pprint`` before
    returning it.
    """
    pprint.pprint(item)
    return item


@minion_function
def terminate(item):
    """
    Function that terminates processing for the current item by raising
    :class:`Terminate`.
    """
    raise Terminate
