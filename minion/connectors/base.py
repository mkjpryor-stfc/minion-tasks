"""
Base classes for connectors.
"""


class Provider:
    """
    Base class for Minion providers.
    """
    def __init__(self, name):
        self.name = name
