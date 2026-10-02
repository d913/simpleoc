"""Errors describing invalid container configuration and resolution."""


class SimpleOCError(Exception):
    """Base exception for errors raised by simpleoc itself."""


class FrozenError(SimpleOCError):
    """Configuration was changed after resolution started."""


class ClosedError(SimpleOCError):
    """A closed container was used."""


class CircularDependencyError(SimpleOCError):
    """Factories depend on each other in a cycle."""


class AsyncFactoryError(SimpleOCError):
    """A synchronous container received an awaitable factory result."""
