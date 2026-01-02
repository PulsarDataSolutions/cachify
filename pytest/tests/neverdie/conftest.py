import pytest

from cachify import clear_never_die_registry


@pytest.fixture(autouse=True)
def clear_neverdie_state():
    """Clear never_die global state before each test to prevent cross-test pollution."""
    clear_never_die_registry()
    yield
    clear_never_die_registry()
