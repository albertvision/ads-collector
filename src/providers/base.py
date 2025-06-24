from abc import ABC, abstractmethod


class BaseProvider(ABC):
    """Abstract base class for ad providers."""

    name: str

    @abstractmethod
    def fetch_data(self, start_date, end_date):
        """Return a list of ad records between the given dates."""
        raise NotImplementedError
