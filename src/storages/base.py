from abc import ABC, abstractmethod


class BaseStorage(ABC):
    """Abstract base class for storage backends."""

    name: str

    @abstractmethod
    def save(self, df, output_name: str) -> None:
        """Persist dataframe. `output_name` is used as file base name."""
        raise NotImplementedError
