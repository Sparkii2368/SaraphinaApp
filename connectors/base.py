from abc import ABC, abstractmethod
from typing import Any, Dict, Generator

class BaseConnector(ABC):
    """All connectors must follow this interface."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique connector name for provenance tagging."""

    @abstractmethod
    def fetch_tasks(self) -> Generator[Dict[str, Any], None, None]:
        """Yield task descriptors (URLs, API params, IDs)."""

    @abstractmethod
    def fetch(self, task: Dict[str, Any]) -> Any:
        """Execute task and return raw payload (HTML, JSON, bytes)."""

    @abstractmethod
    def parse(self, raw: Any) -> Generator[Dict[str, Any], None, None]:
        """Yield normalized document dicts: {content, metadata}."""
