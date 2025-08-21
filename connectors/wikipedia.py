import requests
from typing import Any, Dict, Generator
from connectors.base import BaseConnector

class WikipediaConnector(BaseConnector):
    @property
    def name(self) -> str:
        return "wikipedia"

    def fetch_tasks(self) -> Generator[Dict[str, Any], None, None]:
        topics = ["Artificial_intelligence", "Machine_learning"]
        for title in topics:
            yield {"title": title}

    def fetch(self, task: Dict[str, Any]) -> Any:
        url = f"https://en.wikipedia.org/api/rest_v1/page/plain/{task['title']}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.text

    def parse(self, raw: Any) -> Generator[Dict[str, Any], None, None]:
        yield {
            "connector": self.name,
            "content": raw,
            "metadata": {}
        }
