import sys
from pathlib import Path
from typing import Any, Iterable

import requests

if __package__ in (None, ""):
    repo_root = Path(__file__).resolve().parents[3]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard

try:
    from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard
except ImportError:  # pragma: no cover - fallback for direct script execution
    from lib.utils import Collector, SimpleStandard


class GiGLCollect(Collector[str, dict[str, Any]]):

    base_url = "https://www.gigl.org.uk/wp-json/gigl/v1"
    datasets_url = f"{base_url}/datasets"

    @classmethod
    def gather(cls) -> list[str]:
        response = requests.get(cls.datasets_url, timeout=20)
        response.raise_for_status()

        payload = response.json()
        datasets = payload.get("datasets", [])
        if not isinstance(datasets, list):
            return []

        gathered = []
        for dataset in datasets:
            dataset_id = dataset.get("id")
            if dataset_id:
                gathered.append(f"{cls.datasets_url}/{dataset_id}")

        return gathered

    @classmethod
    def gather_identifier(cls, received: str) -> str:
        return received.rsplit("/", 1)[-1]

    @classmethod
    def fetch(cls, received: str) -> dict[str, Any]:
        response = requests.get(received, timeout=20)
        response.raise_for_status()

        payload = response.json()
        payload["upstream_url"] = received
        payload["upstream_id"] = cls.gather_identifier(received)
        return payload

    @classmethod
    def transform(cls, received: dict[str, Any], org_name: str) -> Iterable[SimpleStandard]:
        dataset_id = received.get("id") or received.get("upstream_id") or "unknown"
        title = received.get("title") or received.get("slug") or "Untitled dataset"
        description = received.get("description") or ""

        maintainer = received.get("maintainer") or {}
        author = ""
        author_email = ""
        if isinstance(maintainer, dict):
            author = maintainer.get("name") or ""
            author_email = maintainer.get("email") or ""

        resources = []
        dataset_url = received.get("url")
        if dataset_url:
            resources.append({
                "name": "GiGL dataset page",
                "url": dataset_url,
                "format": "HTML"
            })

        yield SimpleStandard(
            package_id=SimpleStandard.create_hashed_id(received.get("upstream_url") or dataset_id),
            title=title,
            description=description,
            url=dataset_url or "",
            author=author,
            author_email=author_email,
            org_name=org_name,
            upstream_url=received.get("upstream_url") or f"{cls.datasets_url}/{dataset_id}",
            license_id=received.get("license") or "",
            update_frequency=received.get("update_frequency"),
            geography_level=received.get("geography_level"),
            resources=resources or None,
        )



