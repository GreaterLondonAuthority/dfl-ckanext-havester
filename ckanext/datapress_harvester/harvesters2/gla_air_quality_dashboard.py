from typing import Any, Iterable

import requests

try:
    from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard
except ImportError:  # pragma: no cover - fallback for direct script execution
    from lib.utils import Collector, SimpleStandard


class GlaAirQualityDashboardCollect(Collector[dict[str, Any], dict[str, Any]]):
    """Collector for the Greater London Authority air quality dashboard catalogue."""

    CATALOGUE_URL = "https://wkzfnyshd4.execute-api.eu-west-2.amazonaws.com/Dev/catalogue"

    def __init__(self, catalogue_url: str = CATALOGUE_URL) -> None:
        self.catalogue_url = catalogue_url

    def gather(self) -> list[dict[str, Any]]:
        response = requests.get(self.catalogue_url, timeout=20)
        response.raise_for_status()
        payload = response.json()
        datasets = payload.get("datasets") or []
        return datasets

    def gather_identifier(self, received: dict[str, Any]) -> str:
        return str(received.get("identifier") or received.get("title") or "")

    def fetch(self, received: dict[str, Any]) -> dict[str, Any]:
        return received

    def transform(self, received: dict[str, Any], org_name: str) -> Iterable[SimpleStandard]:
        title = received.get("title") or ""
        description = received.get("description") or ""
        access = received.get("access") or {}
        endpoint = access.get("endpoint") or ""
        spatial_coverage = received.get("spatial_coverage") or {}
        update_frequency = received.get("update_frequency") or {}
        cadence = update_frequency.get("cadence") or ""
        geography_region = spatial_coverage.get("region") or ""

        yield SimpleStandard(
            package_id=SimpleStandard.create_hashed_id(self.gather_identifier(received)),
            org_name=org_name,
            upstream_url=self.catalogue_url,
            title=title,
            description=description,
            url=endpoint,
            private=False,
            geography_level=geography_region,
            update_frequency=cadence,
        )

