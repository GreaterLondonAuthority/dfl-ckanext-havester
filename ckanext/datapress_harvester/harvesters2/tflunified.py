import requests
from typing import Any, Iterable

from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard
from ckanext.datapress_harvester.harvesters2.lib.harvesters import SimpleHarvester


class TflCollect(Collector[dict[str, Any]]):

    def gather(self, api_version: str = "2022-04-01-preview") -> list[str]:

        apis_url = "https://api-portal.tfl.gov.uk/developer/apis"
        params = {"api-version": api_version}

        api_details = requests.get(apis_url, params=params).json()["value"]

        api_urls = [f"https://api-portal.tfl.gov.uk/developer/apis/{api['id']}" for api in api_details]

        return api_urls

    def fetch(self, from_url: str, api_version: str = "2022-04-01-preview") -> dict[str, Any]:

        headers = {"Accept": "application/vnd.oai.openapi+json"}  # send nothing for a simpler response
        params = {"export": "true", "api-version": api_version}

        api_details = requests.get(from_url, params=params, headers=headers).json()

        return dict(api_details)

    def transform(self, response_details: dict[str, Any], upstream_url: str, org_name: str) -> Iterable[SimpleStandard]:

        # include path descriptions
        # todo how do we actually want to represent this? Resources won't show in search and it looks a bit silly
        operations = []
        for path, path_details in response_details["paths"].items():
            for method, method_details in path_details.items():
                # operations.append(f"{method.upper()} {path}: {method_details['description']}")
                operations.append(
                    {"name": method_details['description'],
                     "description": f"{method.upper()} {path}"}
                )

        yield SimpleStandard(
            package_id=SimpleStandard.create_hashed_id(upstream_url + response_details["info"]["title"]),
            title=f"TfL Unified API - {response_details['info']['title']}",
            description=response_details['info']['description'],
            upstream_url=upstream_url,
            resources=operations,
            org_name=org_name
        )


class TflUnifiedHarvester(SimpleHarvester):

    @staticmethod
    def collector() -> TflCollect:
        return TflCollect()

    @staticmethod
    def info() -> dict[str, str]:
        return {
            "name": "tfl-unified",
            "title": "TfL Unified API",
            "description": "Harvests from TfL's Unified API"
        }
