import requests
from typing import Any, Iterable

from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard
from ckanext.datapress_harvester.harvesters2.lib.harvesters import SimpleHarvester


class TflCollect(Collector[dict[str, Any]]):

    api_version = "2022-04-01-preview"

    def gather(self) -> list[str]:

        apis_url = "https://api-portal.tfl.gov.uk/developer/apis"
        params = {"api-version": self.api_version}

        api_details = requests.get(apis_url, params=params).json()["value"]

        api_urls = [f"https://api-portal.tfl.gov.uk/developer/apis/{api['id']}" for api in api_details]

        return api_urls

    def fetch(self, from_url: str) -> dict[str, Any]:

        headers = {"Accept": "application/vnd.oai.openapi+json"}  # send nothing for a simpler response
        params = {"export": "true", "api-version": self.api_version}

        api_details = requests.get(from_url, params=params, headers=headers).json()

        return dict(api_details)

    def transform(self, response_details: dict[str, Any], upstream_url: str, org_name: str) -> Iterable[SimpleStandard]:

        # get upstream id from url (before including args)
        upstream_id = upstream_url.split("/")[-1]
        # include version in upstream url
        upstream_url = f"{upstream_url}?api-version={self.api_version}"

        # include path descriptions
        operations = []

        for path, path_details in response_details["paths"].items():
            for method, method_details in path_details.items():
                operations.append(f"{method.upper()} {path}: {method_details['description']}")
                # currently not listing as resources since those descriptions don't show in search
                # operations.append(
                #     {
                #         "name": f"{method.upper()} {path}".encode(),
                #         "description": f"{method_details['description']}".encode(),
                #         "url": f"https://api-portal.tfl.gov.uk/api-details#"
                #                f"api={upstream_id}"
                #                f"&operation={method_details['operationId']}"
                #     }
                # )

        operations.insert(0, response_details['info']['description'])
        description = "\n".join(operations)

        resources =[{
                        "name": f"TfL Unified API",
                        "url": f"https://api-portal.tfl.gov.uk/api-details#api={upstream_id}"
                    }]

        yield SimpleStandard(
            package_id=SimpleStandard.create_hashed_id(upstream_url + response_details["info"]["title"]),
            title=f"TfL Unified API - {response_details['info']['title']}",
            description=description,
            upstream_url=upstream_url,
            resources=resources,
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
