import requests
from typing import Any, Iterable

from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard
from ckanext.datapress_harvester.harvesters2.lib.harvesters import SimpleHarvester


class TflCollect(Collector[str, dict[str, Any]]):

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

        # neither the url nor the internal id are included in the response, so include these to send to next step
        api_details["upstream_id"] = from_url.split("/")[-1]
        api_details["upstream_url"] = f"{from_url}?api-version={self.api_version}"

        return dict(api_details)

    def transform(self, response_details: dict[str, Any], org_name: str) -> Iterable[SimpleStandard]:

        # Show the tfl portal for the current api as a resource
        resources = [{
            "name": f"TfL Unified API",
            "url": f"https://api-portal.tfl.gov.uk/api-details#api={response_details['upstream_id']}"
        }]

        # include path descriptions in main description
        operations = []

        for path, path_details in response_details["paths"].items():
            for method, method_details in path_details.items():
                operations.append(f"{method.upper()} {path}: {method_details['description']}")
                # currently not listing as resources since resources don't show in search & some require args
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
        description = "<p>".join(operations)

        yield SimpleStandard(
            package_id=SimpleStandard.create_hashed_id(response_details["upstream_url"] + response_details["info"]["title"]),
            title=f"TfL Unified API - {response_details['info']['title']}",
            description=description,
            upstream_url=response_details["upstream_url"],
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
