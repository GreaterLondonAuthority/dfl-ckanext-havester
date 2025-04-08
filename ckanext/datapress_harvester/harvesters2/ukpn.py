import requests
import logging
from datetime import datetime
from typing import Any, Iterable

from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard
from ckanext.datapress_harvester.harvesters2.lib.harvesters import SimpleHarvester

log = logging.getLogger(__name__)


def parse_datetime(timestamp):
    # Function to handle different date formats received in API response
    formats = [
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
    ]

    for format in formats:
        try:
            return datetime.strptime(timestamp, format)
        except ValueError:
            continue


class UKPNCollect(Collector[dict[str, Any]]):

    def gather(self) -> list[str]:
        # UKPN api gives access to metadata for all sources under a single url
        api_urls = [
            "https://ukpowernetworks.opendatasoft.com/api/explore/v2.1/catalog/datasets/"]

        return api_urls

    def fetch(self, from_url: str) -> dict[str, Any]:
        response = requests.get(from_url).json()
        return response['results']

    def transform(self, sources: dict[str, Any], upstream_url: str, org_name: str) -> Iterable[SimpleStandard]:
        transformed_sources = []
        for source in sources:
            source_data = source["metas"]

            print(source_data.get("dublin-core", {}).get("title"),)
            transformed_sources.append(SimpleStandard(
                package_id=f"ukpn-{source.get('dataset_uid')}",
                title=source_data.get("dublin-core", {}).get("title"),
                description=source_data.get(
                    "dublin-core", {}).get("description"),
                update_frequency=source_data.get(
                    "dublin-core", {}).get("description"),
                author=source_data.get("dublin-core", {}).get("creator"),
                org_name=org_name,
                upstream_url=upstream_url,
                org_link=source_data.get("dublin-core", {}).get("source"),
                notes=source_data.get("dublin-core", {}).get("description"),
                data_updated_at=parse_datetime(
                    source_data.get("dublin-core", {}).get("modified"))
            ))

        return transformed_sources


# TESTING


# def collector():
#     return UKPNCollect()


# urls = collector().gather()
# print(f"urls: {urls}")

# print("Fetching metadata...")
# for url in urls:
#     package_data = collector().fetch(url)

# print("\nTransforming metadata...")

# transformed_source_data = collector().transform(
#     package_data, upstream_url="ulr_example", org_name="example_org")


# print(f"\n Transformed metadata for {len(transformed_source_data)} sources")

# HARVESTER


class UkpnHarvester(SimpleHarvester):

    @staticmethod
    def collector():
        return UKPNCollect()

    @staticmethod
    def info():
        return {
            "name": "ukpn",
            "title": "UK Power Networks API",
            "description": "Harvests from UK Power Networks API"
        }
