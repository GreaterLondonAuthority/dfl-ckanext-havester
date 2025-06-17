import requests
from datetime import datetime
from typing import Any, Iterable

from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard
from ckanext.datapress_harvester.harvesters2.lib.harvesters import SimpleHarvester


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


class UKPNCollect(Collector[dict[str, Any], dict[str, Any]]):

    catalogue_url = "https://ukpowernetworks.opendatasoft.com/api/explore/v2.1/catalog/datasets/"

    def gather(self) -> list[dict[str, Any]]:
        """UKPN api gives access to metadata for all sources under a single url with pagination"""
        all_metas = []
        offset = 0
        limit = 100  # Maximum 100 allowed per request

        response = requests.get(self.catalogue_url).json()
        available_metas = response.get('total_count', 0)
        
        while len(all_metas) < available_metas:
            # Construct URL with pagination parameters
            paginated_url = f"{self.catalogue_url}?limit={limit}&offset={offset}&include_links=true"
            
            batch_response = requests.get(paginated_url).json()
            batch_results = batch_response.get('results', [])
            
            # Append to the list
            all_metas.extend(batch_results)
                
            # Move to next batch
            offset += limit
                
            # API documentation mentions offset+limit should be less than 10000
            if offset > 10000:
                break
        
        return all_metas
    
    def gather_identifier(self, source_data: dict[str, Any]) -> str:
        return f"{self.catalogue_url}{source_data.get('dataset_uid')}"

    def fetch(self, from_url: dict[str, Any]) -> dict[str, Any]:
        return from_url

    def transform(self, source_data: dict[str, Any], org_name: str) -> Iterable[SimpleStandard]:

        source_metas = source_data["metas"]

        yield SimpleStandard(
            package_id=f"ukpn-{source_data.get('dataset_uid')}",
            title=source_metas.get("dublin-core", {}).get("title"),
            description=source_metas.get(
               "dublin-core", {}).get("description"),
            update_frequency=source_metas.get(
                "dublin-core", {}).get("description"),
            author=source_metas.get("dublin-core", {}).get("creator"),
            org_name=org_name,
            upstream_url=self.catalogue_url,
            org_link=source_metas.get("dublin-core", {}).get("source"),
            notes=source_metas.get("dublin-core", {}).get("description"),
            data_updated_at=parse_datetime(
                source_metas.get("dublin-core", {}).get("modified"))
        )


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

UKPNCollect.gather