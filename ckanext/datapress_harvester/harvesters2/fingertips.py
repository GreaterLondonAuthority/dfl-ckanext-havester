import requests
from datetime import datetime
from typing import Any, Iterable

from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard
from ckanext.datapress_harvester.harvesters2.lib.harvesters import SimpleHarvester


class FingertipsCollect(Collector[dict[str, Any]]):

    def gather(self) -> list[str]:
        # fingertips api gives access to metadata for all sources under a single url
        api_urls = [
            "https://fingertips.phe.org.uk/api/indicator_metadata/all?include_definition=yes&include_system_content=yes"]

        return api_urls

    def fetch(self, from_url: str) -> dict[str, Any]:
        return requests.get(from_url).json()

    def transform(self, sources: dict[str, Any], upstream_url: str, org_name: str) -> Iterable[SimpleStandard]:
        transformed_sources = []

        for source in sources:
            source_data = sources[source]
            # find the most recent update between "uploaded" and "deleted" dates
            src_last_updated = None
            if source_data.get("DataChange"):
                timestamps = [datetime.strptime(source_data["DataChange"]["LastUploadedAt"], "%Y-%m-%dT%H:%M:%S"),
                              datetime.strptime(source_data["DataChange"]["LastDeletedAt"], "%Y-%m-%dT%H:%M:%S")]

                src_last_updated = max(
                    timestamps, default=None)

            source_descriptive = source_data["Descriptive"]

            transformed_sources.append(SimpleStandard(
                package_id=f"fingertips-{source_data.get('IID')}",
                title=source_descriptive.get("Name"),
                description=source_descriptive.get(
                    "Definition"),
                author=source_descriptive.get("DataSource"),
                maintainer=source_descriptive.get(
                    "srcSourceLink"),
                update_frequency=source_descriptive.get(
                    "Frequency"),
                private=False,
                notes=source_descriptive.get(
                    "Notes"),
                # todo: check 'LatestChangeTimestampOverride' response field as alternative
                data_updated_at=src_last_updated,
                upstream_url=upstream_url,
                org_name=org_name
            ))

        return transformed_sources


class FingertipsHarvester(SimpleHarvester):

    @staticmethod
    def collector():
        return FingertipsCollect()

    @staticmethod
    def info():
        return {
            "name": "fingertips",
            "title": "Public Health Data API",
            "description": "Harvests from Public Health Data Fingertips API"
        }
