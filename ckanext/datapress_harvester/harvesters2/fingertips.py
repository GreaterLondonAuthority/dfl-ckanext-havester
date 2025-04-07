import json
import requests
import logging
from datetime import datetime
from typing import Any, Iterable

from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard
from ckanext.datapress_harvester.harvesters2.lib.harvesters import SimpleHarvester

from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject

log = logging.getLogger(__name__)


class FingertipsCollect(Collector[dict[str, Any]]):

    def gather(self) -> list[str]:
        # fingertips api gives access to metadata for all sources under a single url
        api_urls = [
            "https://fingertips.phe.org.uk/api/indicator_metadata/all?include_definition=yes&include_system_content=yes"]

        return api_urls

    def fetch(self, from_url: str) -> dict[str, Any]:
        return requests.get(from_url).json()

    def transform(self, source_details: dict[str, Any],  upstream_url: str, org_name: str) -> Iterable[SimpleStandard]:
        # find the most recent update between "uploaded" and "deleted" dates
        src_last_updated = None
        if source_details.get("DataChange"):

            timestamps = [datetime.strptime(source_details["DataChange"].get("LastUploadedAt", None), "%Y-%m-%dT%H:%M:%S"),
                          datetime.strptime(source_details["DataChange"].get("LastDeletedAt", None), "%Y-%m-%dT%H:%M:%S")]

            src_last_updated = max(
                timestamps, default=None)

        source_descriptive = source_details.get("Descriptive")

        yield SimpleStandard(
            package_id=f"fingertips-{source_details.get('IID')}",
            title=source_descriptive.get("Name", None),
            description=source_descriptive.get(
                "Definition", None),
            author=source_descriptive.get("DataSource", None),
            maintainer=source_descriptive.get(
                "srcSourceLink", None),
            update_frequency=source_descriptive.get(
                "Frequency", None),
            private=False,
            notes=source_descriptive.get(
                "Notes", None),
            # todo: check 'LatestChangeTimestampOverride' response field as alternative
            data_updated_at=src_last_updated,
            upstream_url=upstream_url,
            org_name=org_name
        )


# TESTING
# def collector():
#     return FingertipsCollect()


# urls = collector().gather()
# print(f"urls: {urls}")

# print("Fetching metadata...")
# for url in urls:
#     package_data = collector().fetch(url)

# print("\nTransforming metadata...")
# index = 1
# for i, source in enumerate(package_data):
#     try:
#         transformed_source_data = collector().transform(
#             package_data[source])

#         index = index + 1

#         if i == 800:
#             print(
#                 f"SOURCE: \n: {json.dumps(package_data[source], indent = 2)}")
#             print(f"\n TRANSFORMED: \n : {transformed_source_data}")

#     except Exception as e:
#         print(f"error transforming {i+1}th source id: {source}: {e}")
#         raise

# print(f"\n Transformed metadata for {index} sources")

# HARVESTER


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
