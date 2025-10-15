import requests
from datetime import datetime
from typing import Any, Iterable

from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard


class FingertipsCollect(Collector[dict[str, Any], dict[str, Any]]):

    catalogue_url = "https://fingertips.phe.org.uk/api/indicator_metadata/all?include_definition=yes&include_system_content=yes"

    @classmethod
    def gather(cls) -> list[dict[str, Any]]:
        # fingertips api gives access to metadata for all sources under a single url
        all_meta = requests.get(cls.catalogue_url).json()
        return list(all_meta.values())

    @ classmethod
    def gather_identifier(cls, source_data: dict[str, Any]) -> str:
        return f"{cls.catalogue_url}{source_data['IID']}"

    @classmethod
    def fetch(cls, from_url: dict[str, Any]) -> dict[str, Any]:
        return from_url

    @classmethod
    def transform(cls, source_data: dict[str, Any], org_name: str) -> Iterable[SimpleStandard]:

        # find the most recent update between "uploaded" and "deleted" dates
        src_last_updated = None
        if source_data.get("DataChange"):
            timestamps = [datetime.strptime(source_data["DataChange"]["LastUploadedAt"], "%Y-%m-%dT%H:%M:%S"),
                          datetime.strptime(source_data["DataChange"]["LastDeletedAt"], "%Y-%m-%dT%H:%M:%S")]

            src_last_updated = max(
                timestamps, default=None)

        source_descriptive = source_data["Descriptive"]

        yield SimpleStandard(
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
            upstream_url=cls.catalogue_url,
            org_name=org_name
        )
