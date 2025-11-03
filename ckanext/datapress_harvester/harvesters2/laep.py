from datetime import datetime

import requests
from typing import Any, Iterable

from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard


class LAEPCollect(Collector[dict[str, Any], dict[str, Any]]):

    dcat_url = "https://laep-datahub-alpha-cityhall.hub.arcgis.com/api/feed/dcat-ap/2.0.1.json"

    @classmethod
    def gather(cls) -> list[dict[str, Any]]:

        content = requests.get(cls.dcat_url).json()
        return content["dcat:dataset"]

    @classmethod
    def gather_identifier(cls, source_data: dict[str, Any]) -> str:

        return source_data["@id"]

    @classmethod
    def fetch(cls, from_url: dict[str, Any]) -> dict[str, Any]:

        return from_url

    @classmethod
    def transform(cls, source_data: dict[str, Any], org_name: str) -> Iterable[SimpleStandard]:

        yield SimpleStandard(
            package_id=SimpleStandard.create_hashed_id(cls.gather_identifier(source_data)),
            org_name=org_name,
            upstream_url=cls.dcat_url,
            title=source_data["dct:title"],
            description=source_data["dct:description"],
            upstream_metadata_modified=datetime.fromisoformat(source_data["dct:modified"]),
            upstream_metadata_created=datetime.fromisoformat(source_data["dct:issued"]),
            author=source_data["dct:publisher"]
        )
