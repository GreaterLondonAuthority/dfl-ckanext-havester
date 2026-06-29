
import dateutil.parser

import requests
from typing import Any, Iterable

from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard


class LAEPCollect(Collector[dict[str, Any], dict[str, Any]]):

    def gather(self) -> list[dict[str, Any]]:
        content = requests.get(self._target_source_url).json()
        return content["dcat:dataset"]

    @classmethod
    def gather_identifier(cls, source_data: dict[str, Any]) -> str:
        return source_data["@id"]

    @classmethod
    def fetch(cls, source_data: dict[str, Any]) -> dict[str, Any]:
        return source_data

    def transform(self, source_data: dict[str, Any], org_name: str) -> Iterable[SimpleStandard]:

        # todo simpler dcat parsing method


        # not sure how structured title, description, format are
        resources = []
        for distribution in source_data["dcat:distribution"]:

            # move description to title if there isn't a title
            resource = {
                "name": distribution.get("dct:title") or distribution.get("dct:description") or source_data["dct:title"],
                "description": distribution.get("dct:description") if "dct:title" in distribution else "",
                "format": distribution.get("dct:format", {}).get("@id","")
            }

            # if no distribution then assume link to main page
            if "dcat:accessURL" in distribution:
                resource["url"] = distribution["dcat:accessURL"]["@id"]
            else:
                resource["url"] = source_data["dct:landingPage"]

            resources.append(resource)

        yield SimpleStandard(
            package_id=SimpleStandard.create_hashed_id(self.gather_identifier(source_data)),
            org_name=org_name,
            upstream_url=self._target_source_url,
            title=source_data["dct:title"],
            description=source_data["dct:description"],

            resources=resources,

            # datetime.isoformat() works with a trailing z from python3.11 onwards
            upstream_metadata_modified=dateutil.parser.isoparse(source_data["dct:modified"]["@value"]),
            upstream_metadata_created=dateutil.parser.isoparse(source_data["dct:issued"]["@value"]),
            author=source_data["dct:publisher"]["foaf:name"]
        )
