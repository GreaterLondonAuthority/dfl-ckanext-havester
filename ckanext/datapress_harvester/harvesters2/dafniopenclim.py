import os
import subprocess
import shutil
import json
import logging

logging = logging.getLogger(__name__)

from typing import Any, Iterable

from ckanext.datapress_harvester.harvesters2.lib.utils import SimpleStandard, Collector


class DafniCollect(Collector[dict[str, Any], dict[str, Any]]):

    @classmethod
    def gather(cls) -> list[dict[str, Any]]:
        logging.info("Gathering DAFNI datasets...")
        logging.info("DAFNI_USERNAME: %s", os.environ["DAFNI_USERNAME"])
        logging.info("DAFNI_PASSWORD: %s", os.environ["DAFNI_PASSWORD"])
        search_term = "OpenClim"

        # Get filtered datasets via the DAFNI cli
        dafni_path = shutil.which("dafni")
        if not dafni_path:
            raise RuntimeError("dafni CLI not found on PATH")

        process = subprocess.run([dafni_path, "get", "datasets", "--search", search_term,
                                 "-j"], capture_output=True, text=True, check=True, env=os.environ)
        content = json.loads(process.stdout)
        print(
            f"Found {len(content['metadata'])} datasets matching search term '{search_term}'")
        return content["metadata"]

    @classmethod
    def gather_identifier(cls, source_data: dict[str, Any]) -> str:
        return source_data["id"]["asset_id"]

    @classmethod
    def fetch(cls, source_data: dict[str, Any]) -> dict[str, Any]:
        return source_data

    @classmethod
    def transform(cls, source_data: dict[str, Any], org_name: str) -> Iterable[SimpleStandard]:
        yield SimpleStandard(
            package_id=SimpleStandard.create_hashed_id(
                cls.gather_identifier(source_data)),
            org_name=org_name,
            title=source_data["title"],
            description=source_data["description"],
            maintainer="DAFNI",
            author=source_data["source"],
            data_updated_at=source_data["modified_date"]
        )


# collector = DafniCollect()

# metadata = collector.gather()

# print(json.dumps(metadata, indent=2))
