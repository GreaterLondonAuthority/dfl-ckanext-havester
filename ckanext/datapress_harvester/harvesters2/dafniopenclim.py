import os
import subprocess
import shutil
import json

from typing import Any, Iterable

from ckanext.datapress_harvester.harvesters2.lib.utils import SimpleStandard, Collector


class DafniCollect(Collector[dict[str, Any], dict[str, Any]]):
    """
    Collector for DAFNI datasets related to OpenClim. This harvester uses the DAFNI CLI to fetch datasets that match the search term "OpenClim".
    
    Requires following environment variables to be set for DAFNI CLI authentication to a service account:
    - DAFNI_USERNAME
    - DAFNI_PASSWORD
    """

    @classmethod
    def gather(cls) -> list[dict[str, Any]]:

        search_term = "OpenClim"

        # Get filtered datasets via the DAFNI cli
        dafni_path = shutil.which("dafni")
        if not dafni_path:
            raise RuntimeError("dafni CLI not found on PATH")

        process = subprocess.run([dafni_path, "get", "datasets", "--search", search_term,
                                  "-j"], capture_output=True, text=True, check=False, env=os.environ)

        # Check return code manually and extract message from stdout and stderr
        if process.returncode != 0:
            error_msg = process.stdout + "\n" + process.stderr
            raise RuntimeError(f"Error running dafni CLI: {error_msg}")

        content = json.loads(process.stdout)

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
