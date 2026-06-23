import requests
from typing import Any, Iterable

from lib.utils import Collector, SimpleStandard


class DAFNI(Collector[dict[str, Any], dict[str, Any]]):

    API_URL = "https://snd.secure.dafni.rl.ac.uk/public/catalogue/"

    @classmethod
    def gather(cls) -> list[dict[str, Any]]:
        """
        STEP 1 — GATHER

        Send POST request and return list of dataset objects.
        """

        payload = {
            "sort_by": "recent",
            "offset": {
                "start": 0,
                "size": 500
            },
            "search_text": "OpenCLIM"
        }

        r = requests.post(cls.API_URL, json=payload, timeout=10)
        r.raise_for_status()

        data = r.json()

        # Extract the list of datasets
        return data.get("metadata", [])


    @classmethod
    def gather_identifier(cls, received: dict[str, Any]) -> str:
        """
        NOT A PIPELINE STEP — just define unique ID
        """
        return received["id"]["asset_id"]


    @classmethod
    def fetch(cls, received: dict[str, Any]) -> dict[str, Any]:
        """
        STEP 2 — FETCH

        No-op: gather already returned full metadata
        """
        return received


    @classmethod
    def transform(cls, received: dict[str, Any], org_name: str) -> Iterable[SimpleStandard]:
        """
        STEP 3 — TRANSFORM

        Convert DAFNI metadata into SimpleStandard
        """

        title = received.get("title", "")
        description = received.get("description", "")
        author = received.get("contact_name", "")

        # Build a readable external link if possible
        upstream_id = received["id"]["dataset_uuid"]
        upstream_url = f"https://snd.secure.dafni.rl.ac.uk/catalogue/datasets/{upstream_id}"

        yield SimpleStandard(
            package_id=SimpleStandard.create_hashed_id(received["id"]["asset_id"]),
            title=title,
            description=description,
            org_name=org_name,
            author=author,
            upstream_url=upstream_url,
            url=upstream_url,
            license_title=None,
            data_updated_at=received.get("modified_date"),
            private=False
        )

c = DAFNI()
g = c.gather()

print("Items from gather:", g)

for x in g:
    print("Identifier:", c.gather_identifier(x))

    f = c.fetch(x)
    print("Fetched data:", f)

    t = c.transform(f, "DAFNI")
    print("Final datasets:", [dataset.__dict__ for dataset in t])

    # only run first
    break