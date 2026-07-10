import requests
from typing import Any, Iterable, Optional

from lib.utils import Collector, SimpleStandard


# the types here are the types of the thing you produce in a list from gather(), and the type of what's returned from fetch()
class CEDA(Collector[dict[str, Any], dict[str, Any]]):
    """Collectors form the basis of a pipeline, results of each step being passed to the next
    It's important to remember steps may be run independently in pipelines in future - hence classmethod
    """
    # see https://github.com/GreaterLondonAuthority/dfl-ckanext-havester/blob/1.x/ckanext/datapress_harvester/harvesters2/readme.md

    _target_source_url: Optional[str] = None

    def __init__(self, target_source_url: Optional[str] = None) -> None:
        super().__init__(target_source_url=target_source_url)
        # Store in class variable for @classmethod access
        CEDA._target_source_url = target_source_url

    @classmethod
    def gather(cls) -> list[dict[str, Any]]:
        # gather initial items from the configured URL
        # if no URL is provided, default to the observations endpoint
        url = cls._target_source_url or "https://catalogue.ceda.ac.uk/api/v3/observations/"
        
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        
        # Handle both direct list responses and paginated responses
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "results" in data:
            return data["results"]
        elif isinstance(data, dict) and "observations" in data:
            return data["observations"]
        else:
            # If it's a dict of URLs (old behavior), collect them
            list_of_urls = list(data.values())
            return list_of_urls

    @classmethod
    def gather_identifier(cls, received: dict[str, Any] | str) -> str:
        # how should a guid be created for each item in the list from gather()?
        # doesn't have to be this way, it just needs to be unique
        if isinstance(received, dict):
            # If it's a dict object with an ID
            if "id" in received:
                return str(received["id"])
            elif "uuid" in received:
                return received["uuid"]
            elif "@id" in received:
                return received["@id"]
            else:
                # Fallback: use first non-id field
                return str(list(received.values())[0])
        else:
            # If it's a URL string
            return received.split("/")[-2]

    @classmethod
    def fetch(cls, received: dict[str, Any] | str) -> dict[str, Any]:
        # fetch any extra metadata per item received in gather()
        if isinstance(received, dict):
            # Already have the full object from gather()
            return received
        else:
            # It's a URL
            r = requests.get(received)
            r.raise_for_status()
            return r.json()

    @classmethod
    def transform(cls, received: dict[str, Any], org_name: str) -> Iterable[SimpleStandard]:
        # make any adjustments to received data and create standardised items
        # Extract relevant fields from the observation record
        yield SimpleStandard(
            package_id=SimpleStandard.create_hashed_id(cls.gather_identifier(received)),
            title=received.get("title", received.get("name", "")),
            description=received.get("description", received.get("abstract", "")),
            org_name=org_name
        )

c = CEDA()
g = c.gather()
print("Urls from gather:", g)
for x in g:
    print("Each url gathered will be identified by:", c.gather_identifier(x))
    f = c.fetch(x)
    print("Metadata that was fetched:", f)
    t = c.transform(f, "CEDA")
    print("Final datasets that will be created:", [dataset for dataset in t])
    # only runs once on the first url, remove to run all
    break