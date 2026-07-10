import requests
from typing import Any, Iterable, Optional

from lib.utils import Collector, SimpleStandard


# the types here are the types of the thing you produce in a list from gather(), and the type of what's returned from fetch()
class CEDA(Collector[dict[str, Any], dict[str, Any]]):
    """Collector for CEDA catalogue metadata.
    
    Gathers observation datasets from the CEDA catalogue API.
    
    Configurable with one optional parameter:
    - source_url: URL of the CEDA API observations endpoint (defaults to v3 observations)
    
    Example json config string:
    .. code-block:: json
    {
        "url_source": "https://catalogue.ceda.ac.uk/api/v3/observations/"
    }
    """
    # see https://github.com/GreaterLondonAuthority/dfl-ckanext-havester/blob/1.x/ckanext/datapress_harvester/harvesters2/readme.md

    def __init__(self, source_url: Optional[str] = None) -> None:
        # Pass the configuration up to the base Collector class
        super().__init__(target_source_url=source_url)
        self.source_url = source_url or "https://catalogue.ceda.ac.uk/api/v3/observations/"

    def gather(self) -> list[dict[str, Any]]:
        # gather initial items from the configured URL
        r = requests.get(self.source_url, timeout=10)
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

    def gather_identifier(self, received: dict[str, Any] | str) -> str:
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

    def fetch(self, received: dict[str, Any] | str) -> dict[str, Any]:
        # fetch any extra metadata per item received in gather()
        if isinstance(received, dict):
            # Already have the full object from gather()
            return received
        else:
            # It's a URL
            r = requests.get(received)
            r.raise_for_status()
            return r.json()

    def transform(self, received: dict[str, Any], org_name: str) -> Iterable[SimpleStandard]:
        # make any adjustments to received data and create standardised items
        # Extract relevant fields from the observation record
        yield SimpleStandard(
            package_id=SimpleStandard.create_hashed_id(self.gather_identifier(received)),
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