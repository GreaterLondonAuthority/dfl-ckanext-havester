import requests
from typing import Any, Iterable, Optional

try:
    from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard
except ImportError:  # pragma: no cover - fallback for direct script execution
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
        items: list[dict[str, Any]] = []
        next_url = self.source_url
        seen_urls: set[str] = set()

        while next_url:
            if next_url in seen_urls:
                break
            seen_urls.add(next_url)

            r = requests.get(next_url, timeout=10)
            r.raise_for_status()
            data = r.json()

            if isinstance(data, list):
                items.extend(data)
                break

            if isinstance(data, dict):
                if "results" in data:
                    items.extend(data.get("results", []))
                elif "observations" in data:
                    items.extend(data.get("observations", []))
                else:
                    return list(data.values())

                next_url = data.get("next") or data.get("next_url") or data.get("nextPage") or data.get("next_page")
                if not next_url:
                    break
                continue

            break

        return items

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
