import requests
from typing import Any, Iterable

from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard

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
        r = requests.get(received)
        # filter for relevance to london here if it can't be worked out beforehand
        # pagination here too
        return r.json() # will call transform() on this result
    @classmethod
    def transform(cls, received, org_name: str) -> Iterable[SimpleStandard]:
        # make any adjustments to B received from fetch() and create standardised items
        # any transformation that needs doing to get from the raw data received from fetch(), to get it
        # into the format you want it to be in a dataset
        # you don't have to produce 1:1, whatever list you produce will have a library dataset created for each
        # SimpleStandard contained in it
        # (e.g. if you ended up producing multiples you can return [SimpleStandard1, SimpleStandard2]
        yield SimpleStandard(package_id=SimpleStandard.create_hashed_id(received["next"]),
                             title="",
                             description="",
                             org_name=org_name)

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
 
