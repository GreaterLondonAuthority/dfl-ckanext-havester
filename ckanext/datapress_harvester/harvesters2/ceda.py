import requests
from typing import Any, Iterable

from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard


# the types here are the types of the thing you produce in a list from gather(), and the type of what's returned from fetch()
class CEDA(Collector[str, dict[str, Any]]):
    """Collectors form the basis of a pipeline, results of each step being passed to the next
    It's important to remember steps may be run independently in pipelines in future - hence classmethod
    """
    # see https://github.com/GreaterLondonAuthority/dfl-ckanext-havester/blob/1.x/ckanext/datapress_harvester/harvesters2/readme.md

    @classmethod
    def gather(cls) -> list[str]:
        # gather initial items
        r = requests.get("https://catalogue.ceda.ac.uk/api/v3/", timeout=10)
        r.raise_for_status()
        list_of_urls = []
        for key, url in r.json().items():
            list_of_urls.append(url)
        return list_of_urls  # will call gather_identifier() and fetch() for each item in this list
    @classmethod
    def gather_identifier(cls, received) -> str:
        # how should a guid be created for each item in the list from gather()?
        # doesn't have to be this way, it just needs to be unique
        return received.split("/")[-2]
    @classmethod
    def fetch(cls, received) -> dict[str, Any]:
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
 
