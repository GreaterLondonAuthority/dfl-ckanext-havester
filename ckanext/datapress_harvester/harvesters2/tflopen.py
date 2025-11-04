import requests
from typing import Any, Iterable, Generator

from bs4 import BeautifulSoup

from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard


class TflOpenCollect(Collector[dict[str, Any], dict[str, Any]]):
    clean_missing_upstream = True
    source_page_url = "https://tfl.gov.uk/info-for/open-data-users/our-open-data"

    @classmethod
    def gather(cls) -> Generator[dict]:
        content = requests.get(cls.source_page_url).text
        soup = BeautifulSoup(content, "html.parser")

        # horrible parsing is simpler here but may be better done in fetch rather than gather if errors start showing

        # text from main page
        paras = soup.find_all("p")
        common = str(paras[0])  # + str(paras[4])

        # syndication guidelines pdf
        guidelines = soup.find("div", class_="multi-document-download-container")
        # how reliable are links? can we url_parse?
        guidelines_link = "https://tfl.gov.uk" + guidelines.find("a").get("href")
        guidelines_text = guidelines.find("div", class_="document-download-text").find("p").text
        guidelines_attach = guidelines.find("div", class_="document-download-attachment").find("p").text.strip()
        guidelines_desc = str(paras[3])

        # dataset per api
        sets = soup.find_all("div", class_="expandable-box")

        for s in sets:
            heading = s.find("div", class_="accordion-heading").decode_contents()
            content = s.find("div", class_="start-hidden").decode_contents()

            yield {
                "heading": heading,
                "content": content,
                "guidelines_link": guidelines_link,
                "guidelines_text": guidelines_text,
                "guidelines_attach": guidelines_attach,
                "guidelines_desc": guidelines_desc,
                "common": common
            }

    @classmethod
    def gather_identifier(cls, source_data: dict) -> str:
        return cls.source_page_url + source_data["heading"]

    @classmethod
    def fetch(cls, source_data: dict) -> dict[str, Any]:
        return source_data

    @classmethod
    def transform(cls, source_data: dict[str, Any], org_name: str) -> Iterable[SimpleStandard]:
        # guidelines link in description so it all shows in search but may be better as a resource
        guidelines = (f"""
                      <h3>Guidelines</h3>
                      <p>
                        <a href={source_data['guidelines_link']}>
                          {source_data['guidelines_text']} ({source_data['guidelines_attach']})
                        </a>
                      </p>
                        {source_data['guidelines_desc']}
                      """)

        description = source_data["content"] + guidelines + source_data["common"]

        yield SimpleStandard(
            package_id=SimpleStandard.create_hashed_id(cls.gather_identifier(source_data)),
            title=source_data["heading"],
            upstream_url=cls.source_page_url,
            description=description,
            org_name=org_name
        )
