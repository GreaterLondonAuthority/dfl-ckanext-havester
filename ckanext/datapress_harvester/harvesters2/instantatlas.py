import re
import requests
from typing import Any, Iterable

from bs4 import BeautifulSoup

from ckanext.datapress_harvester.harvesters2.lib.utils import SimpleStandard, Collector

class InstantAtlasCollect(Collector[dict[str, Any], dict[str, Any]]):
    """Collector for Instant Atlas based data portals.
    Gathers metadata from two sources:
    1. themes pages on the portal website (scraped HTML)
    2. Data Explorer via ArcGIS API
    Configurable with three required parameters:
    - source_page_url: URL of the main portal page containing themes links
    - arc_gis_url: URL of the ArcGIS FeatureServer query endpoint for data explorer
    - themes_tab: Name of the top-level tab on the portal page that contains theme links
    
    Example json config string:
    .. code-block:: json
    {
        "url_source": "https://www.croydonobservatory.org/",
        "url_arc_gis": "https://services1.arcgis.com/HumUw0sDQHwJuboT/arcgis/rest/services/Croydon_MasterTable/FeatureServer/0/query",
        "themes_tab": "Croydon Profile"
    }
    """

    def __init__(self, source_page_url: str, arc_gis_url: str, themes_tab: str) -> None:
        # Pass the configuration up to the base Collector class
        super().__init__(target_source_url=source_page_url,
                         target_arc_gis_url=arc_gis_url, themes_tab=themes_tab)

        self.source_page_url = source_page_url
        self.arc_gis_url = arc_gis_url
        self.themes_tab = themes_tab
        # Instant Atlas metadata service URL, same for all portals
        self.instant_atlas_url = "https://hub.instantatlas.com/data-catalog-metadata-service/query"

    def _extract_org_identifier(self, url: str) -> str:
        """Extract organization identifier from ArcGIS URL.
        
        Example: https://services1.arcgis.com/HumUw0sDQHwJuboT/arcgis/rest/services/Hounslow_MasterTable/FeatureServer/0/query
        Returns: Hounslow_MasterTable
        """

        # Match the service name before /FeatureServer
        match = re.search(r'/services/([^/]+)/FeatureServer', url)
        if match:
            return match.group(1)
        # Fallback: use the entire URL hash if pattern doesn't match
        return SimpleStandard.create_hashed_id(url)[:8]

    def _gather_from_data_explorer(self) -> list[dict[str, Any]]:
        """Gathers metadata from Data Explorer via ArcGIS API.

        Two API endpoints are used here: arc_gis_url (from harvester config json parameter 'url') to get a list of indicators,
        and instant_atlas_url to get detailed metadata for each indicator."""

        page_size = 2000
        current_offset = 0
        all_features: list[dict[str, Any]] = []
        max_iterations = 1000  # safeguard to prevent infinite loops from API changes

        for _ in range(max_iterations):
            ag_params = {
                # request the specific fields we need
                'f': 'json',
                'where': "Item_Type='Indicator'",
                'outFields': 'ID,Item_Type,Name,Short_Name,Theme_ID',
                'resultOffset': current_offset,
                'resultRecordCount': page_size,
                'returnDistinctValues': 'true'
            }

            arc_gis_resp = requests.get(self.arc_gis_url, params=ag_params)
            arc_gis_resp.raise_for_status()
            # Consider renaming variable for clarity or moving json() call directly into features extraction
            feature_data = arc_gis_resp.json()

            features = feature_data.get('features') or []
            if not features:
                break

            all_features.extend(features)

            # if no more features, break the loop
            if len(features) < page_size:
                break

            current_offset += len(features)

        else:
            raise RuntimeError(
                "Max iterations reached while gathering indicators. The external API may have changed behavior.")

        feature_ids = [feature['attributes']['ID'] for feature in all_features]

        # Gather detailed metadata for each indicator in batches
        batch_size = 100
        all_metadata_features = []
        for i in range(0, len(feature_ids), batch_size):
            batch_ids = feature_ids[i:i+batch_size]
            # Use where parameter to batch request indicators and filter indicators with non-null title as this is the master version containing the metadata.
            where_clause = f"IndicatorID IN ({','.join([repr(j) for j in batch_ids])}) AND Title IS NOT NULL"
            ia_params = {
                'f': 'json',
                'outFields': '*',
                'where': where_clause,
                'resultOffset': 0
            }
            instant_atlas_resp = requests.get(
                self.instant_atlas_url, params=ia_params)
            instant_atlas_resp.raise_for_status()
            instant_atlas_data = instant_atlas_resp.json()
            batch_features = instant_atlas_data.get('features', [])
            all_metadata_features.extend(batch_features)

        # Flatten features: remove 'attributes' key
        flattened_metadata = [f['attributes']
                              for f in all_metadata_features if 'attributes' in f]
        return flattened_metadata

    def _gather_from_themes_pages(self) -> list[dict[str, Any]]:
        """Gathers metadata by scraping themes pages from the source portal website.
        
        **Note:** This assumes a specific HTML structure for the portal pages. 
        Currently does not work for portals that rely on JavaScript injected report content to extract metadata from. 
        """

        # Add headers to mimic a real browser request (otherwise it is blocked by the server)
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0"
        }
        content = requests.get(self.source_page_url, headers=headers).text
        soup = BeautifulSoup(content, "html.parser")

        # Extract theme page URLs
        theme_urls = []

        themes_tab_link = soup.find("a", string=self.themes_tab)

        if not themes_tab_link:
            raise ValueError(
                f"No matching themes tab with the name:'{self.themes_tab}' found on source page. Possible harvester configuration error.")

        parent_li = themes_tab_link.find_parent(
            "li") if themes_tab_link else None
        theme_list_elements = parent_li.find_all("li") if parent_li else []

        for list_element in theme_list_elements:
            link_element = list_element.find("a")
            href = link_element.get("href") if link_element else None

            if href and href != '#':
                theme_urls.append(href)

        # For each theme page, extract metadata
        all_theme_metadata = []
        for theme_url in theme_urls:
            theme_content = requests.get(theme_url, headers=headers).text
            theme_soup = BeautifulSoup(theme_content, "html.parser")

            # Extract description
            theme_desc_elements = []

            for content_div in theme_soup.find_all("div", class_="entry-content"):
                parapraphs = content_div.find_all("p")

                if not parapraphs:
                    # Set to None if no description paragraphs found so it is consistent with API returned metadata and can be handled appropriately at transform stage
                    # Should there be a default value instead in order to still harvest the resource?
                    theme_desc_elements = None
                    continue

                for p in parapraphs:
                    text = p.get_text(strip=True)
                    # Skip empty paragraphs
                    if not text:
                        continue

                    # TODO: Description extraction is imperfect, may need further refinement to avoid unrelated text in some cases.
                    if theme_desc_elements is not None:
                        theme_desc_elements.append(text)

            theme_desc = "\n\n".join(
                theme_desc_elements) if theme_desc_elements else None

            # Extract title
            title_element = theme_soup.select_one(
                "header.page-header .page-title")
            theme_title = title_element.get_text(
                strip=True) if title_element else None

            theme_metadata: dict[str, Any] = {
                "Title": theme_title,
                "Description": theme_desc,
                "Source_URL": theme_url,
                "Upstream_URL": theme_url}

            all_theme_metadata.append(theme_metadata)

        return all_theme_metadata

    def gather(self) -> list[dict[str, Any]]:
        # SOURCE 1: Gather metadata from themes pages via scraping
        themes_metadata = self._gather_from_themes_pages()

        # SOURCE 2: Gather metadata from Data Explorer via ArcGIS API
        data_explorer_metadata = self._gather_from_data_explorer()

        # Combine all metadata sources
        all_gathered_metadata = themes_metadata + data_explorer_metadata

        return all_gathered_metadata

    def gather_identifier(self, source_data: dict[str, Any]) -> str:
        # Create unique identifier combining org and indicator to avoid id duplication across different orgs
        indicator_id = source_data["IndicatorID"] if "IndicatorID" in source_data else source_data["Title"]
        org_identifier = self._extract_org_identifier(self.arc_gis_url)

        return f"{org_identifier}_{indicator_id}"

    def fetch(self, source_data: dict[str, Any]) -> dict[str, Any]:
        return source_data

    def transform(self, source_data: dict[str, Any], org_name: str) -> Iterable[SimpleStandard]:
        # Handle required description and/or title fields being 'null' for some indicators
        if source_data.get("Description") is None or source_data.get("Title") is None:
            raise ValueError(
                "Description and Title fields need to be provided as a minimum and one or both are missing. Inspect the data source.")

        yield SimpleStandard(
            package_id=SimpleStandard.create_hashed_id(
                self.gather_identifier(source_data)),
            org_name=org_name,
            # using instant atlas metadata service as upstream url
            upstream_url=source_data.get(
                "Upstream_URL", self.instant_atlas_url),
            title=source_data.get("Title", ""),
            description=source_data.get("Description", ""),
            url=source_data.get("Source_URL", ""),
            author=source_data.get("Publisher", ""),
            license_title=source_data.get("Rights", ""),
            private=False,
            geography_level=source_data.get("Spatial"),
            update_frequency=source_data.get("Frequency"),
            data_updated_at=source_data.get("LastUpdated"),
        )
