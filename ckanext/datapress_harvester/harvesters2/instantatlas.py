
import requests
from typing import Any, Iterable

from ckanext.datapress_harvester.harvesters2.lib.utils import SimpleStandard, Collector


class InstantAtlasCollect(Collector[dict[str, Any], dict[str, Any]]):
    # Two API endpoints are used here: arc_gis_url to get a list of indicators,
    # and instant_atlas_url to get detailed metadata for each indicator.
    arc_gis_url = "https://services1.arcgis.com/HumUw0sDQHwJuboT/arcgis/rest/services/Richmond_upon_Thames_MasterTable/FeatureServer/0/query"
    instant_atlas_url = "https://hub.instantatlas.com/data-catalog-metadata-service/query"

    @classmethod
    def gather(cls) -> list[dict[str, Any]]:
        # Gather all indicators
        page_size = 2000
        current_offset = 0
        all_features: list[dict[str, Any]] = []

        while True:
            ag_params = {
                # request the specific fields we need
                'f': 'json',
                'where': "Item_Type='Indicator'",
                'outFields': 'ID,Item_Type,Name,Short_Name,Theme_ID',
                'resultOffset': current_offset,
                'resultRecordCount': page_size,
                'returnDistinctValues': 'true'
            }

            arc_gis_resp = requests.get(cls.arc_gis_url, params=ag_params)
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
                cls.instant_atlas_url, params=ia_params)
            instant_atlas_resp.raise_for_status()
            instant_atlas_data = instant_atlas_resp.json()
            batch_features = instant_atlas_data.get('features', [])
            all_metadata_features.extend(batch_features)

        # Flatten features: remove 'attributes' key
        flattened_metadata = [f['attributes']
                              for f in all_metadata_features if 'attributes' in f]

        return flattened_metadata

    @classmethod
    def gather_identifier(cls, source_data: dict[str, Any]) -> str:
        return source_data["IndicatorID"]

    @classmethod
    def fetch(cls, source_data: dict[str, Any]) -> dict[str, Any]:
        return source_data

    @classmethod
    def transform(cls, source_data: dict[str, Any], org_name: str) -> Iterable[SimpleStandard]:

        yield SimpleStandard(
            package_id=SimpleStandard.create_hashed_id(
                cls.gather_identifier(source_data)),
            org_name=org_name,
            # using instant atlas metadata service as upstream url
            upstream_url=cls.instant_atlas_url,
            title=source_data.get("Title", ""),
            description=source_data.get("Description", ""),
            url=source_data.get("Source_URL", ""),
            author=source_data.get("Publisher", ""),
            license_title=source_data.get("Rights", ""),
            private=False,
            geography_level=source_data.get("Spatial"),
            update_frequency=source_data.get("Frequency"),
            notes=source_data.get("Methodology"),
            data_updated_at=source_data.get("LastUpdated"),
        )
