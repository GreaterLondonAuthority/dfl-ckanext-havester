import json
import requests
import logging
from datetime import datetime
from typing import Optional, Any

from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard

from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject

log = logging.getLogger(__name__)

def parse_datetime(timestamp):
     # Function to handle different date formats received in API response
     formats = [
         "%Y-%m-%dT%H:%M:%S.%f%z",
         "%Y-%m-%dT%H:%M:%S%z",
     ]
 
     for format in formats:
         try:
             return datetime.strptime(timestamp, format)
         except ValueError:
             continue
 
class UKPNCollect(Collector[dict[str, Any]]):
 
    def gather(self) -> list[str]:
        # UKPN api gives access to metadata for all sources under a single url
        api_urls = [
            "https://ukpowernetworks.opendatasoft.com/api/explore/v2.1/catalog/datasets/"]

        return api_urls
 
    def fetch(self, from_url: str) -> dict[str, Any]:
        response = requests.get(from_url).json()
        return response['results']

 
    def transform(self, source_details: dict[str, Any]) -> SimpleStandard:
        data = source_details["metas"]
        
        return SimpleStandard(
            package_id=f"ukpn-{source_details.get('dataset_uid')}",
            title=data.get("dublin-core", {}).get("title"),
            description=data.get("dublin-core", {}).get("description"),
            update_frequency=data.get("dublin-core", {}).get("description"),
            author = data.get("dublin-core", {}).get("creator"),
            org_name=data.get("dublin-core", {}).get("publisher"),
            org_link=data.get("dublin-core", {}).get("source"),
            notes=data.get("dublin-core", {}).get("description"),
            data_updated_at=parse_datetime(data.get("dublin-core", {}).get("modified"))
            )
 
# # TESTING
# def collector():
#     return UKPNCollect()

# urls = collector().gather()
# print(f"urls: {urls}")

# print("Fetching metadata...")
# for url in urls:
#     package_data = collector().fetch(url)

# print("\nTransforming metadata...")
# index = 1
# for i, source in enumerate(package_data):
#     try:
#         transformed_source_data = collector().transform(
#             package_data[i])

#         index = index + 1

#         if i == 9:
#             print(
#                 f"SOURCE: \n: {json.dumps(package_data[i], indent = 2)}")
#             print(f"\n TRANSFORMED: \n : {transformed_source_data}")

#     except Exception as e:
#         log.error(f"error transforming {i+1}th source id: {json.dumps(source, indent = 2)}: {e}", exc_info= True)
#         raise

# print(f"\n Transformed metadata for {index} sources")

# HARVESTER
class FingertipsHarvester(HarvesterBase):

    @staticmethod
    def collector():
        return FingertipsCollect()

    @staticmethod
    def info():
        return {
            "name": "fingertips",
            "title": "Public Health Data API",
            "description": "Harvests from Public Health Data Fingertips API"
        }

    def gather_stage(self, harvest_job):
        try:
            log.info(f"Gathering {self.info()['name']}")

            all_urls = self.collector().gather()
            all_jobs = []

            for url in all_urls:
                    obj = HarvestObject(guid=url, job=harvest_job)
                    obj.save()
                    all_jobs.append(obj.id) 
            return all_jobs

        except Exception as e:
            # todo set up proper exceptions
            logging.error(f"Gather failed: {str(e)}")
            self._save_gather_error(
                f"Couldn't gather {self.info()['name']}", harvest_job)
            raise

    def fetch_stage(self, harvest_object):
        try:
            log.info(f"Importing {harvest_object.guid}")

            url = harvest_object.guid
            content = self.collector().fetch(url)

            harvest_object.content = json.dumps(content)
            harvest_object.save()

            return True

        except Exception as e:
            # todo set up proper exceptions
            logging.error(f"Fetch failed: {str(e)}")
            self._save_object_error(
                f"Couldn't fetch {harvest_object.guid}", harvest_object)
            raise

    def import_stage(self, harvest_object):
        try:

            log.info(f"Importing {harvest_object.guid}")

            content = json.loads(harvest_object.content)

            for source in content:
                try:
                    package_dict = self.collector().transform(
                        content).as_dfl_package()

                    result = self._create_or_update_package(
                        package_dict,
                        harvest_object,
                        package_dict_form="package_show"
                    )
                except KeyError as e:
                    log.error("Couldnt find 'Description' key")

            return result

        except Exception as e:
            # todo set up proper exceptions
            logging.error(f"Import failed: {str(e)}")
            self._save_object_error(
                f"Couldn't import {harvest_object.guid}", harvest_object)
            raise