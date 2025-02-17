import json
import requests
import logging
from typing import Any

from ckanext.datapress_harvester.harvesters2.lib.utils import Collector, SimpleStandard

from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra

from ckan import model
import ckan.plugins.toolkit as toolkit


logging = logging.getLogger(__name__)


class TflCollect(Collector[dict[str, Any]]):

    def gather(self, api_version: str = "2022-04-01-preview") -> list[str]:

        apis_url = "https://api-portal.tfl.gov.uk/developer/apis"
        params = {"api-version": api_version}

        api_details = requests.get(apis_url, params=params).json()["value"]

        api_urls = [f"https://api-portal.tfl.gov.uk/developer/apis/{api['id']}" for api in api_details]

        return api_urls

    def fetch(self, from_url: str, api_version: str = "2022-04-01-preview") -> dict[str, Any]:

        headers = {"Accept": "application/vnd.oai.openapi+json"}  # send nothing for a simpler response
        params = {"export": "true", "api-version": api_version}

        api_details = requests.get(from_url, params=params, headers=headers).json()

        return dict(api_details)

    def transform(self, response_details: dict[str, Any], upstream_url: str, org_name: str) -> SimpleStandard:

        # include path descriptions
        # todo how do we actually want to represent this? Resources won't show in search and it looks a bit silly
        operations = []
        for path, path_details in response_details["paths"].items():
            for method, method_details in path_details.items():
                #operations.append(f"{method.upper()} {path}: {method_details['description']}")
                operations.append(
                    {"name": method_details['description'],
                     "description": f"{method.upper()} {path}"}
                )

        return SimpleStandard(
            package_id=SimpleStandard.create_hashed_id(upstream_url + response_details["info"]["title"]),
            title=f"TfL Unified API - {response_details['info']['title']}",
            description=response_details['info']['description'],
            upstream_url=upstream_url,
            resources=operations,
            org_name=org_name
        )


class TflUnifiedHarvester(HarvesterBase):

    # todo investigate having one standard DfLHarvester class - would ckan extensions support that?

    @staticmethod
    def collector():
        return TflCollect()

    @staticmethod
    def info():
        return {
            "name": "tfl-unified",
            "title": "TfL Unified API",
            "description": "Harvests from TfL's Unified API"
        }

    def gather_stage(self, harvest_job):
        try:
            logging.info(f"Gathering {self.info()['name']}")

            all_urls = self.collector().gather()
            all_jobs = []

            for url in all_urls:

                obj = HarvestObject(guid=SimpleStandard.create_hashed_id(url), job=harvest_job, content=url)
                obj.save()

                all_jobs.append(obj.id)

            return all_jobs

        except Exception as e:
            # todo set up proper exceptions
            logging.error(f"Gather failed: {str(e)}")
            self._save_gather_error(f"Couldn't gather {self.info()['name']}", harvest_job)

        return []

    def fetch_stage(self, harvest_object):
        try:

            source_url = harvest_object.content

            logging.info(f"Fetching {source_url}")

            content = self.collector().fetch(source_url)

            harvest_object.content = json.dumps(content)
            harvest_object.save()

            return True

        except Exception as e:
            # todo set up proper exceptions
            # https://github.com/GSA/data.gov/wiki/Examples-of-Harvest-Job-Errors
            logging.error(f"Fetch failed: {str(e)}")
            self._save_object_error(f"Couldn't fetch {harvest_object.content}", harvest_object)

        return False

    def import_stage(self, harvest_object):
        try:

            content = json.loads(harvest_object.content)
            logging.info(f"Importing {content.get('info')}")

            # object.source.publisher_id is left empty so look it up from the api for no reason:|
            base_context = {
                "model": model,
                "session": model.Session,
                "user": self._get_user_name(),
            }
            harvest_source = toolkit.get_action("package_show")(
                base_context.copy(), {"id": harvest_object.source.id}
            )
            harvester_org = harvest_source.get("owner_org")


            package_dict = self.collector().transform(content,
                                                      upstream_url=harvest_object.source.url,
                                                      org_name=harvester_org).as_dfl_package()

            result = self._create_or_update_package(
                package_dict,
                harvest_object,
                package_dict_form="package_show"
            )

            return result

        except Exception as e:
            # todo set up proper exceptions
            logging.error(f"Import failed: {str(e)}")
            self._save_object_error(f"Couldn't import {harvest_object.source.url}", harvest_object)

        return False