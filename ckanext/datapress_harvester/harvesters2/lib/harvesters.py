import json
import logging
from abc import abstractmethod
from typing import Any, Dict

from ckanext.datapress_harvester.harvesters2.lib.utils import SimpleStandard, Collector
from ckanext.datapress_harvester.harvesters2 import fingertips, tflunified, ukpn, tflopen

from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra

from ckan import model
import ckan.plugins.toolkit as toolkit

logging = logging.getLogger(__name__)


"""
SimpleHarvester provides a generalised harvester that runs a Collector

If your source needs extra behaviour, it's preferable to improve SimpleHarvester rather than create many bespoke ones!

Example linking to your collector:

class FingertipsHarvester(SimpleHarvester):

    @staticmethod
    def collector() -> fingertips.FingertipsCollect:
        return fingertips.FingertipsCollect()

    @staticmethod
    def info():
        return {
            "name": "fingertips",
            "title": "Public Health Data API",
            "description": "Harvests from Public Health Data Fingertips API"
        }
"""

class SimpleHarvester(HarvesterBase):

    @staticmethod
    @abstractmethod
    def collector() -> Collector[Any, Any]:
        pass

    @staticmethod
    @abstractmethod
    def info() -> Dict[str, Any]:
        pass

    def gather_stage(self, harvest_job):
        try:
            logging.info(f"Gathering {self.info()['name']}")

            gathered_items = self.collector().gather()
            all_jobs = []

            for item in gathered_items:
                identifier = self.collector().gather_identifier(item)

                # should hashing happen in the collector?
                obj = HarvestObject(guid=SimpleStandard.create_hashed_id(identifier),
                                    job=harvest_job,
                                    content=json.dumps(item))
                obj.save()

                all_jobs.append(obj.id)

            return all_jobs

            # todo delete datasets that don't exist in gathered items

        except Exception as e:
            # todo set up proper exceptions
            logging.error(f"Gather failed: {str(e)}")
            self._save_gather_error(f"Couldn't gather {self.info()['name']}", harvest_job)

        return []

    def fetch_stage(self, harvest_object):

        logging.info(f"Fetching {self.info()['name']}")

        try:

            fetched_content = self.collector().fetch(json.loads(harvest_object.content))
            harvest_object.content = json.dumps(fetched_content)

        except Exception as e:
            # todo set up proper exceptions
            # may be useful https://github.com/GSA/data.gov/wiki/Examples-of-Harvest-Job-Errors
            logging.error(f"Failed to fetch {harvest_object.guid}: {str(e)}")
            self._save_object_error(f"Couldn't fetch id {harvest_object.guid}, see logs for detail", harvest_object)

            return False

        return True

    def import_stage(self, harvest_object):

        logging.info(f"Importing {self.info()['name']}")

        try:

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

            for item in self.collector().transform(json.loads(harvest_object.content), org_name=harvester_org):
                package_dict = item.as_dfl_package()

                result = self._create_or_update_package(
                    package_dict,
                    harvest_object,
                    package_dict_form="package_show"
                )

                logging.info(f"Saved {item.title}: {result}")

            return True

        except Exception as e:
            # todo set up proper exceptions
            # may be useful https://github.com/GSA/data.gov/wiki/Examples-of-Harvest-Job-Errors
            logging.error(f"Failed to import {harvest_object.guid}: {str(e)}")
            self._save_object_error(f"Couldn't import id {harvest_object.guid}, see logs for detail", harvest_object)

        return False


class FingertipsHarvester(SimpleHarvester):

    @staticmethod
    def collector():
        return fingertips.FingertipsCollect()

    @staticmethod
    def info():
        return {
            "name": "fingertips",
            "title": "Public Health Data API",
            "description": "Harvests from Public Health Data Fingertips API"
        }


class TflUnifiedHarvester(SimpleHarvester):

    @staticmethod
    def collector():
        return tflunified.TflCollect()

    @staticmethod
    def info() -> dict[str, str]:
        return {
            "name": "tfl-unified",
            "title": "TfL Unified API",
            "description": "Harvests from TfL's Unified API"
        }


class UkpnHarvester(SimpleHarvester):

    @staticmethod
    def collector():
        return ukpn.UKPNCollect()

    @staticmethod
    def info():
        return {
            "name": "ukpn",
            "title": "UK Power Networks API",
            "description": "Harvests from UK Power Networks API"
        }


class TflOpenHarvester(SimpleHarvester):

    @staticmethod
    def collector() -> tflopen.TflOpenCollect:
        return tflopen.TflOpenCollect()

    @staticmethod
    def info() -> dict[str, str]:
        return {
            "name": "tfl-open-data",
            "title": "TfL Open Data",
            "description": "Harvests from TfL's Open Data Summary page"
        }
