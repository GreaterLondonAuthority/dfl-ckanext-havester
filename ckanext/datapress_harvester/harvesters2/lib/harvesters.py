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

# yoinked from harvesters v1, could be refactored
def harvester_search_dict(source_id, page, limit):
    return {
        "fq": '+harvest_source_id:"{0}"'.format(source_id),
        "fl": "id",
        "rows": limit,
        "start": (page - 1) * limit,
    }

# yoinked from harvesters v1, could be refactored
def get_harvested_dataset_ids(harvest_source_id):
    context = {"model": model, "session": model.Session}
    page = 1
    limit = 1000
    query_result = toolkit.get_action("package_search")(
        context,
        harvester_search_dict(harvest_source_id, page, limit),
    )
    datasets = query_result["results"]
    while len(datasets) < query_result["count"]:
        page += 1
        datasets += toolkit.get_action("package_search")(
            context, harvester_search_dict(harvest_source_id, page, limit)
        )["results"]

    return {d["id"] for d in datasets}

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

        retrieved_packages = set()

        success = False

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

                retrieved_packages.add(item.package_id)

                package_dict = item.as_dfl_package()

                result = self._create_or_update_package(
                    package_dict,
                    harvest_object,
                    package_dict_form="package_show"
                )

                logging.info(f"Saved {item.title}: {result}")

            success = True

        except Exception as e:
            # todo set up proper exceptions - except Exception needed to report any problem in _save_object_error()
            # may be useful https://github.com/GSA/data.gov/wiki/Examples-of-Harvest-Job-Errors
            logging.error(f"Failed to import {harvest_object.guid}: {str(e)}")
            self._save_object_error(f"Couldn't import id {harvest_object.guid}, see logs for detail", harvest_object)

        else:
            # only clean up if there were no errors - avoid accidental deletes

            # only clean up if a collector explicitly says it's ok to do so
            if self.collector().clean_missing_upstream:
                
                # todo sort out this nested try
                try:
                    harvested_datasets_all = get_harvested_dataset_ids(harvest_object.source.id)
                    harvested_datasets_stale = harvested_datasets_all - retrieved_packages

                    for stale_id in harvested_datasets_stale:
                        toolkit.get_action("dataset_purge")(
                            base_context.copy(), {"id": stale_id}
                        )

                except Exception as e:
                    logging.error(f"Failed to remove datasets no longer upstream for {harvest_object.guid}: {str(e)}")
                    self._save_object_error(f"Successfully imported {harvest_object.guid} but failed to remove datasets no longer upstream, see logs for details",
                                            harvest_object)

        return success


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
