import logging
import requests
import hashlib
import xmltodict

from ckan import model
from ckan.lib.helpers import json
import ckan.plugins.toolkit as toolkit

from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject

from ckanext.datapress_harvester.util import (
    remove_extras,
    upsert_package_extra,
    get_package_extra_val,
    get_harvested_dataset_ids,
    add_default_keys,
    add_default_extras,
    add_existing_extras,
)
from mixins.harvester_mixin import DFLHarvesterMixin
log = logging.getLogger(__name__)

REDBRIDGE_API_URL = "http://data.redbridge.gov.uk/api/"


def _generate_resource(package_id, dataset, is_csv):
    """Generate a resource dict for use in a package_dict"""
    url = dataset["FriendlyUrl"]
    if is_csv:
        url = url.replace("XML", "CSV")

    sha1 = hashlib.sha1()
    resource_id = sha1.update(url.encode())
    resource_id = sha1.hexdigest()

    return {
        "id": resource_id,
        "package_id": package_id,
        "url": url,
        "name": dataset["Title"],
        "metedata_modified": dataset["DateUpdated"],
        "last_modified": dataset["DateUpdated"],
        "format": "csv" if is_csv else "xml",
    }


class RedbridgeHarvester(HarvesterBase, DFLHarvesterMixin):
    def info(self):
        return {
            "name": "redbridge",
            "title": "Redbridge",
            "description": "Harvests from Redbridge's DataShare API",
            "form_config_interface": "Text",
        }

    def gather_stage(self, harvest_job):
        pkg_dicts = []

        try:
            response = requests.get(REDBRIDGE_API_URL)
        except requests.exceptions.ConnectionError as e:
            self._save_gather_error(
                f"Connection error for Redbridge API: {REDBRIDGE_API_URL}",
                harvest_job,
            )
            return pkg_dicts

        # The Redbrige API doesn't have a single "list all datasets" endpoint,
        # you have to go through a bit of a convoluted paths from "categories" to
        # "schemas" to "datasets".
        #
        # The rest of the code in this function finds all the datasets served by
        # the Redbridge API and converts them into a package_dict-like thing.

        category_urls = [
            f"{REDBRIDGE_API_URL}{c['FriendlyUrl']}"
            for c in xmltodict.parse(response.text)["ArrayOfRestCategory"][
                "RestCategory"
            ]
        ]

        for c in category_urls:
            try:
                category = requests.get(c)
            except requests.exceptions.ConnectionError as e:
                self._save_gather_error(
                    f"Connection error for category: {c}",
                    harvest_job,
                )
                continue

            schemas = xmltodict.parse(category.text)["ArrayOfRestSchema"]["RestSchema"]
            if not isinstance(schemas, list):
                schemas = [schemas]

            for s in schemas:
                schema_title = s["Title"]
                schema_description = s["ShortDescription"]

                datasets_url = f"{c}/{s['FriendlyUrl']}"

                try:
                    datasets = requests.get(datasets_url)
                except requests.exceptions.ConnectionError as e:
                    self._save_gather_error(
                        f"Connection error for category: {datasets_url}",
                        harvest_job,
                    )
                    continue

                datasets_metadata = xmltodict.parse(datasets.text)[
                    "ArrayOfRestDataSet"
                ]["RestDataSet"]
                if not isinstance(datasets_metadata, list):
                    datasets_metadata = [datasets_metadata]

                for d in datasets_metadata:
                    full_title = f"Redbrige - {schema_title} - {d['Title']}"
                    sha1 = hashlib.sha1()
                    package_id = sha1.update(full_title.encode())
                    package_id = sha1.hexdigest()

                    package_dict = {
                        "id": package_id,
                        "title": full_title,
                        "name": full_title,
                        "notes": schema_description,
                        "license_id": "uk-ogl",
                        "metadata_modified": d["DateUpdated"],
                        "upstream_metadata_created": d["DateUpdated"],
                        "upstream_metadata_modified": d["DateUpdated"],
                        "resources": [
                            _generate_resource(package_id, d, is_csv=False),
                            _generate_resource(package_id, d, is_csv=True),
                        ],
                    }
                    pkg_dicts.append(package_dict)

        # Create a Set of dataset ids fetched from upstream,
        # for comparing with those that have been harvested previously and are already in the database
        fetched_ids = {p["id"] for p in pkg_dicts}

        # Get the Set of ids of datasets in the database that belong to this harvest source
        existing_dataset_ids = get_harvested_dataset_ids(harvest_job.source.id)

        # Datasets that are present locally but not upstream need to be deleted locally
        to_be_deleted = existing_dataset_ids - fetched_ids
        log.info(f"{len(to_be_deleted)} datasets need to be deleted")

        # Create harvest objects for each dataset
        object_ids = []
        try:
            for p in pkg_dicts:
                obj = HarvestObject(
                    guid=p["id"], job=harvest_job, content=json.dumps(p)
                )
                obj.save()
                object_ids.append(obj.id)

            for i in to_be_deleted:
                # the dataset_purge function in the import_stage only needs the dataset ID to be able to purge the dataset.
                pkg_dict = {"id": i, "action": "delete"}
                obj = HarvestObject(
                    guid=i, job=harvest_job, content=json.dumps(pkg_dict)
                )
                obj.save()
                object_ids.append(obj.id)
            return object_ids
        except Exception as e:
            self._save_gather_error("%r" % e.message, harvest_job)

    def fetch_stage(self, harvest_object):
        return True

    def import_stage(self, harvest_object):
        base_context = {
            "model": model,
            "session": model.Session,
            "user": self._get_user_name(),
        }
        if not harvest_object:
            log.error("No harvest object received")
            return False

        if harvest_object.content is None:
            self._save_object_error(
                "Empty content for object %s" % harvest_object.id,
                harvest_object,
                "Import",
            )
            return False

        try:
            package_dict = json.loads(harvest_object.content)
            if package_dict.get("action", None) == "delete":
                log.info(f"Deleting dataset with ID: {package_dict['id']}")
                result = toolkit.get_action("dataset_purge")(
                    base_context.copy(), package_dict
                )
                return True
        except Exception as e:
            self._save_object_error(
                "Failed to parse harvest object: %s" % e, harvest_object, "Import"
            )

        try:
            # Set the owner org of the new dataset to the org set in the harvest source
            harvest_source = toolkit.get_action("package_show")(
                base_context.copy(), {"id": harvest_object.source.id}
            )
            
            org = harvest_source.get("owner_org")
            remote_orgs = self.config.get("remote_orgs", None)   
            mapped_org = self.get_mapped_organization(base_context, harvest_object, org["name"], remote_orgs, package_dict, None)
            package_dict["owner_org"] = mapped_org

            add_default_keys(package_dict)

            add_existing_extras(package_dict, base_context.copy())

            add_default_extras(package_dict)

            upsert_package_extra(
                package_dict["extras"], "harvest_source_frequency", harvest_object.source.frequency
            )

            result = self._create_or_update_package(
                package_dict, harvest_object, package_dict_form="package_show"
            )

            return result
        except Exception as e:
            self._save_object_error("%s" % e, harvest_object, "Import")
