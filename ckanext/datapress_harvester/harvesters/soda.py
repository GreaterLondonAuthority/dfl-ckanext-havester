import logging
import requests
import hashlib
import datetime
from ckan import model
from ckan.lib.helpers import json
import ckan.plugins.toolkit as tk
from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject
from ckan.logic import NotFound 
from .mixins import DFLHarvesterMixin

from ckanext.datapress_harvester.util import (
    get_package_extra_val,
    upsert_package_extra,
    sanitise,
    get_harvested_dataset_ids,
    add_default_keys,
    add_default_extras,
    add_existing_extras
)

log = logging.getLogger(__name__)

# In ckan, we should be able to add a license list such as
# https://licenses.opendefinition.org/licenses/groups/all.json by setting the ckan.licenses_group_url field in the config, but that doesn't seem to be
# working as we get an error at http://localhost:5000/api/3/action/license_list
licenses = {"UK Open Government Licence v3": "OGL-UK-3.0",
            "Public Domain": "other-pd",
            "Open Data Commons Public Domain Dedication and License": "PDDL-1.0",
            "Creative Commons 1.0 Universal (Public Domain Dedication)": "CC0-1.0"}

formats = {"application/pdf": "pdf",
           "application/zip": "zip",
           "application/x-zip-compressed": "zip",
           "text/plain": "txt",
           "image/png": "png",
           "application/msword": "doc",
           "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
           "application/vnd.openxmlformats-officedocument.wordprocessingml.template": "dotx",
           "application/vnd.ms-word.document.macroEnabled.12": "docm",
           "application/vnd.ms-word.template.macroEnabled.12": "dotm",
           "application/vnd.ms-excel": "xls",
           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
           "application/vnd.openxmlformats-officedocument.spreadsheetml.template": "xltx",
           "application/vnd.ms-excel.sheet.macroEnabled.12": "xlsm",
           "application/vnd.ms-excel.template.macroEnabled.12": "xltm",
           "application/vnd.ms-excel.addin.macroEnabled.12": "xlam",
           "application/vnd.ms-excel.sheet.binary.macroEnabled.12": "xlsb",
           "application/vnd.ms-excel.sheet.binary.macroenabled.12": "xlsb",
           "application/vnd.ms-powerpoint": "ppt",
           "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx",
           "application/vnd.openxmlformats-officedocument.presentationml.template": "potx",
           "application/vnd.openxmlformats-officedocument.presentationml.slideshow": "ppsx",
           "application/vnd.ms-powerpoint.addin.macroEnabled.12": "ppam",
           "application/vnd.ms-powerpoint.presentation.macroEnabled.12": "pptm",
           "application/vnd.ms-powerpoint.template.macroEnabled.12": "potm",
           "application/vnd.ms-powerpoint.slideshow.macroEnabled.12": "ppsm",}

def to_iso_date(opendata_date_str):
    dt = datetime.datetime.strptime(opendata_date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    return dt.isoformat()

class SODAHarvester(HarvesterBase, DFLHarvesterMixin):
    url = None
    domain = None
    app_token = None
    create_organisations = False

    def _set_config(self, source):
        self.url = source.url.rstrip("/")
        self.domain = self.url.split("://")[1]
        config = json.loads(source.config)
        self.app_token = config["app_token"]
        self.create_organisations = config.get("remote_orgs") == "create"

    def info(self):
        return {
            "name": "soda",
            "title": "Socrata Open Data API",
            "description": "Harvests from a Socrata Open Data source",
            "form_config_interface": "Text",
        }

    def validate_config(self, source_config):
        source_config_obj = json.loads(source_config)
        if "app_token" not in source_config:
            raise ValueError("No application token provided in the 'app_token' field")
        return source_config

    def _dataset_link_info(self, ds_id, resource_name):
        """If no valid link directly to the resource can be found, create a link to the dataset page"""
        return {"url": f"{self.url}/dataset/{ds_id}",
                "name": resource_name,
                "format": "html"}

    def _resource_link_info(self, ds_id, resource_name, mimetype):
        """Get the URL, name, and resource type to display on dataset page. Where possible, this will
        include a direct link to the resource. If we can't find the resource using ID and file type,
        default to a link to the source dataset page."""
        if mimetype is None:
            format_id = "csv"
            file_url = f"{self.url}/resource/{ds_id}.csv"
        else:
            format_id = formats.get(mimetype.split(";")[0])
            file_url = f"{self.url}/download/{ds_id}/{mimetype}"
        if format_id is None:
            return self._dataset_link_info(ds_id, resource_name)
        file_ok = requests.get(file_url, headers={"X-App-Token": self.app_token}).ok
        if file_ok:
            return {"url": file_url,
                    "format": format_id,
                    "name": f"{resource_name}.{format_id}"}
        else:
            return self._dataset_link_info(ds_id, resource_name)


    def _create_catalog_entry(self, dataset):
        license_name = dataset["metadata"].get("license")
        license_id = licenses.get(license_name, license_name)
        ds_id = dataset["resource"]["id"]
        created_at = to_iso_date(dataset["resource"]["createdAt"])
        modified_at = to_iso_date(dataset["resource"]["updatedAt"])
        name = dataset["resource"]["name"]


        resources = [{"package_id": ds_id,
                      "created": created_at,
                      "last_modified": modified_at,
                      **self._resource_link_info(ds_id, name, dataset["resource"]["blob_mime_type"])}]
        pkg_dict =  {"name": name,
                     "package_id": ds_id,
                     "private": False,
                     "author": dataset["creator"]["display_name"],
                     "maintainer": dataset["resource"]["attribution"],
                     "maintainer_email": dataset["resource"]["contact_email"],
                     "org_name": dataset["resource"]["attribution"],
                     "org_link": dataset["resource"]["attribution_link"],
                     "license_id": license_id,
                     "license_title": license_name,
                     "notes": dataset["resource"]["description"],
                     "url": dataset["permalink"],
                     "state": "active",
                     "resources": resources}

        md5 = hashlib.md5()
        content_hash = md5.update(str(pkg_dict).encode())
        content_hash = md5.hexdigest()
        return {**pkg_dict, "content_hash": content_hash}

    def gather_stage(self, harvest_job):
        log.debug("In SODA harvester gather_stage (%s)", str(harvest_job))
        self._set_config(harvest_job.source)
        catalog_url = f"{self.url}/api/catalog/v1?domains={self.domain}"
        batch_size = 100
        start_index = 0
        total_num_results = float("inf")
        datasets = []
        while len(datasets) < total_num_results:
            response = requests.get(catalog_url,
                                    headers={"X-App-Token": self.app_token},
                                    params={"limit": batch_size, "offset": start_index})
            if not response.ok:
                self._save_gather_error(f"Source URL responded with {response.status_code}",
                                        harvest_job)
                return None

            data = response.json()
            if total_num_results == float("inf"):
                total_num_results = data["resultSetSize"]
            datasets.extend(data["results"])
            start_index += batch_size
            log.info(f"Fetched {len(datasets)} out of {total_num_results} datasets")

        log.info("Converting datasets into HarvestObjects")

        catalog_entries = [self._create_catalog_entry(d) for d in datasets]

        source_ds_ids = {d["package_id"] for d in catalog_entries}
        existing_ids = get_harvested_dataset_ids(harvest_job.source.id)
        deleted_ids = existing_ids - source_ds_ids
        log.info(f"""Fetched changes from source:
        {len(source_ds_ids - existing_ids)} to add
        {len(existing_ids.intersection(source_ds_ids))} to modify
        {len(deleted_ids)} to delete
        """)

        object_ids = []
        try:
            for d in catalog_entries:
                pkg_id = d["package_id"]
                d["action"] = "update" if pkg_id in existing_ids else "create"
                obj = HarvestObject(
                    guid=pkg_id, job=harvest_job, content=json.dumps(d)
                )
                obj.save()
                object_ids.append(obj.id)
        except Exception as e:
            error_msg = "Error gathering dataset updates: %r" % e.message
            log.exception(error_msg)
            self._save_gather_error(error_msg, harvest_job)
        try:
            for pk_id in deleted_ids:
                obj = HarvestObject(
                    guid=pk_id,
                    job=harvest_job,
                    content=json.dumps({"id": pk_id, "action": "delete"})
                )
                obj.save()
                log.info(obj)
                object_ids.append(obj.id)
            return object_ids
        except Exception as e:
            error_msg = "Error gathering datasets to delete: %r" % e.message
            log.exception(error_msg)
            self._save_gather_error(error_msg, harvest_job)


    def _dataset_to_pkgdict(self, dataset):
        modified = datetime.datetime.now().isoformat()
        return {**dataset,
                "id": dataset["package_id"],
                "title": dataset["name"],
                "upstream_metadata_created": modified,
                "upstream_metadata_modified": modified,}

    def modify_package_dict(self, package_dict, harvest_object):
        return package_dict

    def fetch_stage(self, harvest_object):
        return True

    def _delete_dataset(self, base_context, pkg_dict):
        pkg_id = pkg_dict["id"]
        log.info(f"Deleting dataset {pkg_id}")
        result = tk.get_action("dataset_purge")(
            base_context.copy(), pkg_dict
        )
        return True

    def import_stage(self, harvest_object):
        self._set_config(harvest_object.source)

        base_context = {
            "model": model,
            "session": model.Session,
            "user": self._get_user_name(),
        }

        imported_dataset = json.loads(harvest_object.content)


        match imported_dataset["action"]:
            case "delete":
                try:
                    ok = self._delete_dataset(base_context.copy(), imported_dataset)
                    log.info("Successfully deleted")
                    return ok
                except Exception as e:
                    error_msg = "Failed to delete dataset: %s" % e
                    log.exception(error_msg)

                    self._save_object_error(error_msg, harvest_object, "Import")
                    return False
            case "create":
                log.info(f"Dataset \"{imported_dataset['name']}\" does not currently exist. Importing...")
                try:
                    package_dict = self._dataset_to_pkgdict(imported_dataset)
                    # Assuming that organisations never change - if they do we need to do this for update also
                    if self.create_organisations and package_dict["org_name"] is not None:
                        owner_org = self.get_mapped_organization(base_context, harvest_object, package_dict.get("org_name"), self.create_organisations, package_dict, package_dict.get("org_link"))
                    else:
                        harvest_source = tk.get_action("package_show")(
                            base_context.copy(), {"id": harvest_object.source.id}
                        )
                        owner_org = self.get_mapped_organization(base_context, harvest_object, harvest_source['organization']['name'], self.create_organisations, package_dict, None)

                    package_dict["owner_org"] = owner_org

                    add_default_keys(package_dict)
                    add_default_extras(package_dict)

                    upsert_package_extra(
                        package_dict["extras"], "harvest_source_frequency", harvest_object.source.frequency
                    )

                    result = self._create_or_update_package(package_dict,
                                                            harvest_object,
                                                            package_dict_form="package_show")
                    return result
                except Exception as e:
                    error_msg = "Error creating new dataset: %s" % e
                    log.exception(error_msg)
                    self._save_object_error(error_msg, harvest_object, "Import")

            case "update":
                try:
                    existing_dataset = tk.get_action("package_show")(
                        base_context.copy(), {"id": imported_dataset["package_id"]}
                    )
                    existing_hash = get_package_extra_val(
                        existing_dataset["extras"], "content_hash"
                    )
                    if existing_hash == imported_dataset["content_hash"]:
                        log.info(f"Dataset \"{imported_dataset['name']}\" has not been changed. Skipping.")
                        return "unchanged"
                    else:
                        package_dict = {**existing_dataset, **self._dataset_to_pkgdict(imported_dataset)}
                        add_existing_extras(package_dict, base_context.copy())

                        upsert_package_extra(
                            package_dict["extras"], "content_hash", imported_dataset["content_hash"]
                        )
                        upsert_package_extra(
                            package_dict["extras"], "harvest_source_frequency", harvest_object.source.frequency
                        )

                        result = self._create_or_update_package(package_dict,
                                                                harvest_object,
                                                                package_dict_form="package_show")
                        return result
                except Exception as e:
                    error_msg = "Error modifying existing dataset: %s" % e
                    log.exception(error_msg)
                    self._save_object_error(error_msg, harvest_object, "Import")
