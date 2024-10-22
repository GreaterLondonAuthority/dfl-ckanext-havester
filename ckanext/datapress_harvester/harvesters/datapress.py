from __future__ import absolute_import

import logging
import os
import re
import urllib
from datetime import datetime

import requests
from ckan import model
from ckan.lib.helpers import json
from ckan.logic import NotFound, ValidationError, get_action, validators
from ckan.plugins import toolkit

from ckanext.datapress_harvester.util import (
    add_default_extras,
    add_existing_extras,
    get_harvested_dataset_ids,
)
from ckanext.harvest.harvesters import HarvesterBase
from .mixins import DFLHarvesterMixin
from ckanext.harvest.model import HarvestObject
log = logging.getLogger(__name__)

EXTRA_PKG_FIELDS = ['london_smallest_geography', 'update_frequency']
EXTRA_RESOURCE_FIELDS = ['temporal_coverage_from', 'temporal_coverage_to']

def normalise_ckan_resources(package_dict):
    normalised_resources = package_dict.get('resources',[])
    if not package_dict.get('resources') and package_dict.get('organization',{}).get('resources',[]):
        # The brent/barnet datapress instances return resources under
        # organization/resources not /resources like london data-press.
        #
        # So we harmonise them here
        normalised_resources = package_dict.get('organization',{}).get('resources',[])

    def fixup_id(res):
        input_id = res['id']
        # CKAN has a validation that ID's must have a minimum length,
        # but upstream sources have different rules, so pad ids with
        # 0's if they're shorter than 7 characters
        res['id'] = input_id.rjust(7,'0')
        
        return res

    normalised_resources = list(map(fixup_id, normalised_resources))
    package_dict['resources'] = normalised_resources
    
    

class DataPressHarvester(HarvesterBase, DFLHarvesterMixin):
    """
    A Harvester for DataPress instances.

    Based on the CKAN harvester:
    https://github.com/ckan/ckanext-harvest/blob/master/ckanext/harvest/harvesters/ckanharvester.py

    The DataPress API is *almost* compatible with the CKAN API, with some weird
    quirks, and some missing routes. DataPress's CKAN API is documented here:
    https://datapress.gitbook.io/datapress/ckan-requests
    """

    config = None

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
            if "api_version" in self.config:
                self.api_version = int(self.config["api_version"])

            log.debug("Using config: %r", self.config)
        else:
            self.config = {}

    def info(self):
        return {
            "name": "datapress",
            "title": "DataPress",
            "description": "Harvests remote DataPress instances",
            "form_config_interface": "Text",
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if "api_version" in config_obj:
                try:
                    int(config_obj["api_version"])
                except ValueError:
                    raise ValueError("api_version must be an integer")

            if "default_tags" in config_obj:
                if not isinstance(config_obj["default_tags"], list):
                    raise ValueError("default_tags must be a list")
                if config_obj["default_tags"] and not isinstance(
                    config_obj["default_tags"][0], dict
                ):
                    raise ValueError("default_tags must be a list of " "dictionaries")

            if "default_groups" in config_obj:
                if not isinstance(config_obj["default_groups"], list):
                    raise ValueError(
                        "default_groups must be a *list* of group" " names/ids"
                    )
                if config_obj["default_groups"] and not isinstance(
                    config_obj["default_groups"][0], str
                ):
                    raise ValueError(
                        "default_groups must be a list of group "
                        "names/ids (i.e. strings)"
                    )

                # Check if default groups exist
                context = {"model": model, "user": toolkit.c.user}
                config_obj["default_group_dicts"] = []
                for group_name_or_id in config_obj["default_groups"]:
                    try:
                        group = get_action("group_show")(
                            context, {"id": group_name_or_id}
                        )
                        # save the dict to the config object, as we'll need it
                        # in the import_stage of every dataset
                        config_obj["default_group_dicts"].append(group)
                    except NotFound:
                        raise ValueError("Default group not found")
                config = json.dumps(config_obj)

            if "default_extras" in config_obj:
                if not isinstance(config_obj["default_extras"], dict):
                    raise ValueError("default_extras must be a dictionary")

            if (
                "organizations_filter_include" in config_obj
                and "organizations_filter_exclude" in config_obj
            ):
                raise ValueError(
                    "Harvest configuration cannot contain both "
                    "organizations_filter_include and organizations_filter_exclude"
                )

            if (
                "groups_filter_include" in config_obj
                and "groups_filter_exclude" in config_obj
            ):
                raise ValueError(
                    "Harvest configuration cannot contain both "
                    "groups_filter_include and groups_filter_exclude"
                )

            if "user" in config_obj:
                # Check if user exists
                context = {"model": model, "user": toolkit.c.user}
                try:
                    get_action("user_show")(context, {"id": config_obj.get("user")})
                except NotFound:
                    raise ValueError("User not found")

            if "read_only" in config_obj:
                if not isinstance(config_obj["read_only"], bool):
                    raise ValueError("read_only must be boolean")

            if "datapress_api_key" in config_obj:
                if not isinstance(config_obj["datapress_api_key"], str):
                    raise ValueError("datapress_api_key must be string")

            if "harvest_private_datasets" in config_obj:
                if not isinstance(config_obj["harvest_private_datasets"], bool):
                    raise ValueError("harvest_private_datasets must be boolean")

        except ValueError as e:
            raise e

        return config

    def modify_package_dict(self, package_dict, harvest_object):
        """
        Allows custom harvesters to modify the package dict before
        creating or updating the actual package.
        """
        unprocessed_dataset_dict = json.loads(harvest_object.content)

        for field in EXTRA_PKG_FIELDS:
            if unprocessed_dataset_dict.get(field):
                package_dict["extras"] += [
                    {
                        "key": field,
                        "value": unprocessed_dataset_dict[field],
                    }
                ]

        # Update modified date so package is updated in database
        # (see _create_or_update_package() in harvester plugin)
        package_dict["metadata_modified"] = strip_time_zone(datetime.now().isoformat())

        return package_dict

    def request_jwt_token(self,remote_datapress_base_url):
        """
        Request a datapress JWT token from its undocumented /api/whoami route.
        Data press developers were consulted by Sven Latham, and it seems this is
        the recommended method.
        """
        url_route = f'{remote_datapress_base_url}/api/whoami'
        json_response = requests.get(url_route,headers={'Authorization': self.config['datapress_api_key'] }).json()
        jwt_token = json_response['readonly']['libraryJwt']
        # log.debug(f'JWT token: {jwt_token}')
        return jwt_token

    def gather_stage(self, harvest_job):
        log.debug("In DataPressHarvester gather_stage (%s)", harvest_job.source.url)
        toolkit.requires_ckan_version(min_version="2.0")

        self._set_config(harvest_job.source.config)

        # Get source URL
        remote_datapress_base_url = harvest_job.source.url.rstrip("/")

        # TODO we can't filter based on organizations_filter_include,
        # organizations_filter_exclude, groups_filter_include, and
        # groups_filter_exclude at fetch time since DataPress doesn't support
        # fq, so if we want this feature we'll have to filter the package list
        # after we've fetched it.

        # Ideally we would be able to request from the remote DataPress only
        # those datasets modified since the last completely successful harvest,
        # but we can't because the DataPress package_search endpoint doesn't
        # support the filter query parameter. The full blob of metadata isn't
        # too large so this turns out not to be a big deal.
        try:
            pkg_dicts = self._fetch_packages(remote_datapress_base_url)
        except ContentFetchError as e:
            log.exception("Fetching datasets gave an error")
            self._save_gather_error(
                "Unable to fetch datasets from DataPress:%s url:%s"
                % (e, remote_datapress_base_url),
                harvest_job,
            )
            return None

        if not pkg_dicts:
            self._save_gather_error(
                "No datasets found at DataPress: %s" % remote_datapress_base_url,
                harvest_job,
            )
            return []

        # Create a Set of dataset ids fetched from upstream,
        # for comparing with those that have been harvested previously and are already in the database
        fetched_ids = {p["id"] for p in pkg_dicts}

        # Get the Set of ids of datasets in the database that belong to this harvest source
        existing_dataset_ids = get_harvested_dataset_ids(harvest_job.source.id)

        # Datasets that are present locally but not upstream need to be deleted locally
        to_be_deleted = existing_dataset_ids - fetched_ids
        log.info(f"{len(to_be_deleted)} datasets need to be deleted")

        # Create harvest objects for each dataset
        try:
            package_ids = set()
            object_ids = []

            for pkg_dict in pkg_dicts:
                if pkg_dict["private"] and not self.config.get('harvest_private_datasets'):
                    log.info('Discarding private dataset %s %s', {pkg_dict["name"]}, {pkg_dict["id"]})
                    continue

                if pkg_dict["id"] in package_ids:
                    log.info(
                        "Discarding duplicate dataset %s - probably due "
                        "to datasets being changed at the same time as "
                        "when the harvester was paging through",
                        pkg_dict["id"],
                    )
                    continue
                package_ids.add(pkg_dict["id"])

                # Add a field signifying that this is a create/update to a dataset, rather than one that needs deleting.
                # Not currently used for anything.
                pkg_dict["action"] = "upsert"

                log.debug(
                    "Creating HarvestObject for %s %s", pkg_dict["name"], pkg_dict["id"]
                )
                obj = HarvestObject(
                    guid=pkg_dict["id"], job=harvest_job, content=json.dumps(pkg_dict)
                )
                obj.save()
                object_ids.append(obj.id)

            # Create jobs to purge the datasets that no longer exist upstream.
            # Needs to be 'purge' instead of 'delete' so that the dataset can be re-harvested
            # if it gets un-deleted upstream.
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
            log.exception("Exception during gather_stage")
            self._save_gather_error("%r" % e.message, harvest_job)

    def _fetch_datapress_extra_fields(self, remote_datapress_base_url, request_headers):
        """
        Get extra fields from DataPress API that aren't present in the datapress package list (see _fetch_packages())
        """
        url = f"{remote_datapress_base_url}/api/datasets/export.json"
        response = requests.get(url, headers=request_headers)
        response.raise_for_status()
        response_dict = response.json()

        lookup = {}

        def has_value(v):
            return v != None or v != ''

        for package_dict in response_dict:
            pkg_id = package_dict['id']
            pkg_extra_fields = {}
            for field in EXTRA_PKG_FIELDS:
                if field in package_dict and has_value(package_dict[field]):
                    pkg_extra_fields[field] = package_dict[field]
                    lookup[pkg_id] = pkg_extra_fields

            resources_extras = {}

            if isinstance(package_dict.get('resources'), list):
               # NOTE: the barnet/brent datapress instances on the
               # export API return a structure like:
               #
               # resources: [<res_obj>]
               #
               # Where as the data for london datapress returns:
               # resources: {<res_id>: <res_obj>}
               #
               # So we normalise these to the london datapress format.

               normalised_resources = []
               for res_obj in package_dict.get('resources',[]):
                   res_id = res_obj['id']
                   
                   normalised_resources.append({res_id: res_obj})
               package_dict['resources'] = normalised_resources

            
            if isinstance(package_dict.get('resources'), dict):
               for res_id, res_obj in package_dict.get('resources',[]).items():                   
                   resource_extra_fields = {}
                   for field in EXTRA_RESOURCE_FIELDS:
                       if field in res_obj and has_value(res_obj[field]):
                           resource_extra_fields[field] = res_obj[field]
                   if resource_extra_fields:
                       resources_extras[res_id] = resource_extra_fields
               package_dict['resources'] = resources_extras
               lookup[pkg_id] = package_dict
        
        return lookup

    def _fetch_packages(self, remote_datapress_base_url):
        """Fetch the current package list from DataPress"""

        if self.config.get('datapress_api_key'):
            # NOTE: Data press uses a non-standard 'Identity' HTTP
            # header that we need to use to pass the JWT
            # authentication token.
            #
            # If you don't pass this token in this header, datapress
            # will not reveal organisation/collaborator datasets that
            # the user with this API key should be able to see.
            request_headers = {'Identity': self.request_jwt_token(remote_datapress_base_url)}
        else:
            request_headers = {}

        # This route is datapress's CKAN compatibility API (it does not support all CKANs flags)
        # Datapress documentation for this route can be found here:
        #
        # https://datapress.gitbook.io/datapress/ckan-requests
        url = f"{remote_datapress_base_url}/api/action/current_package_list_with_resources"
        log.info("Fetching DataPress datasets: %s", url)
        data = requests.get(url, headers=request_headers).json()

        assert data["success"]

        results = data["result"]

        # Get extra fields from DataPress API that aren't present in the datapress package list
        self.extra_fields_lookup = self._fetch_datapress_extra_fields(
            remote_datapress_base_url, request_headers
        )

        for dataset_dict in results:
            extra_fields = self.extra_fields_lookup.get(dataset_dict["id"], {})
            extra_resource_fields = extra_fields.pop('resources',[])
            for resource_obj in dataset_dict.get('resources',[]):
                res_id = resource_obj['id']
                if res_id in extra_resource_fields:
                    resource_obj.update(extra_resource_fields[res_id])

            dataset_dict.update(extra_fields)
        
        return results

    def fetch_stage(self, harvest_object):
        # Nothing to do here - we got the package dict in the search in the
        # gather stage
        return True

    def _resource_format_from_url(self, url):
        try:
            p = urllib.parse.urlparse(url).path
            return os.path.splitext(p)[1][1:] or "data"
        except Exception as e:
            return "data"

    def _guess_image_format(self, url):
        try:
            # Get the response headers from the image url
            # (stream=True does not download the response body immediately)
            r = requests.get(url, stream=True)
            content_type = r.headers["Content-Type"]
            return content_type.split("/")[1]
        except Exception as e:
            return "image"

    def _datapress_to_ckan(self, package_dict, harvest_object):
        """
        Shims to transform DataPress packages into a format CKAN understands.

        Long term we might want some of these transformations to be errors
        instead, and seek changes to the upstream metadata.
        """
        # Remove Nones
        for key in list(package_dict):
            if package_dict[key] is None:
                del package_dict[key]

        def fixup_tag(tag):
            new_tag = re.sub("[^a-zA-Z0-9 \-_.]", "", tag)
            return {'name': new_tag}

        if package_dict.get('tags'):
            package_dict["tags"] = list(map(fixup_tag, package_dict["tags"]))

        # Some emails need cleaning up. (I think CKAN is actually too strict
        # here, and rejects valid emails. You're allowed some pretty weird
        # characters in an email address!)
        if "author_email" in package_dict:
            package_dict["author_email"] = urllib.parse.quote(
                package_dict["author_email"].strip(), safe="@"
            )
        if "maintainer_email" in package_dict:
            package_dict["maintainer_email"] = urllib.parse.quote(
                package_dict["maintainer_email"].strip(), safe="@"
            )

        if "organization" in package_dict:
            organization = package_dict["organization"]
            try:
                validators.name_validator(organization["name"], None)
            except validators.Invalid:
                log.info(
                    f"renaming organization from {organization['name']} to {organization['id']}"
                )
                organization["name"] = organization["id"]

        # CKAN expects these things to be empty strings rather than None
        default_keys = [
            "author",
            "author_email",
            "license_id",
            "license_title",
            "url",
            "version",
        ]
        for key in default_keys:
            if key not in package_dict:
                package_dict[key] = ""

        for resource in package_dict.get("resources",[]):
            # Remove Nones
            for key in list(resource):
                if resource[key] is None:
                    del resource[key]

            if "created" in resource:
                # Datapress exposes a datetime like YYYY-MM-DDTHH:MM:SS... but
                # we only want the date portion
                resource["created"] = resource["created"][:10]

            # these URLs are forbidden, so we need to reconstruct the
            # data.london.gov URLs
            # TODO this is specific to data.london.gov.uk, we need a way to make
            # tweaks like this on a per-datapress-instance basis.
            # Do all DataPress instances work similarly? Can we use the harvest
            # URL here?
            if resource["url"].startswith("https://airdrive-secure.s3-eu-west-1"):
                base = "https://data.london.gov.uk/download"
                dataset = package_dict["name"]
                id = resource["id"]
                file = urllib.parse.quote(resource["name"])
                format = resource["format"]
                resource["url"] = f"{base}/{dataset}/{id}/{file}.{format}"

            if "format" not in resource or not resource["format"]:
                resource["format"] = self._resource_format_from_url(resource["url"])

            if resource["format"] == "image":
                resource["format"] = self._guess_image_format(resource["url"])

        # Remove the timezone from the dates. CKAN doesn't store it internally and it
        # messes up date-based comparisons later if the timezone is kept (because the base
        # CKAN harvester compares the string representation of the dates, not e.g. a datetime
        # object.
        # I.e. 2023-06-27T10:45:57.284Z comes after 2023-06-27T10:45:57.284000 alphanumerically,
        # even though it's the same datetime)
        package_dict["metadata_modified"] = strip_time_zone(
            package_dict["metadata_modified"]
        )
        package_dict["metadata_created"] = strip_time_zone(
            package_dict["metadata_created"]
        )

        # We remove the "state" key so that the current state (ie active/deleted) is
        # used instead of the state in the source. This is to prevent deleted datasets
        # being marked as active.
        del package_dict["state"]
        return package_dict

    def import_stage(self, harvest_object):
        log.debug("In DataPressHarvester import_stage")

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

        self._set_config(harvest_object.job.source.config)

        try:
            package_dict = json.loads(harvest_object.content)

            # Delete the dataset if its "action" is "delete"
            if package_dict["action"] == "delete":
                log.info(f"Deleting dataset with ID: {package_dict['id']}")
                result = toolkit.get_action("dataset_purge")(
                    base_context.copy(), package_dict
                )
                return True

            package_dict = self._datapress_to_ckan(package_dict, harvest_object)

            if package_dict.get("type") == "harvest":
                log.warn("Remote dataset is a harvest source, ignoring...")
                return True

            # Set default tags if needed
            default_tags = self.config.get("default_tags", [])
            if default_tags:
                if "tags" not in package_dict:
                    package_dict["tags"] = []
                package_dict["tags"].extend(
                    [t for t in default_tags if t not in package_dict["tags"]]
                )

            remote_groups = self.config.get("remote_groups", None)
            if remote_groups not in ("only_local", "create"):
                # Ignore remote groups
                package_dict.pop("groups", None)
            else:
                if "groups" not in package_dict:
                    package_dict["groups"] = []

                # check if remote groups exist locally, otherwise remove
                validated_groups = []

                for group_ in package_dict["groups"]:
                    try:
                        try:
                            if "id" in group_:
                                data_dict = {"id": group_["id"]}
                                group = get_action("group_show")(
                                    base_context.copy(), data_dict
                                )
                            else:
                                raise NotFound

                        except NotFound:
                            if "name" in group_:
                                data_dict = {"id": group_["name"]}
                                group = get_action("group_show")(
                                    base_context.copy(), data_dict
                                )
                            else:
                                raise NotFound
                        # Found local group
                        validated_groups.append(
                            {"id": group["id"], "name": group["name"]}
                        )

                    except NotFound:
                        log.info("Group %s is not available", group_)
                        if remote_groups == "create":
                            try:
                                group = self._get_group(
                                    harvest_object.source.url, group_
                                )
                            except RemoteResourceError:
                                log.error("Could not get remote group %s", group_)
                                continue

                            for key in [
                                "packages",
                                "created",
                                "users",
                                "groups",
                                "tags",
                                "extras",
                                "display_name",
                            ]:
                                group.pop(key, None)

                            get_action("group_create")(base_context.copy(), group)
                            log.info("Group %s has been newly created", group_)
                            validated_groups.append(
                                {"id": group["id"], "name": group["name"]}
                            )

                package_dict["groups"] = validated_groups
            
            # Local harvest source organization
            harvest_source = get_action("package_show")(
                base_context.copy(), {"id": harvest_object.source.id}
            )

            harvester_org = harvest_source.get("owner_org")

            remote_orgs = self.config.get("remote_orgs", None)                       
            validated_org = None

            if remote_orgs not in ("only_local", "create"):
                # Assign dataset to the source organization
                validated_org = self.get_mapped_organization(base_context, harvest_object, harvester_org, remote_orgs, package_dict, None)
                package_dict["owner_org"] = validated_org
            else:
                if "owner_org" not in package_dict:
                    package_dict["owner_org"] = None

                # check if remote org exist locally, otherwise remove
                remote_org = package_dict.get("owner_org")
                
                if remote_org:
                    validated_org = self.get_mapped_organization(base_context, harvest_object, remote_org, remote_orgs, package_dict, None)

                package_dict["owner_org"] = validated_org or harvester_org

            # Set default groups if needed
            default_groups = self.config.get("default_groups", [])
            if default_groups:
                if "groups" not in package_dict:
                    package_dict["groups"] = []
                existing_group_ids = [g["id"] for g in package_dict["groups"]]
                package_dict["groups"].extend(
                    [
                        g
                        for g in self.config["default_group_dicts"]
                        if g["id"] not in existing_group_ids
                    ]
                )

            if "extras" not in package_dict:
                package_dict["extras"] = []

            default_extras = {}
            default_extras.update(self.config.get("default_extras", {}))

            def get_extra(key, package_dict):
                for extra in package_dict.get("extras", []):
                    if extra["key"] == key:
                        return extra

            if default_extras:
                override_extras = self.config.get("override_extras", False)
                for key, value in default_extras.items():
                    existing_extra = get_extra(key, package_dict)
                    if existing_extra and not override_extras:
                        continue  # no need for the default
                    if existing_extra:
                        package_dict["extras"].remove(existing_extra)
                    # Look for replacement strings
                    if isinstance(value, str):
                        value = value.format(
                            harvest_source_id=harvest_object.job.source.id,
                            harvest_source_url=harvest_object.job.source.url.strip("/"),
                            harvest_source_title=harvest_object.job.source.title,
                            harvest_source_frequency=harvest_object.job.source.frequency,
                            harvest_job_id=harvest_object.job.id,
                            harvest_object_id=harvest_object.id,
                            dataset_id=package_dict["id"],
                        )

                    package_dict["extras"].append({"key": key, "value": value})

            # Add any existing extras here so they override any default extras
            # specified in the harvest source. E.g. if data_quality is set as a default_extra
            # we want to override that with whatever the current value is.
            add_existing_extras(package_dict, base_context.copy())

            # Add some default extras
            add_default_extras(package_dict)

            package_dict["extras"] += [
                {
                    "key": "upstream_url",
                    "value": "{}/dataset/{}".format(
                        harvest_object.job.source.url.rstrip("/"),
                        package_dict["id"],
                    ),
                },
                {
                    "key": "upstream_metadata_modified",
                    "value": package_dict["metadata_modified"],
                },
                {
                    "key": "upstream_metadata_created",
                    "value": package_dict["metadata_created"],
                },
            ]

            normalise_ckan_resources(package_dict)
            
            for resource in package_dict.get("resources", []):
                
                # Clear remote url_type for resources (eg datastore, upload) as
                # we are only creating normal resources with links to the
                # remote ones
                resource.pop("url_type", None)

                # Clear revision_id as the revision won't exist on this CKAN
                # and saving it will cause an IntegrityError with the foreign
                # key.
                resource.pop("revision_id", None)

                # if "extras" not in resource:
                #     resource["extras"] = []

            package_dict_form = self.modify_package_dict(package_dict, harvest_object)
            result = self._create_or_update_package(
                package_dict_form, harvest_object, package_dict_form="package_show"
            )

            return result
        except ValidationError as e:
            log.exception("ValidationError during Import")

            self._save_object_error(
                "Invalid package with GUID %s: %r"
                % (harvest_object.guid, e.error_dict),
                harvest_object,
                "Import",
            )
        except Exception as e:
            log.exception("Exception during Import")
            self._save_object_error("%s" % e, harvest_object, "Import")


class ContentFetchError(Exception):
    pass


class ContentNotFoundError(ContentFetchError):
    pass


class RemoteResourceError(Exception):
    pass


# This makes me uncomfortable, but CKAN doesn't accept time zone specifiers so
# we have to strip them. If we ever need to harvest a source in another time
# zone we'll have to update CKAN to handle them.
def strip_time_zone(iso_timestamp):
    return re.sub(r"Z|([+-]\d\d:?(\d\d)?)$", "", iso_timestamp)
