from __future__ import absolute_import
import requests
from requests.exceptions import HTTPError, RequestException

import re
import urllib

from ckan import model
from ckan.logic import ValidationError, NotFound, get_action, validators
from ckan.lib.helpers import json
from ckan.plugins import toolkit

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters import HarvesterBase

import logging

log = logging.getLogger(__name__)


class DataPressHarvester(HarvesterBase):
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

        except ValueError as e:
            raise e

        return config

    def modify_package_dict(self, package_dict, harvest_object):
        """
        Allows custom harvesters to modify the package dict before
        creating or updating the actual package.
        """
        return package_dict

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
            log.info("Fetching datasets gave an error: %s", e)
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

        # Create harvest objects for each dataset
        try:
            package_ids = set()
            object_ids = []
            for pkg_dict in pkg_dicts:
                if pkg_dict["id"] in package_ids:
                    log.info(
                        "Discarding duplicate dataset %s - probably due "
                        "to datasets being changed at the same time as "
                        "when the harvester was paging through",
                        pkg_dict["id"],
                    )
                    continue
                package_ids.add(pkg_dict["id"])

                log.debug(
                    "Creating HarvestObject for %s %s", pkg_dict["name"], pkg_dict["id"]
                )
                obj = HarvestObject(
                    guid=pkg_dict["id"], job=harvest_job, content=json.dumps(pkg_dict)
                )
                obj.save()
                object_ids.append(obj.id)

            return object_ids
        except Exception as e:
            self._save_gather_error("%r" % e.message, harvest_job)

    def _fetch_packages(self, remote_datapress_base_url):
        """Fetch the current package list from DataPress"""
        url = f"{remote_datapress_base_url}/api/action/current_package_list_with_resources"
        log.debug("Fetching DataPress datasets: %s", url)
        data = requests.get(url).json()
        assert data["success"]
        return data["result"]

    def fetch_stage(self, harvest_object):
        # Nothing to do here - we got the package dict in the search in the
        # gather stage
        return True

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

        # Tags must be alphanumeric
        for i, tag in enumerate(package_dict["tags"]):
            package_dict["tags"][i]["name"] = re.sub(
                "[^a-zA-Z0-9 \-_.]", "", tag["name"]
            )

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

        for resource in package_dict["resources"]:
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
            source_dataset = get_action("package_show")(
                base_context.copy(), {"id": harvest_object.source.id}
            )
            local_org = source_dataset.get("owner_org")

            remote_orgs = self.config.get("remote_orgs", None)

            if remote_orgs not in ("only_local", "create"):
                # Assign dataset to the source organization
                package_dict["owner_org"] = local_org
            else:
                if "owner_org" not in package_dict:
                    package_dict["owner_org"] = None

                # check if remote org exist locally, otherwise remove
                validated_org = None
                remote_org = package_dict["owner_org"]

                if remote_org:
                    try:
                        data_dict = {"id": remote_org}
                        org = get_action("organization_show")(
                            base_context.copy(), data_dict
                        )
                        validated_org = org["id"]
                    except NotFound:
                        log.info("Organization %s is not available", remote_org)
                        if remote_orgs == "create" and "organization" in package_dict:
                            org = package_dict["organization"]
                            for key in [
                                "packages",
                                "created",
                                "users",
                                "groups",
                                "tags",
                                "extras",
                                "display_name",
                                "type",
                            ]:
                                org.pop(key, None)
                            get_action("organization_create")(base_context.copy(), org)
                            log.info(
                                "Organization %s has been newly created", remote_org
                            )
                            validated_org = org["id"]

                package_dict["owner_org"] = validated_org or local_org

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

            # Set default extras if needed
            default_extras = {"data_quality": ""}
            default_extras.update(self.config.get("default_extras", {}))

            def get_extra(key, package_dict):
                for extra in package_dict.get("extras", []):
                    if extra["key"] == key:
                        return extra

            if default_extras:
                override_extras = self.config.get("override_extras", False)
                if "extras" not in package_dict:
                    package_dict["extras"] = []
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
                            harvest_job_id=harvest_object.job.id,
                            harvest_object_id=harvest_object.id,
                            dataset_id=package_dict["id"],
                        )

                    package_dict["extras"].append({"key": key, "value": value})

            if "extras" not in package_dict:
                package_dict["extras"] = []
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
                    "value": strip_time_zone(package_dict["metadata_modified"]),
                },
                {
                    "key": "upstream_metadata_created",
                    "value": strip_time_zone(package_dict["metadata_created"]),
                },
            ]

            for resource in package_dict.get("resources", []):
                # Clear remote url_type for resources (eg datastore, upload) as
                # we are only creating normal resources with links to the
                # remote ones
                resource.pop("url_type", None)

                # Clear revision_id as the revision won't exist on this CKAN
                # and saving it will cause an IntegrityError with the foreign
                # key.
                resource.pop("revision_id", None)

            package_dict = self.modify_package_dict(package_dict, harvest_object)

            result = self._create_or_update_package(
                package_dict, harvest_object, package_dict_form="package_show"
            )

            return result
        except ValidationError as e:
            self._save_object_error(
                "Invalid package with GUID %s: %r"
                % (harvest_object.guid, e.error_dict),
                harvest_object,
                "Import",
            )
        except Exception as e:
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
