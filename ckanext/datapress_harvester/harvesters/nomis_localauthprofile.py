import logging
import requests
import hashlib
import datetime

from bs4 import BeautifulSoup

from ckan import model
from ckan.lib.helpers import json
import ckan.plugins.toolkit as tk

from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject

from ckanext.datapress_harvester.util import (
    NOMIS_BOROUGHS,
    NOMIS_LAP_SELECT_URL,
    NOMIS_LMP_BASE,
    sanitise,
)

log = logging.getLogger(__name__)


def _generate_resource(dataset, url_key, name):
    """Generate a resource dict for use in a package_dict"""
    resource_id = f'{dataset["resource_id"]}_{url_key}'
    modified = datetime.datetime.now().isoformat()
    return {
        "id": resource_id,
        "package_id": dataset["package_id"],
        "url": dataset[url_key],
        "name": name,
        "metadata_modified": modified,
        "last_modified": modified,
    }


def _dataset_to_pkgdict(dataset):
    """Convert a scraped dataset to a CKAN package_dict"""
    modified = datetime.datetime.now().isoformat()
    return {
        "id": dataset["package_id"],
        "name": dataset["name"],
        "title": dataset["name"],
        "notes": dataset["description"],
        "license_id": dataset["license_id"],
        "resources": [
            _generate_resource(dataset, "sectionlink", "nomis data tables"),
            _generate_resource(dataset, "querylink", "query the nomis data"),
        ],
        "metadata_modified": modified,
        "upstream_metadata_created": modified,
        "upstream_metadata_modified": modified,
    }


# Helper functions for getting and setting values in package["extras"].
# package["extras"] is a list of dictionaries of the form:
# [ {"key": <key>, "value": <value>}, {"key": <key>, "value": <value>}, ...]
def _get_package_extra_val(extras, key):
    """
    Return a value from package extras, given the key.
    Return None if the key does not exist.
    """
    for extra in extras:
        if extra["key"] == key:
            return extra["value"]
    return None


def _upsert_package_extra(extras, key, val):
    """
    Update the value for <key> in package['extras'] if it exists.
    Insert it if not.
    Returns the updated package['extras'].
    """
    for extra in extras:
        if extra["key"] == key:
            extra["value"] = val
            return extras

    extras.append({"key": key, "value": val})
    return extras


class NomisLocalAuthorityProfileScraper(HarvesterBase):
    def info(self):
        return {
            "name": "nomis-localauthprofile",
            "title": "Nomis Local Authority Profile",
            "description": "Harvests local authority profiles from nomis",
            "form_config_interface": "Text",
        }

    def validate_config(self, source_config):
        if not source_config:
            return source_config

        # "boroughs" is the only valid config option
        # Check that it is a list with at least one element,
        # and check that each element is one of the NOMIS_BOROUGHS
        try:
            source_config_obj = json.loads(source_config)
            if "boroughs" in source_config_obj:
                if not isinstance(source_config_obj["boroughs"], list):
                    raise ValueError("boroughs must be a list")

                if len(source_config_obj["boroughs"]) == 0:
                    raise ValueError("At least one borough must be specified")

                invalid_boroughs = list(
                    filter(
                        lambda b: b not in NOMIS_BOROUGHS,
                        source_config_obj["boroughs"],
                    )
                )
                if len(invalid_boroughs) > 0:
                    raise ValueError(
                        f"The following boroughs were not recognised: {', '.join(invalid_boroughs)}"
                    )
        except ValueError as e:
            raise

        return source_config

    config = {}

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
            if "api_version" in self.config:
                self.api_version = int(self.config["api_version"])

            log.debug("Using config: %r", self.config)

    def _get_borough_ids(self, required_boroughs, harvest_job):
        """
        Returns a dictionary of the form {"borough_name": nomis_borough_id}
        For each of the boroughs in the Select box on the nomis local authority profile page which matches
        one of the required_boroughs
        """
        try:
            page = BeautifulSoup(requests.get(NOMIS_LAP_SELECT_URL).text)
        except requests.exceptions.ConnectionError as e:
            self._save_gather_error(
                f"Connection error when getting borough IDs: {NOMIS_LAP_SELECT_URL}",
                harvest_job,
            )
            return {}
        try:
            nomis_local_authorities = page.find("select").findChildren("option")
        except AttributeError as e:
            nomis_local_authorities = []

        if len(nomis_local_authorities) == 0:
            msg = f"Did not find borough select box on page: {NOMIS_LAP_SELECT_URL}"
            self._save_gather_error(msg, harvest_job)
            return {}

        return {
            option.text: option["value"]
            for option in nomis_local_authorities
            if option.text in required_boroughs
        }

    def _extract_topics(self, page, url, harvest_job):
        """
        Returns a list of dictionaries of the form [{"name": <name>, "location": <location>}, {...}]
        for each topic in the summary box on a local authority profile page
        """
        try:
            topic_links = (
                page.find(class_="summary-stat-overview-section-wrapper")
                .find("ul", class_="links-list")
                .findAll("li")
            )
        except AttributeError as e:
            topic_links = []

        if len(topic_links) == 0:
            self._save_gather_error(f"Failed to extract topics for: {url}", harvest_job)
            return []

        topics = [
            {"name": link.text, "location": link.find("a")["href"]}
            for link in topic_links
        ]
        return topics

    def _extract_dataset(self, page, borough_name, borough_id, topic, harvest_job):
        """
        Returns a dataset dictionary corresponding to one of the topics
        on a nomis local authority profile page.
        """
        # Find the <a> tag that signifys the start of a 'topic'
        topic_start = page.find("a", {"name": topic["location"].replace("#", "")})
        if topic_start is None:
            self._save_gather_error(
                f"Did not find topic: {topic['name']} start for {borough_name}",
                harvest_job,
            )
            return None
        # The querylink is the next <a> tag with target:nomisquery after the start of the topic
        querylink = topic_start.find_next("a", {"target": "nomisquery"})
        if querylink is None:
            self._save_gather_error(
                f"Did not find querylink for {borough_name}", harvest_job
            )
            return None

        # Most topics have more than one data table, but for now we're only interested in the first one.
        # Get the contents of the table and hash it to store in the db, to compare when the harvester is run next
        try:
            table_content = topic_start.find_next("tbody").text.strip()
        except AttributeError as e:
            self._save_gather_error(
                f"Did not find data table for {borough_name}: {topic['name']}",
                harvest_job,
            )
            return None

        md5 = hashlib.md5()
        content_hash = md5.update(table_content.encode())
        content_hash = md5.hexdigest()

        name = f"{borough_name} {topic['name']}"
        package_id = f"nomis_{sanitise(name)}"
        resource_id = f"{package_id}_{sanitise(topic['location'])}"
        return {
            "package_id": package_id,
            "resource_id": resource_id,
            "name": name,
            "description": f"Data about {topic['name'].lower()} in {borough_name}, provided by nomis",
            "sectionlink": NOMIS_LMP_BASE.format(nomis_code=borough_id)
            + topic["location"],
            "querylink": "https://www.nomisweb.co.uk" + querylink["href"],
            "license_id": "uk-ogl",
            "content_hash": content_hash,
        }

    def gather_stage(self, harvest_job):
        self._set_config(harvest_job.source.config)
        log.info("Getting borough ids")

        required_boroughs = self.config.get("boroughs", NOMIS_BOROUGHS)
        scraped_boroughs = self._get_borough_ids(required_boroughs, harvest_job)
        if len(scraped_boroughs) != len(required_boroughs):
            self._save_gather_error(
                "Did not get IDs for all required boroughs", harvest_job
            )
            return None

        datasets = []
        for name, code in scraped_boroughs.items():
            log.info(f"Extracting datasets for {name}")
            borough_url = NOMIS_LMP_BASE.format(nomis_code=code)
            try:
                borough_page = BeautifulSoup(requests.get(borough_url).content)
            except requests.exceptions.ConnectionError as e:
                self._save_gather_error(
                    f"Connection error when getting page for {name}", harvest_job
                )
                continue
            topics = self._extract_topics(borough_page, borough_url, harvest_job)
            datasets += [
                self._extract_dataset(borough_page, name, code, t, harvest_job)
                for t in topics
            ]

        if None in datasets:
            self._save_gather_error(
                "Some datasets were not extracted correctly. Stopping."
            )
            return []

        log.info(f"Extracted {len(datasets)} datasets in total")

        # Create harvest objects for each dataset
        log.info(f"Converting datasets into HarvestObjects")
        object_ids = []
        try:
            for d in datasets:
                obj = HarvestObject(
                    guid=d["package_id"], job=harvest_job, content=json.dumps(d)
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

        scraped_dataset = json.loads(harvest_object.content)

        # Check whether a dataset already exists in CKAN.
        # If so, check the content hashes to see if the data has been updated upstream
        existing_dataset = {}
        try:
            existing_dataset = tk.get_action("package_show")(
                base_context.copy(), {"id": scraped_dataset["package_id"]}
            )
            existing_hash = _get_package_extra_val(
                existing_dataset["extras"], "content_hash"
            )
            if existing_hash == scraped_dataset["content_hash"]:
                log.info(
                    f"Dataset \"{scraped_dataset['name']}\" has not been changed. Skipping."
                )
                return "unchanged"
        # If not, a new dataset needs to be created.
        except tk.ObjectNotFound as e:
            log.info(
                f"Dataset \"{scraped_dataset['name']}\" does not currently exist. Importing..."
            )

        try:
            # Merge our scraped package dict into any existing package dict
            # The keys in the dict returned by _dataset_to_pkgdict will override those in existing_dataset
            package_dict = {**existing_dataset, **_dataset_to_pkgdict(scraped_dataset)}

            # Set the owner org of the new dataset to the org set in the harvest source
            harvest_source = tk.get_action("package_show")(
                base_context.copy(), {"id": harvest_object.source.id}
            )
            package_dict["owner_org"] = harvest_source.get("owner_org")

            # Set some default keys so CKAN does not report them as being changed later.
            default_keys = [
                "author",
                "author_email",
                "url",
                "version",
            ]
            for key in default_keys:
                if key not in package_dict:
                    package_dict[key] = ""

            if "extras" not in package_dict:
                package_dict["extras"] = []

            # Add an empty data quality field to extras if it's not already there
            if _get_package_extra_val(package_dict["extras"], "data_quality") is None:
                package_dict["extras"].append({"key": "data_quality", "value": ""})

            # Add the content hash or update the value if one existed
            _upsert_package_extra(
                package_dict["extras"], "content_hash", scraped_dataset["content_hash"]
            )

            result = self._create_or_update_package(
                package_dict, harvest_object, package_dict_form="package_show"
            )

            return result
        except Exception as e:
            self._save_object_error("%s" % e, harvest_object, "Import")
