import logging
import re
import requests
import hashlib
import datetime

from bs4 import BeautifulSoup

from ckan import model
from ckan.lib.helpers import json
import ckan.plugins.toolkit as tk

from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra

import ckanext.datapress_harvester.lib as lib


log = logging.getLogger(__name__)
md5 = hashlib.md5()


def _get_borough_ids(required_boroughs=None):
    """
    Returns a dictionary of the form {"borough_name": nomis_borough_id}
    For each of the boroughs in the Select box on the nomis local authority profile page which match
    one of the BOROUGHS
    """
    boroughs = required_boroughs or lib.NOMIS_BOROUGHS
    page = BeautifulSoup(requests.get(lib.NOMIS_LAP_SELECT_URL).text)
    # TODO Try-catch here for if "select" etc. is not found
    nomis_local_authorities = page.find("select").findChildren("option")

    # TODO : Set this back to all boroughs
    return {
        option.text: option["value"]
        for option in nomis_local_authorities
        if option.text in boroughs[0]
    }


def _extract_topics(page):
    """
    Returns a list of dictionaries of the form [{"name": <name>, "location": <location>}, {...}]
    for each topic in the summary box on a local authority profile page
    """
    topic_links = (
        page.find(class_="summary-stat-overview-section-wrapper")
        .find("ul", class_="links-list")
        .findAll("li")
    )
    # TODO : Set this back to all topics
    topics = [
        {"name": link.text, "location": link.find("a")["href"]}
        for link in topic_links[:1]
    ]
    return topics


def _sanitise(s):
    """
    Returns a sanitised version of string s:
     - Removes all non-alphanumeric character (except space, underscore and dash)
     - Replaces duplicate spaces with one space
     - Replaces remaining spaces with dashes
     - Lower-cases everything
    """
    without_non_alpha = re.sub("[^0-9a-zA-Z _-]+", "", s)
    no_duplicate_spaces = re.sub("\s{2,}", " ", without_non_alpha)
    no_spaces = no_duplicate_spaces.replace(" ", "-")
    return no_spaces.lower()


def _extract_dataset(page, borough_name, borough_id, topic):
    """
    Returns a dataset dictionary corresponding to one of the topics
    on a nomis local authority profile page.
    This dictionary should have everything required to create a CKAN package
    """
    # Find the <a> tag that signifys the start of a 'topic'
    topic_start = page.find("a", {"name": topic["location"].replace("#", "")})
    # The querylink is the next <a> tag with target:nomisquery after the start of the topic
    querylink = topic_start.find_next("a", {"target": "nomisquery"})

    # Most topics have more than one data table, but for now we're only interested in the first one.
    # Get the contents of the table and hash it to store in the db, to compare when the harvester is run next
    table_content = topic_start.find_next("tbody").text
    content_hash = md5.update(table_content.encode())
    content_hash = md5.hexdigest() + "test"

    name = f"{borough_name} {topic['name']}"
    package_id = f"nomis_{_sanitise(name)}"
    resource_id = f"{package_id}_{_sanitise(topic['location'])}"
    return {
        "package_id": package_id,
        "resource_id": resource_id,
        "name": name,
        "description": f"Data about {topic['name'].lower()} in {borough_name}, provided by nomis",
        "sectionlink": lib.NOMIS_LMP_BASE.format(nomis_code=borough_id)
        + topic["location"]
        + "test",
        "querylink": "https://www.nomisweb.co.uk" + querylink["href"],
        "license_id": "uk-ogl",
        "content_hash": content_hash,
    }


def _generate_resource(dataset, url_key, name):
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
    return {
        "id": dataset["package_id"],
        "name": dataset["name"],
        "title": dataset["name"],
        "notes": dataset["description"],
        "license_id": dataset["license_id"],
        "resources": [
            _generate_resource(dataset, "sectionlink", "nomis data tables"),
            # _generate_resource(dataset, "querylink", "query the nomis data"),
        ],
        "metadata_modified": datetime.datetime.now().isoformat(),
    }


def _get_package_extra(extras, key):
    """
    Helper function for retrieving the value from package extras,
    given the key
    """
    for extra in extras:
        if extra["key"] == key:
            return extra["value"]
    return None


def _upsert_package_extra(extras, key, val):
    for extra in extras:
        if extra["key"] == key:
            extra["value"] = val
            return val
    extras.append({"key": key, "value": val})


class NomisLocalAuthorityProfileScraper(HarvesterBase):
    def info(self):
        return {
            "name": "nomis-localauthprofile",
            "title": "Nomis Local Authority Profile",
            "description": "Harvests local authority profiles from nomis",
            "form_config_interface": "Text",
        }

    config = {}

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
            if "api_version" in self.config:
                self.api_version = int(self.config["api_version"])

            log.debug("Using config: %r", self.config)

    def gather_stage(self, harvest_job):
        self._set_config(harvest_job.source.config)
        log.info("Getting borough ids")
        boroughs = _get_borough_ids(self.config.get("boroughs", None))

        # TODO Add try-catch for if the page layout changes. Report error
        datasets = []
        for name, code in boroughs.items():
            log.info(f"Extracting datasets for {name}")
            borough_url = lib.NOMIS_LMP_BASE.format(nomis_code=code)
            borough_page = BeautifulSoup(requests.get(borough_url).text)
            topics = _extract_topics(borough_page)
            datasets += [_extract_dataset(borough_page, name, code, t) for t in topics]

        log.info(f"Extracted {len(datasets)} datasets in total")

        # Create harvest objects for each dataset
        log.info(f"Converting datasets into HarvestObjects")
        object_ids = []
        try:
            for d in datasets:
                log.debug(
                    "Creating HarvestObject for %s %s", d["name"], d["package_id"]
                )
                obj = HarvestObject(
                    guid=d["package_id"], job=harvest_job, content=json.dumps(d)
                )
                obj.save()
                object_ids.append(obj.id)
            return object_ids
        except Exception as e:
            self._save_gather_error("%r" % e.message, harvest_job)

    def fetch_stage(self, harvest_object):
        log.info("Skipping fetch stage")
        return True

    def import_stage(self, harvest_object):
        base_context = {
            "model": model,
            "session": model.Session,
            "user": self._get_user_name(),
        }

        harvest_source = tk.get_action("package_show")(
            base_context.copy(), {"id": harvest_object.source.id}
        )

        scraped_dataset = json.loads(harvest_object.content)

        existing_dataset = {}
        try:
            existing_dataset = tk.get_action("package_show")(
                base_context.copy(), {"id": scraped_dataset["package_id"]}
            )
            log.info("existing dataset:")
            log.info(existing_dataset)
            existing_hash = _get_package_extra(
                existing_dataset["extras"], "content_hash"
            )
            if existing_hash == scraped_dataset["content_hash"]:
                log.info(
                    f"Dataset \"{scraped_dataset['name']}\" has not been changed. Skipping."
                )
                return "unchanged"
        except tk.ObjectNotFound as e:
            log.info(
                f"Dataset \"{scraped_dataset['name']}\" does not currently exist. Importing..."
            )

        try:
            # Merge our package dict into any existing package dict
            package_dict = {**existing_dataset, **_dataset_to_pkgdict(scraped_dataset)}

            owner_org = harvest_source.get("owner_org")
            package_dict["owner_org"] = owner_org
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

            # Add an empty data qualuty field to extras if it's not already there
            if _get_package_extra(package_dict["extras"], "data_quality") is None:
                package_dict["extras"].append({"key": "data_quality", "value": ""})

            # Add the content hash or update the value if one existed
            _upsert_package_extra(
                package_dict["extras"], "content_hash", scraped_dataset["content_hash"]
            )

            result = self._create_or_update_package(
                package_dict, harvest_object, package_dict_form="package_show"
            )

            imported_package = tk.get_action("package_show")(
                base_context.copy(), {"id": package_dict["id"]}
            )
            log.info("imported dataset")
            log.info(imported_package)
            return result
        except Exception as e:
            self._save_object_error("%s" % e, harvest_object, "Import")
