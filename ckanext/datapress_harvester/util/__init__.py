import re

from ckan import model
from ckan.plugins import toolkit

NOMIS_LAP_SELECT_URL = "https://www.nomisweb.co.uk/reports/lmp/la/contents.aspx"
NOMIS_LMP_BASE = "https://www.nomisweb.co.uk/reports/lmp/la/{nomis_code}/report.aspx"

NOMIS_BOROUGHS = [
    "Barking and Dagenham",
    "Barnet",
    "Bexley",
    "Brent",
    "Bromley",
    "Camden",
    "City of London",
    "Croydon",
    "Ealing",
    "Enfield",
    "Haringey",
    "Harrow",
    "Havering",
    "Hillingdon",
    "Hounslow",
    "Greenwich",
    "Hackney",
    "Hammersmith and Fulham",
    "Islington",
    "Kensington and Chelsea",
    "Kingston-upon-Thames",
    "Lambeth",
    "Lewisham",
    "Merton",
    "Newham",
    "Redbridge",
    "Richmond upon Thames",
    "Southwark",
    "Sutton",
    "Tower Hamlets",
    "Waltham Forest",
    "Wandsworth",
    "Westminster",
]


def sanitise(s):
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


# Helper functions for getting and setting values in package["extras"].
# package["extras"] is a list of dictionaries of the form:
# [ {"key": <key>, "value": <value>}, {"key": <key>, "value": <value>}, ...]
def remove_extras(extras, keys):
    """
    Return a new dictionary of extras with the specified keys removed.
    """
    new_extras = []
    for e in extras:
        if e["key"] not in keys:
            new_extras.append({"key": e["key"], "value": e["value"]})
    return new_extras


def get_package_extra_val(extras, key):
    """
    Return a value from package extras, given the key.
    Return None if the key does not exist.
    """
    for extra in extras:
        if extra["key"] == key:
            return extra["value"]
    return None


def upsert_package_extra(extras, key, val):
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


def harvester_search_dict(source_id, page, limit):
    return {
        "fq": '+harvest_source_id:"{0}"'.format(source_id),
        "fl": "id",
        "rows": limit,
        "start": (page - 1) * limit,
    }


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


def add_existing_extras(pkg_dict, context):
    try:
        # Check whether a package already exists that we need to transfer the extras from:
        existing_package = toolkit.get_action("package_show")(
            context,
            {"id": pkg_dict["id"], "use_default_schema": True},
        )

        # These extras keys *should* be updated on every run of the harvester
        remove_from_extras = [
            "upstream_metadata_created",
            "upstream_metadata_modified",
            "upstream_url",
            "harvest_object_id",
            "harvest_source_id",
            "harvest_source_title",
            "london_smallest_geography",
            "update_frequency",
            "notes_with_markup",
        ]
        extras_to_transfer = remove_extras(
            existing_package["extras"], remove_from_extras
        )
    except Exception as e:
        # If the package doesn't exist, there aren't any extras to transfer
        extras_to_transfer = []

    for e in extras_to_transfer:
        upsert_package_extra(pkg_dict["extras"], e["key"], e["value"])

    return extras_to_transfer


def add_default_keys(pkg_dict):
    # Set some default keys so CKAN does not report them as being changed later.
    default_keys = [
        "author",
        "author_email",
        "license_id",
        "license_title",
        "url",
        "version",
    ]
    for key in default_keys:
        if key not in pkg_dict:
            pkg_dict[key] = ""

    if "extras" not in pkg_dict:
        pkg_dict["extras"] = []
    return


def add_default_extras(pkg_dict):
    # Add an empty data_quality field to extras if it's not already there
    if get_package_extra_val(pkg_dict["extras"], "data_quality") is None:
        pkg_dict["extras"].append({"key": "data_quality", "value": ""})

    # Add an empty dataset_boost field to extras if it's not already there
    if get_package_extra_val(pkg_dict["extras"], "dataset_boost") is None:
        pkg_dict["extras"].append({"key": "dataset_boost", "value": 1.0})
