import re

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
