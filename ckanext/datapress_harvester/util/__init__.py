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
