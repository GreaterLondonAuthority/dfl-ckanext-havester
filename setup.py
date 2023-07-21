# -*- coding: utf-8 -*-
from setuptools import setup

setup(
    entry_points="""
        [ckan.plugins]
        datapress_harvester=ckanext.datapress_harvester.harvesters:DataPressHarvester
        nomis_localauthprofile=ckanext.datapress_harvester.harvesters:NomisLocalAuthorityProfileScraper
        redbridge_harvester=ckanext.datapress_harvester.harvesters:RedbridgeHarvester
    """,
    # If you are changing from the default layout of your extension, you may
    # have to change the message extractors, you can read more about babel
    # message extraction at
    # http://babel.pocoo.org/docs/messages/#extraction-method-mapping-and-configuration
    message_extractors={
        "ckanext": [
            ("**.py", "python", None),
            ("**.js", "javascript", None),
            ("**/templates/**.html", "ckan", None),
        ],
    },
)
