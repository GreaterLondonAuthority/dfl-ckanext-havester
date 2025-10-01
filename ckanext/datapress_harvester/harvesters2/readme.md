# Creating a New Harvester

## Create a Collector

**Collectors should not have any dependencies on the harvesting extension**

1. Implement the abstract class Collector
    - gather() returns the urls to fetch from
    - For each item from gather(), gather_identifier() specifies how to id the item
    - For each url from gather(), fetch() retrieves content and returns details
    - For each item of content from fetch(), transform() will be called, 
    - transform() should map the original content to a common format (see class SimpleStandard)
   
2. Ideally perform mypy type checking on your collector before merge
   - To type a Collector, include the return types of your steps in the class definition
     - eg if gather() -> str, fetch() -> dict then your definition could be MyCollector(str, dict)

Collectors are run as a pipeline, the result of each step being passed to the next step as:
```
.
└── gather()
    ├── gather_identifier()
    └── fetch()
        └── transform()
```
## Add Collector to CKAN Harvest Extension

**The following steps utilise the harvest extension, likely to be removed for a more scalable pipeline tool**

1. Create a Harvester 
   - Extend class SimpleHarvester
   - Override collector() to return an instance of the Collector created in the steps above
   - Override info() to return the dict of information that the harvesting extension requires
   - **If unavoidable, ideally add to the behaviour of SimpleHarvester rather than create lots of new classes**
   
2. Update the entrypoints in setup.py to point to your new harvester class

3. Add the name you gave in setup.py to the list of extensions in your ckan config 
   - depending on the environment this may be CKAN__PLUGINS in your .env file

