# Creating a New Harvester
1. **Collectors should not have any dependencies on the harvesting extension**
2. Implement the abstract class Collector
    - Include the return type of what you're passing from fetch() to transform() in the class definition
    - Gather() returns the urls to fetch from
    - For each url from gather(), fetch() retrieves its content and returns the details
    - For each item of content from fetch(), transform() will be called, which should map the original content to the appropriate fields in a common format
3. Create a Harvester class
    - If your collector matches the abstract class then you shouldn't need to implement any behaviour
    - Override collector() to return an instance of the one you created
    - Override info() to return the dict of information that the harvesting extension requires
    - If your source is unusual somehow then you may need to override one or more of the harvesting stages
    - Ideally expand & improve the generic one to handle more cases rather than creating lots of harvesters
4. Update the entrypoints in setup.py to point to your new harvester class
5. Add the name you gave in setup.py to the list of extensions in your ckan config (for dev instances this may be in your .env file)

