# ckanext-datapress_harvester

Data for London harvesters are to be migrated away from the CKAN Harvest Extension

New harvesters should create a Collector in harvesters2/ - see directory for readme and examples

Those in harvesters/ are tangled with the CKAN extension and, although still in use, are to be deprecated


# Legacy README

This repository provides the following four harvester implementations
used by the Data for London site to fetch data from upstream data
sources:

- Datapress
- Nomis
- Redbridge
- Soda

This project builds ontop of [CKAN](https://ckan.org/) and the [ckan-harvest extension](https://github.com/ckan/ckanext-harvest)

## Requirements

Compatibility with core CKAN versions:

| CKAN version       | Compatible?   |
| ------------------ | ------------- |
| 2.10.0 and earlier | not tested    |
| 2.10.1             | yes           |

* "yes" - This has been tested and is known to work
* "not tested" - I can't think of a reason why it wouldn't work
* "not yet" - there is an intention to get it working
* "no" - this is known not to work (and there is no intention to make it work)

## Config settings

See [Harvester configuration](./docs/harvester_config.md)

## License

[AGPL](https://www.gnu.org/licenses/agpl-3.0.en.html)
