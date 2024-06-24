# Harvester Configuration

CKAN harvesters can optionally accept configuration as a JSON object.
This can be provided when they are created in the CKAN admin panel.

This document describes the configuration options supported by our
harvesters.

## Datapress

`remote_orgs`

By default, remote organizations are ignored. Setting this property
enables the harvester to import remote organizations. Setting it to
'create' will make an attempt to create the organizations by copying
the details from the remote CKAN.

`datapress_api_key`

This optional key if provided should be a string containing a
DataPress API key. If the key is provided the harvester will use the
key to request a JWT token from DataPress, and will then use that
token to authenticate requests to Datapress.

This API key is required to access specific private datasets.

## CKAN

`remote_orgs`

By default, remote organizations are ignored. Setting this property
enables the harvester to import remote organizations. Setting it to
'create' will make an attempt to create the organizations by copying
the details from the remote CKAN.


In addition the following default flags documented on [CKAN harvester plugin repository](https://github.com/ckan/ckanext-harvest?tab=readme-ov-file#the-ckan-harvester) can be used. However these have not been tested and are not guaranteed
to work.


## SODA

E.g.

```json
{"app_token": "<TOKEN_HERE>", "remote_orgs": "create"}
```


`remote_orgs`

By default, remote organizations are ignored. Setting this property
enables the harvester to import remote organizations. Setting it to
'create' will make an attempt to create the organizations by copying
the details from the remote CKAN.

`app_token` - All requests should include an app token that identifies
your application, and each application should have its own unique app
token. See [Socrata Developer Portal](https://dev.socrata.com/foundry/opendata.camden.gov.uk/uqwb-mdhe/embed)

## Redbridge and NOMIS

None identified
