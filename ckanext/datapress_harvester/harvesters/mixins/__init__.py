import logging
import csv
from ckan.logic import get_action, NotFound

log = logging.getLogger(__name__)

PROVIDER_ORG_MAPPINGS = {}
try:
    with open("organisation_mappings.csv", mode='r', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            provider_id = row["Provider"]
            original_id = row["Original ID"]
            if provider_id not in PROVIDER_ORG_MAPPINGS:
                PROVIDER_ORG_MAPPINGS[provider_id] = {}
            if original_id not in PROVIDER_ORG_MAPPINGS[provider_id]:
                PROVIDER_ORG_MAPPINGS[provider_id][original_id] = {}
                
            PROVIDER_ORG_MAPPINGS[provider_id][original_id]['name'] = row["Override ID"]
            PROVIDER_ORG_MAPPINGS[provider_id][original_id]['title'] = row["Override Title"]     

except BaseException as ex:    
    log.info(f"No organisation_mappings.csv file was provided to canonicalise organisation names {ex}")
    
# Makes an attempt to canonicalise a string from either an org id, or
# an org name slug and canonicalises it into the org name for mapping,
# though if there is no record of the organisation stored it will
# return the input id.
def canonicalise_org_to_name(base_context, org_name_or_id):
    try:
        org_name = get_action('organization_show')(base_context, data_dict={'id': org_name_or_id})['name']
        return org_name
    except NotFound:
        return org_name_or_id
    
class DFLHarvesterMixin:
    def get_mapped_organization(self, base_context, harvest_object, organization, remote_orgs, package_dict, org_link):
        validated_org = None

        source_name = get_action('harvest_source_show')(base_context.copy(),{'id':harvest_object.source.id}).get('name')

        org_name = canonicalise_org_to_name(base_context.copy(), organization)
        
        mapped_org = PROVIDER_ORG_MAPPINGS.get(source_name,{}).get(org_name)

        try:
            data_dict = {"id": mapped_org['name'] if mapped_org else org_name}
            org = get_action("organization_show")(
                base_context.copy(), data_dict
            )
            validated_org = org["id"]
            log.info(f'Org {validated_org} exists')
            return validated_org
        except NotFound:
            log.info("Organization %s is not available", organization)

            if remote_orgs == "create": 
                org = package_dict.get("organization") or {'name': organization, 'title': package_dict.get('org_name', organization)}

                if mapped_org:
                    org["title"] = mapped_org.get('title') or mapped_org['name']
                    org["name"] = mapped_org['name']
                
                for key in [
                    "packages",
                    "created",
                    "users",
                    "groups",
                    "tags",
                    "extras",
                    "display_name",
                    "type",
                ]:
                    org.pop(key, None)

                if org_link is not None:
                    org = {**org,
                            "extras": [{"key": "Website",
                                        "value": org_link}]}

                log.info(f'Attempt to create {org["name"]}')
                new_org = get_action("organization_create")(base_context.copy(), org)
                log.info(
                    "Organization %s has been newly created", organization
                )
                validated_org = new_org["id"]

        return validated_org
