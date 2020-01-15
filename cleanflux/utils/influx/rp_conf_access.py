import logging


# ------------------------------------------------------------------------

def get_default_rp_for_schema_from_conf(schema, known_retention_policies):
    for rp in known_retention_policies[schema]:
        if 'default' in rp and rp['default'] is True:
            return rp
    return None
