import logging
from datadog import statsd

from cleanflux.corrective_rules.loader import import_rules
from cleanflux.utils.influx.querying import pd_query, get_rp_list
import cleanflux.utils.influx.query_sqlparsing as influx_query_parsing
import cleanflux.utils.influx.rp_auto_selection as influx_rp_auto_selection


class CorrectiveGuard(object):
    """
    The guard checks a given query for their possible impact.
    It does so by iterating over all active rules and checking for violations
    """

    def __init__(self, backend_host, backend_port, backend_user, backend_password,
                 rule_names, retention_policies, aggregation_properties,
                 counter_overflows,
                 max_nb_points_per_query, max_nb_points_per_series):
        self.rules = import_rules(backend_host, backend_port, rule_names)
        self.retention_policies = retention_policies
        self.aggregation_properties = aggregation_properties
        self.counter_overflows = counter_overflows
        self.max_nb_points_per_query = max_nb_points_per_query
        self.max_nb_points_per_series = max_nb_points_per_series
        self.backend_host = backend_host
        self.backend_port = backend_port
        self.backend_user = backend_user
        self.backend_password = backend_password


    @statsd.timed('timer_corrective_guard', use_ms=True)
    def enrich_rp_conf_from_db(self, schema_list=[]):
        retention_policies_auto = get_rp_list(self.backend_host, self.backend_port,
                                              self.backend_user, self.backend_password)
        for rp, props in retention_policies_auto.items():
            if rp in self.retention_policies:
                retention_policies_auto[rp] = self.retention_policies[rp]
        self.retention_policies = retention_policies_auto


    @statsd.timed('timer_corrective_guard', use_ms=True)
    def get_data(self, user, password, schema, query):

        # prevent expensive parsing of query case not select
        if not query.upper().startswith('SELECT '):
            return None

        parsed_query = influx_query_parsing.sqlparse_query(query)

        if not influx_query_parsing.is_select(parsed_query):
            return None

        query_is_modified = False

        from_parts = influx_query_parsing.extract_measurement_from_query(schema, parsed_query)

        query_auto_rp = influx_rp_auto_selection.update_query_with_right_rp(from_parts, query, parsed_query,
                                                                            self.retention_policies,
                                                                            self.aggregation_properties, False)
        if query_auto_rp is not None:
            query_is_modified = True
            query = query_auto_rp

        if self.max_nb_points_per_query is not None:
            query_limit_nb_points = influx_rp_auto_selection.update_query_to_limit_nb_points_for_query(
                self.backend_host, self.backend_port, user, password, from_parts,
                query, parsed_query,
                self.aggregation_properties,
                self.max_nb_points_per_query)
            if query_limit_nb_points is not None:
                query_is_modified = True
                query = query_limit_nb_points
        elif self.max_nb_points_per_series is not None:
            query_limit_nb_points = influx_rp_auto_selection.update_query_to_limit_nb_points_per_series(
                from_parts, query, parsed_query,
                self.aggregation_properties, self.max_nb_points_per_series)
            if query_limit_nb_points is not None:
                query_is_modified = True
                query = query_limit_nb_points

        # for rule in self.rules.itervalues():
        if 'handle_counter_wrap_non_negative_derivative' in self.rules:
            if from_parts['schema'] is not None and from_parts['schema'] in self.counter_overflows:
                measurement_overflows = self.counter_overflows[from_parts['schema']]
                if from_parts['measurement'] in measurement_overflows:
                    if from_parts['measurement'] in measurement_overflows:
                        # NB: should do it for each field, instead of for whole measurement
                        rule = self.rules['handle_counter_wrap_non_negative_derivative']
                        if rule.check(query):
                            more = {'overflow_value': measurement_overflows[from_parts['measurement']]}
                            return rule.action(user, password, schema, query, more)
        if 'remove_partial_intervals_case_sum_group_by_time' in self.rules:
            rule = self.rules['remove_partial_intervals_case_sum_group_by_time']
            if rule.check(query, parsed_query):
                return rule.action(user, password, schema, query, parsed_query)

        if query_is_modified:
            result_df_dict = pd_query(self.backend_host, self.backend_port, user, password, schema, query)
            return result_df_dict

        return None
