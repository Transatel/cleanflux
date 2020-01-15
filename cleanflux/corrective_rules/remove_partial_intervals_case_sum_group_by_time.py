# coding=utf-8
import re
import logging
from datadog import statsd

from cleanflux.utils.influx.querying import pd_query
from cleanflux.corrective_rules.corrective_rule import CorrectiveRule
import cleanflux.utils.influx.query_sqlparsing as influx_query_parsing
import cleanflux.utils.influx.query_modification as influx_query_modification
import cleanflux.utils.influx.date_manipulation as influx_date_manipulation


class RuleChecker(CorrectiveRule):

    @staticmethod
    def description():
        return "Removes start and end partial intervals case doing a SUM() along with a GROUP BY time()"

    @statsd.timed('timer_check_remove_partial_intervals_case_sum_group_by_time', use_ms=True)
    def check(self, query, parsed_query):
        is_sum_group_by_time = influx_query_parsing.is_sum_group_by_time(parsed_query)
        is_lower_time_bound_parsable = influx_query_parsing.is_lower_time_bound_parsable(query)
        return is_sum_group_by_time and is_lower_time_bound_parsable

    @statsd.timed('timer_remove_partial_intervals_case_sum_group_by_time', use_ms=True)
    def action(self, user, password, schema, query, parsed_query, more=None):

        query, group_by_interval = self.rework_query(query, parsed_query)

        result_df_dict = pd_query(self.backend_host, self.backend_port, user, password, schema, query)

        result_df_dict = self.rework_data(result_df_dict, group_by_interval)

        # NB: datetime is the index (df.index), only one column for which values can be retrieved via df.values

        return result_df_dict

    # --------------------------------------------------------------------
    # HELPER METHODS

    @staticmethod
    @statsd.timed('timer_remove_partial_intervals_case_sum_group_by_time_rework_query', use_ms=True)
    def rework_query(query, parsed_query):
        group_by_interval_influx = influx_query_parsing.extract_time_interval_group_by(parsed_query)
        if group_by_interval_influx is None:
            logging.error('Could not extract group by time interval from query')
            return None

        group_by_interval = influx_date_manipulation.influx_interval_to_timedelta(group_by_interval_influx)
        group_by_interval_parts = influx_date_manipulation.split_influx_time(group_by_interval_influx)
        number_group_by_interval = group_by_interval_parts['number']
        unit_group_by_interval = group_by_interval_parts['unit']

        # NB: we extend lower time bound by one group by interval to compensate for it being removed
        query_time_shift = str(2 * number_group_by_interval) + unit_group_by_interval
        query = influx_query_modification.extend_lower_time_bound(query, query_time_shift)
        # logging.debug("new query: {0}".format(query))

        return [query, group_by_interval]

    @staticmethod
    @statsd.timed('timer_remove_partial_intervals_case_sum_group_by_time_rework_data', use_ms=True)
    def rework_data(result_df_dict, group_by_interval):
        for series in result_df_dict:
            df = result_df_dict[series]

            # Case of a timerange partly in the future.
            # We have in this case all the points in the future with null values for all columns, and the one just
            # before is a partial interval.
            # To counter this, we'd have to find all those points with no value and ignore them or drop them.
            # To simplify things, we just drop all points with no values but this could result in dropping legitimate
            # points (not in future).
            df = df.dropna(how='all')

            # remove crappy first and last points, that correspond to partial intervals
            len_df = len(df)
            if len_df > 2:
                df = df.drop(df.index[0])
                df = df.drop(df.index[len_df - 2])

            # transform index so that sum corresponds actually to the last interval and not the next
            df.index = [influx_date_manipulation.apply_timedelta_on_datetime(i, group_by_interval) for i in df.index]

            result_df_dict[series] = df

        # REVIEW: might be unnecessary for dicts
        return result_df_dict
