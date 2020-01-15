# coding=utf-8
import re
import logging
import numpy
from pprint import pprint
from datadog import statsd

from cleanflux.utils.influx.querying import pd_query
from cleanflux.corrective_rules.corrective_rule import CorrectiveRule
import cleanflux.utils.influx.query_sqlparsing as influx_query_parsing
import cleanflux.utils.influx.query_modification as influx_query_modification
import cleanflux.utils.influx.date_manipulation as influx_date_manipulation


class RuleChecker(CorrectiveRule):

    @staticmethod
    def description():
        return "Handles counter overflows when using function non_negative_derivative"

    @statsd.timed('timer_check_handle_counter_wrap_non_negative_derivative', use_ms=True)
    def check(self, query, parsed_query):
        is_non_negative_derivative = influx_query_parsing.is_non_negative_derivative(parsed_query)
        is_lower_time_bound_parsable = influx_query_parsing.is_lower_time_bound_parsable(parsed_query)
        return is_non_negative_derivative and is_lower_time_bound_parsable

    @statsd.timed('timer_handle_counter_wrap_non_negative_derivative', use_ms=True)
    def action(self, user, password, schema, query, more=None):

        overflow_value = more['overflow_value']

        nnd_interval_list = influx_query_parsing.extract_non_negative_derivative_time_interval(query)
        nnd_column_list = influx_query_parsing.extract_non_negative_derivative_column_name(query)

        nnd_interval_ms_list = []
        for nnd_interval in nnd_interval_list:
            nnd_interval_delta = influx_date_manipulation.influx_interval_to_timedelta(nnd_interval)
            nnd_interval_ms = int(nnd_interval_delta.total_seconds() * 1000)
            nnd_interval_ms_list.append(nnd_interval_ms)

        nb_default_column_name = 0
        default_column_name_map = {}
        for i, nnd_column in enumerate(nnd_column_list):
            if nnd_column == 'non_negative_derivative':
                if nb_default_column_name >= 1:
                    nnd_column_list[i] = nnd_column + '_' + str(i)
                nb_default_column_name += 1
                default_column_name_map[i] = nnd_column

        alt_query = influx_query_modification.remove_non_negative_derivative(query, None, forced_column_name_map=default_column_name_map)
        if alt_query is None:
            return None

        group_by_interval_influx = influx_query_parsing.extract_time_interval_group_by(query)
        if group_by_interval_influx is None:
            logging.error('Could not extract group by time interval from query')
            return None
        group_by_interval_parts = influx_date_manipulation.split_influx_time(group_by_interval_influx)
        number_group_by_interval = group_by_interval_parts['number']
        unit_group_by_interval = group_by_interval_parts['unit']
        query_time_shift = str(2 * number_group_by_interval) + unit_group_by_interval
        alt_query = influx_query_modification.extend_lower_time_bound(alt_query, query_time_shift)
        result_df_dict = pd_query(self.backend_host, self.backend_port, user, password, schema, alt_query)

        # remove counter wrapping
        for series_name in result_df_dict:
            df = result_df_dict[series_name]
            prev_value = None
            for index, row in df.iterrows():

                for nnd_column in nnd_column_list:
                    value = row[nnd_column]

                    if numpy.isnan(value):
                        continue

                    if prev_value is None:
                        prev_value = value
                        continue

                    diff = value - prev_value
                    if diff < 0:

                        shift = overflow_value - abs(diff)
                        while shift <= 0:
                            shift += overflow_value

                        new_value = prev_value + shift
                        df.at[index, nnd_column] = new_value
                        prev_value = new_value
                    else:
                        prev_value = value
                result_df_dict[series_name] = df

        # apply nnd
        for series_name in result_df_dict:
            df = result_df_dict[series_name]
            prev_value = None
            prev_index = None
            first_index = None
            for index, row in df.iterrows():
                for i, nnd_column in enumerate(nnd_column_list):
                    value = row[nnd_column]

                    if numpy.isnan(value):
                        continue

                    if prev_value is None:
                        prev_value = value
                        prev_index = index
                        first_index = index
                        df.at[index, nnd_column] = 0
                        continue

                    diff = value - prev_value

                    if diff < 0:
                        df.at[index, nnd_column] = 0
                    else:
                        time_diff = index.value - prev_index.value
                        new_value = diff * nnd_interval_ms_list[i] / time_diff
                        df.at[index, nnd_column] = new_value

                    prev_value = value
                    prev_index = index
            if first_index is not None:
                df = df.drop(first_index)
            result_df_dict[series_name] = df

        return result_df_dict
