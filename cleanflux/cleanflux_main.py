# coding=utf-8
import logging
import urllib.parse
from influxdb import DataFrameClient

from cleanflux.corrective_guard.corrective_guard import CorrectiveGuard
from cleanflux.utils.influx.querying import pd_result_to_influx_result


class Cleanflux(object):
    """
    The main cleanflux class which detects queries that need to be sanitized
    """

    def __init__(self, backend_host, backend_port, rules, retention_policies, aggregation_properties, counter_overflows,
                 max_nb_points_per_query, max_nb_points_per_series, safe_mode=True):
        """
        :param rules: A list of rules to evaluate
        :param safe_mode: If set to True, allow the query in case it can not be parsed
        :return:
        """

        self.backend_host = backend_host
        self.backend_port = backend_port

        self.guard = CorrectiveGuard(backend_host, backend_port, rules, retention_policies, aggregation_properties,
                                     counter_overflows,
                                     max_nb_points_per_query, max_nb_points_per_series)
        self.safe_mode = safe_mode

    def get_alt_data(self, user, password, schema, queries, precision):

        # return None

        got_alt_data = False
        alt_data_list = []
        for query_string in queries:
            logging.debug("Checking {}".format(query_string))
            query_sanitized = urllib.parse.unquote(query_string)#.decode('string_escape')
            alt_data = self.guard.get_data(user, password, schema, query_sanitized)
            if alt_data is not None:
                # logging.debug("Got alternative data for query")
                got_alt_data = True
            alt_data_list.append(alt_data)

        if not got_alt_data:
            return None

        pd_influx_client = DataFrameClient(self.backend_host, self.backend_port, user, password, schema)

        i = 0
        for query_string in queries:
            if alt_data_list[i] is None:
                result_df_dict = pd_influx_client.query(query_string)
                alt_data_list[i] = result_df_dict
            i = i + 1

        my_json = pd_result_to_influx_result(alt_data_list, precision)
        # logging.debug("formatted output: {0}".format(my_json))

        return my_json
